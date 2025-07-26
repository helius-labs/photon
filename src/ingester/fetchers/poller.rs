use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
};

use async_stream::stream;
use cadence_macros::statsd_count;
use futures::{pin_mut, Stream, StreamExt};
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig, rpc_request::RpcError,
};
use tokio::sync::mpsc;

use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

use crate::ingester::gap::RewindCommand;
use crate::{
    ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo},
    metric,
    monitor::{start_latest_slot_updater, LATEST_SLOT},
};

const SKIPPED_BLOCK_ERRORS: [i64; 2] = [-32007, -32009];

fn get_slot_stream(rpc_client: Arc<RpcClient>, start_slot: u64) -> impl Stream<Item = u64> {
    stream! {
        start_latest_slot_updater(rpc_client.clone()).await;
        let mut next_slot_to_fetch = start_slot;
        loop {
            if next_slot_to_fetch > LATEST_SLOT.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }
            yield next_slot_to_fetch;
            next_slot_to_fetch += 1;
        }
    }
}

pub fn get_block_poller_stream(
    rpc_client: Arc<RpcClient>,
    mut last_indexed_slot: u64,
    max_concurrent_block_fetches: usize,
    mut rewind_receiver: Option<mpsc::UnboundedReceiver<RewindCommand>>,
) -> impl Stream<Item = Vec<BlockInfo>> {
    stream! {
        let mut current_start_slot = match last_indexed_slot {
            0 => 0,
            last_indexed_slot => last_indexed_slot + 1
        };

        loop {
            let slot_stream = get_slot_stream(rpc_client.clone(), current_start_slot);
            pin_mut!(slot_stream);
            let block_stream = slot_stream
                .map(|slot| {
                    let rpc_client = rpc_client.clone();
                    async move { fetch_block_with_infinite_retries(rpc_client.clone(), slot).await }
                })
                .buffer_unordered(max_concurrent_block_fetches);
            pin_mut!(block_stream);
            let mut block_cache: BTreeMap<u64, BlockInfo> = BTreeMap::new();
            let mut rewind_occurred = false;
            
            while let Some(block) = block_stream.next().await {
                // Check for rewind commands before processing blocks
                if let Some(ref mut receiver) = rewind_receiver {
                    while let Ok(command) = receiver.try_recv() {
                        match command {
                            RewindCommand::Rewind { to_slot, reason } => {
                                log::error!("Rewinding block stream to {}: {}", to_slot, reason);
                                // Clear cached blocks
                                block_cache.clear();
                                // Reset positions
                                last_indexed_slot = to_slot - 1;
                                current_start_slot = to_slot;
                                rewind_occurred = true;
                                log::info!("Cleared cache, restarting from slot {}", current_start_slot);
                                break;
                            }
                        }
                    }
                }

                if rewind_occurred {
                    break; // Exit inner loop to restart streams
                }

                if let Some(block) = block {
                    // Check if this block has any compression transactions
                    let has_compression_txs = block.transactions.iter().any(|tx| {
                        tx.instruction_groups.iter().any(|group| {
                            group.outer_instruction.program_id == get_compression_program_id()
                        })
                    });
                    
                    // Consider block "empty" if it has no compression transactions
                    let is_empty_block = !has_compression_txs;
                    
                    // If current block is empty, check if we can optimize the cache
                    if is_empty_block && block.metadata.slot > last_indexed_slot + 1 {
                        // Remove any previous empty blocks that are consecutive
                        let parent_slot = block.metadata.parent_slot;
                        if let Some(parent_block) = block_cache.get(&parent_slot) {
                            // Check if parent block also has no compression transactions
                            let parent_has_compression = parent_block.transactions.iter().any(|tx| {
                                tx.instruction_groups.iter().any(|group| {
                                    group.outer_instruction.program_id == get_compression_program_id()
                                })
                            });
                            
                            if !parent_has_compression {
                                // Remove the parent empty block since we have a newer empty block
                                block_cache.remove(&parent_slot);
                                if blocks_processed % 100 == 0 {
                                    log::debug!("Removed non-compression block at slot {}", parent_slot);
                                }
                                
                                // Also remove any other consecutive empty blocks before this
                                let mut check_slot = parent_slot.saturating_sub(1);
                                while check_slot > last_indexed_slot {
                                    if let Some(check_block) = block_cache.get(&check_slot) {
                                        let check_has_compression = check_block.transactions.iter().any(|tx| {
                                            tx.instruction_groups.iter().any(|group| {
                                                group.outer_instruction.program_id == get_compression_program_id()
                                            })
                                        });
                                        
                                        if !check_has_compression {
                                            block_cache.remove(&check_slot);
                                            check_slot = check_slot.saturating_sub(1);
                                        } else {
                                            break;
                                        }
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    
                    // Log warning if cache is getting large
                    if block_cache.len() > 50000 && blocks_processed % 100 == 0 {
                        log::warn!(
                            "Cache size is large: {} blocks. Consider restarting with --start-slot closer to current activity",
                            block_cache.len()
                        );
                    }
                    
                    block_cache.insert(block.metadata.slot, block);
                }
                let (blocks_to_index, last_indexed_slot_from_cache) = pop_cached_blocks_to_index(&mut block_cache, last_indexed_slot);
                last_indexed_slot = last_indexed_slot_from_cache;
                metric! {
                    statsd_count!("rpc_block_emitted", blocks_to_index.len() as i64);
                }
                if !blocks_to_index.is_empty() {
                    yield blocks_to_index;
                }
                
                // Log cache status periodically
                blocks_processed += 1;
                if blocks_processed % 1000 == 0 {
                    if let Some(&min_cached) = block_cache.keys().min() {
                        if let Some(&max_cached) = block_cache.keys().max() {
                            // Count blocks with compression transactions
                            let compression_count = block_cache.values().filter(|b| {
                                b.transactions.iter().any(|tx| {
                                    tx.instruction_groups.iter().any(|group| {
                                        group.outer_instruction.program_id == get_compression_program_id()
                                    })
                                })
                            }).count();
                            let non_compression_count = block_cache.len() - compression_count;
                            
                            log::info!(
                                "Block cache status - size: {} (non-compression: {}, with-compression: {}), range: {}-{}, last_indexed: {}, gap: {}",
                                block_cache.len(),
                                non_compression_count,
                                compression_count,
                                min_cached,
                                max_cached,
                                last_indexed_slot,
                                min_cached.saturating_sub(last_indexed_slot)
                            );
                        }
                    }
                }
            }

            if !rewind_occurred {
                break; // Normal termination
            }
        }
    }
}

fn pop_cached_blocks_to_index(
    block_cache: &mut BTreeMap<u64, BlockInfo>,
    mut last_indexed_slot: u64,
) -> (Vec<BlockInfo>, u64) {
    let mut blocks = Vec::new();
    loop {
        let min_slot = match block_cache.keys().min() {
            Some(&slot) => slot,
            None => break,
        };
        let block: &BlockInfo = block_cache.get(&min_slot).unwrap();
        if block.metadata.parent_slot == last_indexed_slot {
            last_indexed_slot = block.metadata.slot;
            blocks.push(block.clone());
            block_cache.remove(&min_slot);
        } else if min_slot < last_indexed_slot {
            block_cache.remove(&min_slot);
        } else {
            break;
        }
    }
    (blocks, last_indexed_slot)
}

pub async fn fetch_block_with_infinite_retries(
    rpc_client: Arc<RpcClient>,
    slot: u64,
) -> Option<BlockInfo> {
    loop {
        match rpc_client
            .get_block_with_config(
                slot,
                RpcBlockConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    transaction_details: Some(TransactionDetails::Full),
                    rewards: None,
                    commitment: Some(CommitmentConfig::confirmed()),
                    max_supported_transaction_version: Some(0),
                },
            )
            .await
        {
            Ok(block) => {
                metric! {
                    statsd_count!("rpc_block_fetched", 1);
                }
                return Some(parse_ui_confirmed_blocked(block, slot).unwrap());
            }
            Err(e) => {
                if let solana_client::client_error::ClientErrorKind::RpcError(
                    RpcError::RpcResponseError { code, .. },
                ) = e.kind
                {
                    if SKIPPED_BLOCK_ERRORS.contains(&code) {
                        metric! {
                            statsd_count!("rpc_skipped_block", 1);
                        }
                        log::info!("Skipped block: {}", slot);
                        return None;
                    }
                }
                metric! {
                    statsd_count!("rpc_block_fetch_failed", 1);
                }
            }
        }
    }
}
