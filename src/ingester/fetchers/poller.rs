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

use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

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
    canonical_rpc_client: Option<Arc<RpcClient>>,
) -> impl Stream<Item = Vec<BlockInfo>> {
    stream! {
        let start_slot = match last_indexed_slot {
            0 => 0,
            last_indexed_slot => last_indexed_slot + 1
        };
        let slot_stream = get_slot_stream(rpc_client.clone(), start_slot);
        pin_mut!(slot_stream);
        let block_stream = slot_stream
            .map(|slot| {
                let rpc_client = rpc_client.clone();
                let canonical_rpc_client = canonical_rpc_client.clone();
                async move { 
                    let result = fetch_block_with_infinite_retries_and_canonical(rpc_client.clone(), slot, canonical_rpc_client).await;
                    (slot, result)
                }
            })
            .buffer_unordered(max_concurrent_block_fetches);
        pin_mut!(block_stream);
        let mut block_cache: BTreeMap<u64, BlockInfo> = BTreeMap::new();
        let mut skipped_slots: BTreeMap<u64, bool> = BTreeMap::new();
        
        while let Some((slot, block)) = block_stream.next().await {
            if let Some(block) = block {
                block_cache.insert(block.metadata.slot, block);
            } else {
                // Track that this slot was skipped
                skipped_slots.insert(slot, true);
            }
            
            // Clean up skipped slots that are now too old to matter
            skipped_slots.retain(|&s, _| s > last_indexed_slot);
            
            let (blocks_to_index, last_indexed_slot_from_cache) = pop_cached_blocks_to_index(&mut block_cache, last_indexed_slot, &skipped_slots);
            last_indexed_slot = last_indexed_slot_from_cache;
            metric! {
                statsd_count!("rpc_block_emitted", blocks_to_index.len() as i64);
            }
            if !blocks_to_index.is_empty() {
                yield blocks_to_index;
            }
        }
    }
}

fn pop_cached_blocks_to_index(
    block_cache: &mut BTreeMap<u64, BlockInfo>,
    mut last_indexed_slot: u64,
    skipped_slots: &BTreeMap<u64, bool>,
) -> (Vec<BlockInfo>, u64) {
    let mut blocks = Vec::new();
    
    // Helper function to check if we can reach last_indexed_slot from a parent_slot
    // by traversing through skipped slots
    let can_reach_through_skips = |parent_slot: u64, target_slot: u64| -> bool {
        if parent_slot == target_slot {
            return true;
        }
        if parent_slot < target_slot {
            return false;
        }
        
        // Check if all slots between parent_slot and target_slot (exclusive) are skipped
        for slot in (target_slot + 1)..parent_slot {
            if !skipped_slots.contains_key(&slot) {
                // This slot should have a block but we haven't seen it yet
                return false;
            }
        }
        true
    };
    
    loop {
        let min_slot = match block_cache.keys().min() {
            Some(&slot) => slot,
            None => break,
        };
        let block: &BlockInfo = block_cache.get(&min_slot).unwrap();
        
        // Case 1: This block is older than what we've already indexed - discard it
        if min_slot <= last_indexed_slot {
            block_cache.remove(&min_slot);
            continue;
        }
        
        // Case 2: Check if this block can be connected to our last indexed slot
        // either directly or through a chain of skipped slots
        if can_reach_through_skips(block.metadata.parent_slot, last_indexed_slot) {
            // Log if there were skipped slots
            if block.metadata.parent_slot > last_indexed_slot + 1 {
                // There's a real gap between last_indexed_slot and parent_slot
                log::info!("Block at slot {} has parent {} - accepting due to skipped slots between {} and {}", 
                         min_slot, block.metadata.parent_slot, last_indexed_slot + 1, block.metadata.parent_slot);
            } else if block.metadata.parent_slot == last_indexed_slot && min_slot > last_indexed_slot + 1 {
                // Direct parent match but there's a gap in slot numbers
                log::info!("Block at slot {} directly follows {} (slots {}-{} were skipped)", 
                         min_slot, last_indexed_slot, last_indexed_slot + 1, min_slot - 1);
            }
            
            last_indexed_slot = block.metadata.slot;
            blocks.push(block.clone());
            block_cache.remove(&min_slot);
            continue;
        }
        
        // Case 3: This block's parent is in the future - we're missing intermediate blocks
        if block.metadata.parent_slot > last_indexed_slot {
            // Wait for the intermediate blocks to arrive or be marked as skipped
            break;
        }
        
        // Case 4: This block's parent is before our last indexed slot
        // This indicates a fork or invalid block - discard it
        if block.metadata.parent_slot < last_indexed_slot {
            log::warn!("Discarding block at slot {} with parent {} (last indexed: {})", 
                      min_slot, block.metadata.parent_slot, last_indexed_slot);
            block_cache.remove(&min_slot);
            continue;
        }
    }
    (blocks, last_indexed_slot)
}

pub async fn fetch_block_with_infinite_retries(
    rpc_client: Arc<RpcClient>,
    slot: u64,
) -> Option<BlockInfo> {
    fetch_block_with_infinite_retries_and_canonical(rpc_client, slot, None).await
}

pub async fn fetch_block_with_infinite_retries_and_canonical(
    rpc_client: Arc<RpcClient>,
    slot: u64,
    canonical_rpc_client: Option<Arc<RpcClient>>,
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
                    RpcError::RpcResponseError { code, message, .. },
                ) = &e.kind
                {
                    if SKIPPED_BLOCK_ERRORS.contains(&code) {
                        // Check with canonical RPC before marking as skipped
                        if let Some(canonical_client) = &canonical_rpc_client {
                            log::info!("Slot {} reported as skipped by primary RPC, validating with canonical RPC", slot);
                            
                            match canonical_client.get_block_with_config(
                                slot,
                                solana_client::rpc_config::RpcBlockConfig {
                                    encoding: Some(UiTransactionEncoding::Base64),
                                    transaction_details: Some(TransactionDetails::Full),
                                    rewards: Some(false),
                                    commitment: Some(CommitmentConfig::confirmed()),
                                    max_supported_transaction_version: Some(0),
                                },
                            ).await {
                                Ok(block) => {
                                    log::warn!("Slot {} exists on canonical RPC but not on primary - using canonical", slot);
                                    metric! {
                                        statsd_count!("rpc_canonical_fallback", 1);
                                    }
                                    return Some(parse_ui_confirmed_blocked(block, slot).unwrap());
                                }
                                Err(canonical_error) => {
                                    if let solana_client::client_error::ClientErrorKind::RpcError(
                                        RpcError::RpcResponseError { code: canonical_code, .. },
                                    ) = &canonical_error.kind {
                                        if SKIPPED_BLOCK_ERRORS.contains(&canonical_code) {
                                            log::info!("Slot {} confirmed as skipped by canonical RPC", slot);
                                        } else {
                                            log::warn!("Canonical RPC returned different error for slot {}: {}", slot, canonical_error);
                                        }
                                    } else {
                                        log::warn!("Canonical RPC failed for slot {}: {}", slot, canonical_error);
                                    }
                                }
                            }
                        }
                        
                        metric! {
                            statsd_count!("rpc_skipped_block", 1);
                        }
                        log::info!("Slot {} has no block (skipped): {} (code: {})", slot, message, code);
                        return None;
                    }
                    log::warn!("RPC error for slot {} (code: {}): {} - will retry", slot, code, message);
                } else {
                    log::warn!("Failed to fetch block at slot {}: {} - will retry", slot, e);
                }
                metric! {
                    statsd_count!("rpc_block_fetch_failed", 1);
                }
                // Continue retrying for non-skipped errors
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }
}
