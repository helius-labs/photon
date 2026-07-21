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

use solana_commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

use crate::{
    ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo},
    metric,
    monitor::{start_latest_slot_updater, LATEST_SLOT},
};

// -32009/-32004 conflate "slot was skipped" with "missing in long-term storage",
// so a skip answer from one source is only trusted if every source agrees.
const SKIPPED_BLOCK_ERRORS: [i64; 3] = [-32007, -32009, -32004];

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
    fallback_rpc_clients: Vec<Arc<RpcClient>>,
    mut last_indexed_slot: u64,
    max_concurrent_block_fetches: usize,
) -> impl Stream<Item = Vec<BlockInfo>> {
    stream! {
        let start_slot = match last_indexed_slot {
            0 => 0,
            last_indexed_slot => last_indexed_slot + 1
        };
        let slot_stream = get_slot_stream(rpc_client.clone(), start_slot);
        pin_mut!(slot_stream);
        let rpc_clients: Arc<Vec<Arc<RpcClient>>> = Arc::new(
            std::iter::once(rpc_client.clone())
                .chain(fallback_rpc_clients.into_iter())
                .collect(),
        );
        let block_stream = slot_stream
            .map(move |slot| {
                let rpc_clients = rpc_clients.clone();
                async move { fetch_block_with_infinite_retries(&rpc_clients, slot).await }
            })
            .buffer_unordered(max_concurrent_block_fetches);
        pin_mut!(block_stream);
        let mut block_cache: BTreeMap<u64, BlockInfo> = BTreeMap::new();
        while let Some(block) = block_stream.next().await {
            if let Some(block) = block {
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
        }
    }
}

fn pop_cached_blocks_to_index(
    block_cache: &mut BTreeMap<u64, BlockInfo>,
    mut last_indexed_slot: u64,
) -> (Vec<BlockInfo>, u64) {
    let mut blocks = Vec::new();
    while let Some(&min_slot) = block_cache.keys().min() {
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
    rpc_clients: &[Arc<RpcClient>],
    slot: u64,
) -> Option<BlockInfo> {
    loop {
        let mut all_sources_claim_skip = true;
        for rpc_client in rpc_clients {
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
                    ) = *e.kind
                    {
                        if SKIPPED_BLOCK_ERRORS.contains(&code) {
                            continue;
                        }
                    }
                    all_sources_claim_skip = false;
                    metric! {
                        statsd_count!("rpc_block_fetch_failed", 1);
                    }
                }
            }
        }
        // Only classify the slot as skipped once every source, including
        // backups, says so; a lone skip answer can also mean the block is
        // merely missing from that source's long-term storage.
        if all_sources_claim_skip {
            metric! {
                statsd_count!("rpc_skipped_block", 1);
            }
            log::info!("Skipped block: {} (all {} sources)", slot, rpc_clients.len());
            return None;
        }
    }
}
