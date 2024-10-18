use std::{
    cmp::max,
    collections::{BTreeMap, HashSet},
    num::NonZeroUsize,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use async_stream::stream;
use cadence_macros::statsd_count;
use futures::{pin_mut, stream::FuturesUnordered, Stream, StreamExt};
use lru::LruCache;
use solana_client::{
    client_error, nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig,
    rpc_request::RpcError,
};

use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

use crate::{
    common::typedefs::rpc_client_with_uri::RpcClientWithUri,
    ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo},
    metric,
    monitor::{start_latest_slot_updater, LATEST_SLOT},
};

const SKIPPED_BLOCK_ERRORS: [i64; 2] = [-32007, -32009];
const RETRIES: u64 = 3;
const INFINITY: u64 = u64::MAX;

fn get_slot_stream(rpc_client: Arc<RpcClientWithUri>, start_slot: u64) -> impl Stream<Item = u64> {
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
    rpc_client: Arc<RpcClientWithUri>,
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
        let block_stream = slot_stream
            .map(|slot| {
                let rpc_client = rpc_client.clone();
                async move { fetch_block_with_infinite_retries(rpc_client.uri.clone(), slot).await }
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
            yield blocks_to_index;
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
        } else {
            if min_slot < last_indexed_slot {
                panic!(
                    "Block is smaller than last indexed slot: {} < {}",
                    min_slot, last_indexed_slot
                );
            }
            break;
        }
        block_cache.remove(&min_slot);
    }
    (blocks, last_indexed_slot)
}

pub async fn fetch_block_with_infinite_retries(rpc_uri: String, slot: u64) -> Option<BlockInfo> {
    let mut attempt_counter = 0;
    loop {
        let timeout_sec = if attempt_counter <= 1 { 5 } else { 30 };
        attempt_counter += 1;
        let client = RpcClient::new_with_timeout_and_commitment(
            rpc_uri.clone(),
            Duration::from_secs(timeout_sec),
            CommitmentConfig::confirmed(),
        );
        match client
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
                log::debug!("Failed to fetch block: {}. {}", slot, e.to_string());
                metric! {
                    statsd_count!("rpc_block_fetch_failed", 1);
                }
            }
        }
    }
}
