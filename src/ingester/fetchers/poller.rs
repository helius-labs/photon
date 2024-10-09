use std::{
    cmp::max,
    collections::{BTreeMap, HashSet},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::sleep,
    time::Duration,
};

use async_stream::stream;
use cadence_macros::statsd_count;
use futures::{stream::FuturesUnordered, StreamExt};
use lru::LruCache;
use solana_client::{
    client_error, nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig,
    rpc_request::RpcError,
};

use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use tokio::time::interval;

use crate::{
    common::typedefs::rpc_client_with_uri::RpcClientWithUri,
    ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo},
    metric,
};

const SKIPPED_BLOCK_ERRORS: [i64; 2] = [-32007, -32009];
const RETRIES: u64 = 3;
const INFINITY: u64 = u64::MAX;

use once_cell::sync::Lazy;

pub static LATEST_SLOT: Lazy<Arc<AtomicU64>> = Lazy::new(|| Arc::new(AtomicU64::new(0)));

/// This function creates a stream that continuously fetches and emits blocks from a Solana blockchain.
/// It implements a concurrent block fetching algorithm with the following key features:
///
/// 1. Concurrent block fetching: It can fetch multiple blocks simultaneously up to a specified limit.
/// 2. Block caching: Fetched blocks are cached if they can't be immediately processed.
/// 3. Skipped slot handling: It keeps track of skipped slots to avoid unnecessary fetching attempts.
/// 4. Parent block fetching: If a block's parent is missing, it initiates a fetch for the parent.
/// 5. Ordered block emission: It ensures blocks are emitted in the correct order, even if fetched out of order.
///
/// Algorithm overview:
/// - Initialize data structures for block fetching, caching, and tracking.
/// - Enter a loop that continues indefinitely:
///   a. Fetch the current latest slot.
///   b. Initiate new block fetches up to the concurrent limit.
///   c. Process completed block fetches:
///      - If the block is the next in sequence, emit it along with any cached blocks that follow.
///      - If not, cache the block and fetch its parent if necessary.
///   d. Refill the block fetching queue.
///   e. Brief sleep to allow other threads to update the latest slot.
///
/// This approach allows for efficient block fetching while maintaining the correct order of block processing.
pub fn get_poller_block_stream(
    client: Arc<RpcClientWithUri>,
    mut last_indexed_slot: u64,
    max_concurrent_block_fetches: usize,
) -> impl futures::Stream<Item = Vec<BlockInfo>> {
    stream! {
        start_latest_slot_updater(client.clone()).await;
        let mut block_fetching_futures = FuturesUnordered::new();
        let mut block_cache: BTreeMap<u64, BlockInfo> = BTreeMap::new();
        let mut in_process_slots = HashSet::new();
        let mut next_slot_to_fetch = match last_indexed_slot {
            0 => 0,
            last_indexed_slot => last_indexed_slot + 1
        };
        let mut skipped_slot_cache = LruCache::new(NonZeroUsize::new(1000).unwrap());


        loop {
            let current_slot = LATEST_SLOT.load(Ordering::SeqCst);

            // Refill the block fetching futures with new slots to fetch
            //
            // If we just continued from the last indexed slot + 1, we might get into an infinite retry loop when
            // we encounter a large number of skipped slots. To avoid that, when we refill the block fetching
            // futures, we continue from the max of the block fetched in the previous outer loop iteration and
            // the last indexed slot + 1.
            next_slot_to_fetch = max(next_slot_to_fetch, last_indexed_slot + 1);
            for _ in 0..max_concurrent_block_fetches {
                if next_slot_to_fetch > current_slot {
                    break;
                }
                if !skipped_slot_cache.contains(&next_slot_to_fetch) && is_slot_unprocessed(next_slot_to_fetch, &in_process_slots, &block_cache, last_indexed_slot) {
                    block_fetching_futures.push(fetch_block(
                        client.uri.clone(),
                        next_slot_to_fetch,
                        RETRIES
                    ));
                    in_process_slots.insert(next_slot_to_fetch);
                }
                next_slot_to_fetch += 1;
            }

            while let Some(block) = block_fetching_futures.next().await {
                let (block_result, slot) = block;
                in_process_slots.remove(&slot);
                if let Ok(block) = block_result {
                    if let None = block {
                        skipped_slot_cache.push(slot, true);
                        continue;
                    }
                    let block = block.unwrap();

                    let current_slot = LATEST_SLOT.load(Ordering::SeqCst);

                    // If the block is the next block to index, emit it and the consecutive blocks in the block cache
                    if block.metadata.slot == 0 || block.metadata.parent_slot == last_indexed_slot {
                        last_indexed_slot = block.metadata.slot;
                        let mut blocks_to_index = vec![block];
                        let (cached_blocks_to_index, last_indexed_slot_from_cache) = pop_cached_blocks_to_index(&mut block_cache, last_indexed_slot);
                        last_indexed_slot = last_indexed_slot_from_cache;
                        blocks_to_index.extend(cached_blocks_to_index);
                        let blocks_to_index_len = blocks_to_index.len();
                        metric! {
                            statsd_count!("rpc_block_emitted", blocks_to_index_len as i64);
                        }
                        yield blocks_to_index;
                    }
                    else {
                        let parent_slot = block.metadata.parent_slot;

                        // If the parent block is not processed, fetch it
                        if is_slot_unprocessed(parent_slot, &in_process_slots, &block_cache, last_indexed_slot) {
                            block_fetching_futures.push(fetch_block(
                                client.uri.clone(),
                                parent_slot,
                                INFINITY
                            ));
                            in_process_slots.insert(parent_slot);
                        }
                        block_cache.insert(block.metadata.slot, block.clone());
                    }



                    // Refill the block fetching futures with new slots to fetch
                    for next_slot in (last_indexed_slot + 1)..(slot + 1 + max_concurrent_block_fetches as u64) {
                        if in_process_slots.len() >= max_concurrent_block_fetches {
                            break;
                        }
                        if next_slot > current_slot {
                            break;
                        }
                        if !skipped_slot_cache.contains(&next_slot) && is_slot_unprocessed(next_slot, &in_process_slots, &block_cache, last_indexed_slot) {
                            block_fetching_futures.push(fetch_block(
                                client.uri.clone(),
                                next_slot,
                                RETRIES
                            ));
                            in_process_slots.insert(next_slot);
                        }
                    }
                }
            }
            // Sleep to give the chance to other thread to update LATEST_SLOT
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        }
    }
}

fn is_slot_unprocessed(
    slot: u64,
    in_process_slots: &HashSet<u64>,
    block_cache: &BTreeMap<u64, BlockInfo>,
    last_indexed_slot: u64,
) -> bool {
    !in_process_slots.contains(&slot)
        && !block_cache.contains_key(&slot)
        && slot > last_indexed_slot
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

pub async fn fetch_current_slot_with_infinite_retry(client: &RpcClient) -> u64 {
    loop {
        match client.get_slot().await {
            Ok(slot) => {
                return slot;
            }
            Err(e) => {
                log::error!("Failed to fetch current slot: {}", e);
                sleep(Duration::from_secs(5));
            }
        }
    }
}

pub async fn fetch_block(
    rpc_uri: String,
    slot: u64,
    retries: u64,
) -> (Result<Option<BlockInfo>, client_error::ClientError>, u64) {
    let mut attempt_counter = 0;
    loop {
        let timeout_sec = if attempt_counter == 0 { 3 } else { 20 };
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
                return (
                    Ok(Some(parse_ui_confirmed_blocked(block, slot).unwrap())),
                    slot,
                );
            }
            Err(e) => {
                if let solana_client::client_error::ClientErrorKind::RpcError(
                    RpcError::RpcResponseError { code, .. },
                ) = e.kind
                {
                    if SKIPPED_BLOCK_ERRORS.contains(&code) {
                        if retries == INFINITY {
                            log::error!(
                                "Got skipped block error for block that is supposed to exist: {}",
                                slot
                            );
                        } else {
                            metric! {
                                statsd_count!("rpc_skipped_block", 1);
                            }
                            log::info!("Skipped block: {}", slot);
                        }
                        return (Ok(None), slot);
                    }
                }
                log::debug!("Failed to fetch block: {}. {}", slot, e.to_string());
                attempt_counter += 1;
                if attempt_counter >= retries {
                    metric! {
                        statsd_count!("rpc_block_fetch_failed", 1);
                    }
                    return (Err(e), slot);
                }
            }
        }
    }
}

pub async fn update_latest_slot(rpc_client: Arc<RpcClientWithUri>) {
    let slot = fetch_current_slot_with_infinite_retry(&rpc_client.client).await;
    LATEST_SLOT.fetch_max(slot, Ordering::SeqCst);
}

pub async fn start_latest_slot_updater(rpc_client: Arc<RpcClientWithUri>) {
    if LATEST_SLOT.load(Ordering::SeqCst) != 0 {
        return;
    }
    update_latest_slot(rpc_client.clone()).await;
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            update_latest_slot(rpc_client.clone()).await;
        }
    });
}
