use std::{
    collections::{BTreeMap, VecDeque},
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};

use async_stream::stream;
use cadence_macros::statsd_count;
use futures::{pin_mut, Stream, StreamExt};
use sea_orm::{DatabaseConnection, EntityTrait};
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig, rpc_request::RpcError,
};

use solana_commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use tokio::time::{sleep, timeout, Instant};

use crate::{
    dao::generated::blocks,
    ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo},
    metric,
    monitor::{start_latest_slot_updater, LATEST_SLOT},
};

/// RPC error codes that *claim* a slot was skipped. -32009 explicitly conflates
/// "slot was skipped" with "missing in long-term storage", so a single response
/// carrying one of these codes is never trusted on its own — see
/// `fetch_slot_with_infinite_retries`.
const SKIPPED_BLOCK_ERRORS: [i64; 3] = [-32007, -32009, -32004];

/// A slot is classified as skipped only after every source returned a
/// skip-coded error this many times, with backoff between rounds.
const SKIP_CONFIRMATION_ROUNDS: usize = 3;
const SKIP_CONFIRMATION_BACKOFF: Duration = Duration::from_millis(500);

/// Non-skip fetch errors are retried forever, but never silently.
const FETCH_ERROR_LOG_EVERY: u64 = 10;

/// Upper bound on out-of-order blocks held in memory while waiting for the
/// next in-chain block. Converts an unbounded balloon (historically an OOM
/// kill hours after a stall) into a bounded, observable stall.
const MAX_CACHED_BLOCKS: usize = 25_000;

/// If the cache holds blocks but nothing has chained for this long, the head
/// of the cache references a block that never arrived: either a mis-classified
/// skip or a block lost from the RPC source. Triggers gap resolution.
const GAP_STALL_THRESHOLD: Duration = Duration::from_secs(30);

/// Where blocks are fetched from and how a stalled gap is adjudicated.
#[derive(Clone)]
pub struct BlockFetchSources {
    pub primary: Arc<RpcClient>,
    /// Consulted, in order, whenever the primary claims a slot is skipped or
    /// fails persistently (e.g. public RPC endpoints backing a private one).
    pub fallbacks: Vec<Arc<RpcClient>>,
    /// Used during gap resolution to verify that a parent-referenced block
    /// which no source can serve was already indexed by a previous run.
    pub db: Option<Arc<DatabaseConnection>>,
}

impl BlockFetchSources {
    pub fn all_sources(&self) -> Vec<Arc<RpcClient>> {
        std::iter::once(self.primary.clone())
            .chain(self.fallbacks.iter().cloned())
            .collect()
    }
}

enum SlotFetchOutcome {
    Block(Box<BlockInfo>),
    /// Every source repeatedly confirmed no block exists at this slot.
    Skipped,
}

fn get_slot_stream(
    rpc_client: Arc<RpcClient>,
    start_slot: u64,
    refetch_queue: Arc<Mutex<VecDeque<u64>>>,
) -> impl Stream<Item = u64> {
    stream! {
        start_latest_slot_updater(rpc_client.clone()).await;
        let mut next_slot_to_fetch = start_slot;
        loop {
            let refetch_slot = refetch_queue.lock().expect("refetch queue lock poisoned").pop_front();
            if let Some(slot) = refetch_slot {
                yield slot;
                continue;
            }
            if next_slot_to_fetch > LATEST_SLOT.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            yield next_slot_to_fetch;
            next_slot_to_fetch += 1;
        }
    }
}

pub fn get_block_poller_stream(
    sources: BlockFetchSources,
    mut last_indexed_slot: u64,
    max_concurrent_block_fetches: usize,
) -> impl Stream<Item = Vec<BlockInfo>> {
    stream! {
        let start_slot = match last_indexed_slot {
            0 => 0,
            last_indexed_slot => last_indexed_slot + 1
        };
        let refetch_queue: Arc<Mutex<VecDeque<u64>>> = Arc::new(Mutex::new(VecDeque::new()));
        let slot_stream = get_slot_stream(
            sources.primary.clone(),
            start_slot,
            refetch_queue.clone(),
        );
        pin_mut!(slot_stream);
        let fetch_sources = sources.clone();
        let block_stream = slot_stream
            .map(move |slot| {
                let sources = fetch_sources.clone();
                async move { (slot, fetch_slot_with_infinite_retries(&sources, slot).await) }
            })
            .buffer_unordered(max_concurrent_block_fetches);
        pin_mut!(block_stream);
        let mut block_cache: BTreeMap<u64, BlockInfo> = BTreeMap::new();
        let mut last_progress = Instant::now();
        loop {
            if block_cache.len() < MAX_CACHED_BLOCKS {
                match timeout(Duration::from_secs(5), block_stream.next()).await {
                    Ok(Some((slot, SlotFetchOutcome::Block(block)))) => {
                        block_cache.insert(slot, *block);
                    }
                    Ok(Some((_, SlotFetchOutcome::Skipped))) => {}
                    Ok(None) => break,
                    Err(_) => {} // periodic wakeup so gap resolution runs even when quiet
                }
            } else {
                metric! {
                    statsd_count!("block_cache_at_capacity", 1);
                }
                sleep(Duration::from_secs(1)).await;
            }
            let (blocks_to_index, last_indexed_slot_from_cache) =
                pop_cached_blocks_to_index(&mut block_cache, last_indexed_slot);
            last_indexed_slot = last_indexed_slot_from_cache;
            metric! {
                statsd_count!("rpc_block_emitted", blocks_to_index.len() as i64);
            }
            if !blocks_to_index.is_empty() {
                last_progress = Instant::now();
                yield blocks_to_index;
            } else if !block_cache.is_empty() && last_progress.elapsed() > GAP_STALL_THRESHOLD {
                last_indexed_slot = resolve_gap(
                    &sources,
                    &mut block_cache,
                    last_indexed_slot,
                    &refetch_queue,
                )
                .await;
                last_progress = Instant::now();
            }
        }
    }
}

/// The head of the cache references block(s) that never entered the cache.
/// Walk the parent chain downward from that referenced block toward the
/// cursor. Every link on the walk is, by construction, a published block
/// (each is named as a parent by the block above it). Each link is either
/// re-fetched from some source (recovers mis-classified skips), or verified
/// already indexed in the DB by a previous run (nothing is lost by advancing
/// over it). Only if a link is unrecoverable from every source AND absent
/// from the DB do we refuse to advance: that is real data loss, and silently
/// skipping a published block is never acceptable.
async fn resolve_gap(
    sources: &BlockFetchSources,
    block_cache: &mut BTreeMap<u64, BlockInfo>,
    last_indexed_slot: u64,
    refetch_queue: &Arc<Mutex<VecDeque<u64>>>,
) -> u64 {
    const MAX_GAP_WALK: usize = 10_000;
    let Some((&min_cached_slot, min_cached_block)) = block_cache.iter().next() else {
        return last_indexed_slot;
    };
    let gap_end = min_cached_block.metadata.parent_slot;
    if gap_end <= last_indexed_slot {
        return last_indexed_slot;
    }
    log::error!(
        "Chain gap detected: cached block {} references parent {} but cursor is at {}; \
         walking the parent chain to resolve",
        min_cached_slot,
        gap_end,
        last_indexed_slot,
    );
    metric! {
        statsd_count!("chain_gap_detected", 1);
    }
    // Walk downward: each entry is (slot, block if re-fetched from RPC).
    let mut links: Vec<(u64, Option<BlockInfo>)> = Vec::new();
    let mut target = gap_end;
    while target > last_indexed_slot {
        if links.len() >= MAX_GAP_WALK {
            log::error!(
                "Gap walk from {} exceeded {} links without reaching cursor {}; \
                 refusing to advance",
                gap_end,
                MAX_GAP_WALK,
                last_indexed_slot
            );
            return last_indexed_slot;
        }
        match fetch_slot_once_from_any_source(sources, target).await {
            Some(block) => {
                let parent = block.metadata.parent_slot;
                log::warn!(
                    "Recovered mis-classified published block at slot {} during gap resolution",
                    target
                );
                metric! {
                    statsd_count!("published_block_recovered", 1);
                }
                links.push((target, Some(block)));
                target = parent;
            }
            None => match fetch_indexed_parent_slot(sources, target).await {
                Some(parent) => {
                    log::warn!(
                        "Published block at slot {} is unrecoverable from all RPC sources \
                         but already indexed by a previous run; advancing over it",
                        target
                    );
                    metric! {
                        statsd_count!("published_block_satisfied_by_db", 1);
                    }
                    links.push((target, None));
                    target = parent;
                }
                None => {
                    log::error!(
                        "PUBLISHED BLOCK DATA LOSS: slot {} is referenced as a parent in \
                         the chain below slot {} but no RPC source can serve it and it is \
                         not in the database. Refusing to skip a published block; will \
                         keep re-fetching.",
                        target,
                        min_cached_slot,
                    );
                    metric! {
                        statsd_count!("published_block_data_loss", 1);
                    }
                    refetch_queue
                        .lock()
                        .expect("refetch queue lock poisoned")
                        .push_back(target);
                    return last_indexed_slot;
                }
            },
        }
    }
    if target != last_indexed_slot {
        log::error!(
            "Gap walk from {} landed on parent {} below cursor {}; possible fork or \
             cursor inconsistency, refusing to advance",
            gap_end,
            target,
            last_indexed_slot
        );
        return last_indexed_slot;
    }
    // Apply links bottom-up: DB-satisfied links advance the cursor directly;
    // re-fetched links enter the cache and chain via pop_cached_blocks_to_index.
    let mut cursor = last_indexed_slot;
    for (slot, block) in links.into_iter().rev() {
        match block {
            Some(block) => {
                block_cache.insert(slot, block);
                break; // pop_cached chains from here once the cursor reaches its parent
            }
            None => cursor = slot,
        }
    }
    cursor
}

async fn fetch_indexed_parent_slot(sources: &BlockFetchSources, slot: u64) -> Option<u64> {
    let db = sources.db.as_ref()?;
    loop {
        match blocks::Entity::find_by_id(slot as i64).one(db.as_ref()).await {
            Ok(row) => return row.map(|r| r.parent_slot as u64),
            Err(e) => {
                log::error!("Failed to check DB for slot {}: {}", slot, e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// One verification pass over every source. Returns the block if any source
/// has it; None if all sources say skipped/unavailable.
async fn fetch_slot_once_from_any_source(
    sources: &BlockFetchSources,
    slot: u64,
) -> Option<BlockInfo> {
    for rpc_client in sources.all_sources() {
        if let FetchResult::Fetched(block) = fetch_block_once(&rpc_client, slot).await {
            return Some(block);
        }
    }
    None
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

enum FetchResult {
    Fetched(BlockInfo),
    SkipClaimed,
    Error(String),
}

async fn fetch_block_once(rpc_client: &RpcClient, slot: u64) -> FetchResult {
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
        Ok(block) => match parse_ui_confirmed_blocked(block, slot) {
            Ok(block) => FetchResult::Fetched(block),
            Err(e) => {
                // A block we cannot parse can never be indexed correctly;
                // dying loudly beats a silently dead indexer task.
                log::error!("FATAL: failed to parse block at slot {}: {}", slot, e);
                std::process::exit(1);
            }
        },
        Err(e) => {
            if let solana_client::client_error::ClientErrorKind::RpcError(
                RpcError::RpcResponseError { code, .. },
            ) = *e.kind
            {
                if SKIPPED_BLOCK_ERRORS.contains(&code) {
                    return FetchResult::SkipClaimed;
                }
            }
            FetchResult::Error(e.to_string())
        }
    }
}

async fn fetch_slot_with_infinite_retries(
    sources: &BlockFetchSources,
    slot: u64,
) -> SlotFetchOutcome {
    let rpc_clients = sources.all_sources();
    let mut skip_rounds = 0;
    let mut error_attempts: u64 = 0;
    loop {
        let mut all_sources_claim_skip = true;
        for rpc_client in &rpc_clients {
            match fetch_block_once(rpc_client, slot).await {
                FetchResult::Fetched(block) => {
                    metric! {
                        statsd_count!("rpc_block_fetched", 1);
                    }
                    return SlotFetchOutcome::Block(Box::new(block));
                }
                FetchResult::SkipClaimed => {}
                FetchResult::Error(e) => {
                    all_sources_claim_skip = false;
                    error_attempts += 1;
                    metric! {
                        statsd_count!("rpc_block_fetch_failed", 1);
                    }
                    if error_attempts % FETCH_ERROR_LOG_EVERY == 0 {
                        log::error!(
                            "Fetching block {} keeps failing (attempt {}): {}",
                            slot,
                            error_attempts,
                            e
                        );
                    }
                }
            }
        }
        if all_sources_claim_skip {
            skip_rounds += 1;
            if skip_rounds >= SKIP_CONFIRMATION_ROUNDS {
                metric! {
                    statsd_count!("rpc_skipped_block", 1);
                }
                log::info!(
                    "Skipped block: {} (confirmed by {} sources x {} rounds)",
                    slot,
                    rpc_clients.len(),
                    SKIP_CONFIRMATION_ROUNDS
                );
                return SlotFetchOutcome::Skipped;
            }
        }
        sleep(SKIP_CONFIRMATION_BACKOFF).await;
    }
}
