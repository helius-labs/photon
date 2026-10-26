use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use async_stream::stream;
use cadence_macros::statsd_count;
use futures::{stream, Stream, StreamExt};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

use crate::{
    ingester::{
        error::IngesterError,
        retry::Backoff,
        typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo},
    },
    metric,
    monitor::{start_latest_slot_updater, LATEST_SLOT},
};

const SLOT_PAGE: u64 = 4096;

/// Enumerate produced slots rather than interpreting provider errors as proof
/// that a slot was skipped. Ordered, finite batches bound fetch-ahead even when
/// a predecessor is unavailable. No later batch is scheduled before this one
/// has been consumed by the indexer.
pub fn get_block_poller_stream(
    rpc_client: Arc<RpcClient>,
    mut last_indexed_slot: u64,
    max_concurrent_block_fetches: usize,
) -> impl Stream<Item = Vec<BlockInfo>> {
    stream! {
        start_latest_slot_updater(rpc_client.clone()).await;
        let concurrency = max_concurrent_block_fetches.max(1);
        // Slot zero is the bootstrap checkpoint; genesis blocks may have no
        // timestamp or transaction metadata and contain no compression events.
        let mut next_slot = last_indexed_slot.saturating_add(1);
        let mut backoff = Backoff::default();
        loop {
            let head = LATEST_SLOT.load(Ordering::SeqCst);
            if next_slot > head {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            let end = head.min(next_slot.saturating_add(SLOT_PAGE - 1));
            let slots = match rpc_client.get_blocks_with_commitment(next_slot, Some(end), CommitmentConfig::confirmed()).await {
                Ok(slots) if slots.iter().all(|slot| (next_slot..=end).contains(slot))
                    && slots.windows(2).all(|pair| pair[0] < pair[1]) => slots,
                result => {
                    log::error!("getBlocks failed for {}-{}: {:?}", next_slot, end, result);
                    let error = match result {
                        Err(error) => IngesterError::from(error),
                        Ok(_) => IngesterError::ParserError("Invalid produced-slot order or range".into()),
                    };
                    crate::ingester::supervisor::record_source_failure(&error);
                    tokio::time::sleep(backoff.next_delay()).await;
                    continue;
                }
            };
            if slots.is_empty() {
                next_slot = end.saturating_add(1);
                continue;
            }
            let block_stream = stream::iter(slots.into_iter().take(concurrency))
                .map(|slot| fetch_block(rpc_client.clone(), slot))
                .buffered(concurrency);
            futures::pin_mut!(block_stream);
            let mut blocks = Vec::new();
            let mut failure = None;
            while let Some(result) = block_stream.next().await {
                match result {
                    Ok(block) => {
                        let expected_parent = blocks.last().map_or(last_indexed_slot, |block: &BlockInfo| block.metadata.slot);
                        if block.metadata.parent_slot != expected_parent {
                            if blocks.is_empty() {
                                // getBlocks may omit history on one provider. Walk the
                                // actual parent links, asking fallback RPCs as needed.
                                match repair_gap(rpc_client.clone(), block, last_indexed_slot, concurrency).await {
                                    Ok(repaired) => blocks = repaired,
                                    Err(error) => failure = Some(error),
                                }
                            } else {
                                // Commit the valid prefix, then repair on the next pass.
                                log::warn!("Parent gap after slot {}, restarting from that checkpoint", expected_parent);
                            }
                            break;
                        }
                        if blocks.last().is_some_and(|parent| parent.metadata.blockhash != block.metadata.parent_blockhash) {
                            failure = Some(IngesterError::ParserError(format!("Parent hash mismatch at slot {}", block.metadata.slot)));
                            break;
                        }
                        blocks.push(block);
                    }
                    Err(error) => { failure = Some(error); break; }
                }
            }
            if let Some(error) = failure {
                // An empty page on a lagging provider may have moved the scan
                // ahead of real blocks. Retry from the durable chain boundary.
                next_slot = last_indexed_slot.saturating_add(1);
                crate::ingester::supervisor::record_source_failure(&error);
                log::error!("Block fetch stopped after checkpoint {}: {} (retryable={})", last_indexed_slot, error, error.is_retryable());
                metric! { statsd_count!("rpc_block_fetch_failed", 1); }
                tokio::time::sleep(backoff.next_delay()).await;
            }
            if let Some(last) = blocks.last() {
                last_indexed_slot = last.metadata.slot;
                next_slot = last_indexed_slot.saturating_add(1);
                backoff = Backoff::default();
                metric! {
                    statsd_count!("rpc_block_emitted", blocks.len() as i64);
                }
                yield blocks;
            }
        }
    }
}

async fn repair_gap(
    rpc: Arc<RpcClient>,
    block: BlockInfo,
    checkpoint: u64,
    window: usize,
) -> Result<Vec<BlockInfo>, IngesterError> {
    let mut ancestors = vec![block];
    loop {
        let child = ancestors
            .last()
            .ok_or_else(|| IngesterError::ParserError("Empty parent chain".into()))?;
        let parent = child.metadata.parent_slot;
        if parent == checkpoint {
            ancestors.reverse();
            return Ok(ancestors);
        }
        if parent < checkpoint || parent >= child.metadata.slot {
            return Err(IngesterError::ParserError(format!("Cannot connect block {} to checkpoint {}; reindex or canonical-chain reconciliation required", child.metadata.slot, checkpoint)));
        }
        let predecessor = fetch_block(rpc.clone(), parent).await?;
        if predecessor.metadata.blockhash != child.metadata.parent_blockhash {
            return Err(IngesterError::ParserError(format!(
                "Parent hash mismatch at slot {}",
                parent
            )));
        }
        // Keep only the oldest window while walking back. A large, persistent
        // omission must still yield a prefix extending the committed checkpoint.
        // Discarded descendants will be fetched after that prefix is consumed.
        if ancestors.len() == window {
            ancestors.clear();
        }
        ancestors.push(predecessor);
    }
}

async fn fetch_block(rpc_client: Arc<RpcClient>, slot: u64) -> Result<BlockInfo, IngesterError> {
    let block = rpc_client
        .get_block_with_config(
            slot,
            RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                transaction_details: Some(TransactionDetails::Full),
                rewards: Some(false),
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(1),
            },
        )
        .await?;
    let block = parse_ui_confirmed_blocked(block, slot)?;
    metric! { statsd_count!("rpc_block_fetched", 1); }
    Ok(block)
}

/// Kept for callers fetching one block. A skipped response is unresolved until
/// the produced-slot listing or a child's parent link establishes the chain.
pub async fn fetch_block_with_infinite_retries(
    rpc_client: Arc<RpcClient>,
    slot: u64,
) -> Option<BlockInfo> {
    let mut backoff = Backoff::default();
    loop {
        match fetch_block(rpc_client.clone(), slot).await {
            Ok(block) => return Some(block),
            Err(error) => {
                log::error!(
                    "getBlock slot {} failed (attempt {}): {}",
                    slot,
                    backoff.attempts() + 1,
                    error
                );
                tokio::time::sleep(backoff.next_delay()).await;
            }
        }
    }
}
