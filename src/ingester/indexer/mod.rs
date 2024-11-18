use std::{sync::Arc, time::Duration};

use async_std::stream::StreamExt;
use futures::{pin_mut, Stream};
use log::info;
use sea_orm::FromQueryResult;
use solana_client::nonblocking::rpc_client::RpcClient;
use sqlx::types::chrono::Utc;

use crate::common::fetch_current_slot_with_infinite_retry;

use super::typedefs::block_info::BlockInfo;
const POST_BACKFILL_FREQUENCY: u64 = 10;
const PRE_BACKFILL_FREQUENCY: u64 = 10;

#[derive(FromQueryResult)]
pub struct OptionalContextModel {
    // Postgres and SQLlite do not support u64 as return type. We need to use i64 and cast it to u64.
    pub slot: Option<i64>,
}

pub async fn index_block_stream(
    block_stream: impl Stream<Item = Vec<BlockInfo>>,
    rpc_client: Arc<RpcClient>,
    last_indexed_slot_at_start: u64,
) {
    pin_mut!(block_stream);
    let current_slot = fetch_current_slot_with_infinite_retry(&rpc_client).await;
    let number_of_blocks_to_backfill = if current_slot > last_indexed_slot_at_start {
        current_slot - last_indexed_slot_at_start
    } else {
        0
    };
    info!(
        "Backfilling historical blocks. Current number of blocks to backfill: {}",
        number_of_blocks_to_backfill
    );
    let mut last_indexed_slot = last_indexed_slot_at_start;

    let mut finished_backfill_slot = None;
    let mut slot_latencies = Vec::new();

    while let Some(blocks) = block_stream.next().await {
        let last_slot_in_block_batch = blocks.last().unwrap().metadata.slot;

        // Get utc time now in seconds
        let now = Utc::now().timestamp_millis();
        for block in blocks {
            let block_time = block.metadata.block_time * 1000; // Convert seconds to milliseconds
            slot_latencies.push(now - block_time);
            let num_latencies_to_track = 30;
            if slot_latencies.len() > num_latencies_to_track {
                slot_latencies.remove(0);
            }
            let avg_latency = slot_latencies.iter().sum::<i64>() / slot_latencies.len() as i64;
            let duration = Duration::from_millis(avg_latency as u64);
            info!(
                "Average latency: {:?} for the last {} blocks",
                duration, num_latencies_to_track
            );
        }

        for slot in (last_indexed_slot + 1)..(last_slot_in_block_batch + 1) {
            let blocks_indexed = slot - last_indexed_slot_at_start;
            if blocks_indexed < number_of_blocks_to_backfill {
                if blocks_indexed % PRE_BACKFILL_FREQUENCY == 0 {
                    info!(
                        "Backfilled {} / {} blocks",
                        blocks_indexed, number_of_blocks_to_backfill
                    );
                }
            } else {
                if finished_backfill_slot.is_none() {
                    info!("Finished backfilling historical blocks!");
                    info!("Starting to index new blocks...");
                    finished_backfill_slot = Some(slot);
                }
                if slot % POST_BACKFILL_FREQUENCY == 0 {
                    info!("Indexed slot {}", slot);
                }
            }
            last_indexed_slot = slot;
        }
    }
}
