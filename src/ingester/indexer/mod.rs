use std::{sync::Arc, thread::sleep, time::Duration};

use async_std::stream::StreamExt;
use futures::{pin_mut, Stream};
use log::info;
use sea_orm::{sea_query::Expr, DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::{
    common::fetch_current_slot_with_infinite_retry, dao::generated::blocks,
    ingester::index_block_batch_with_infinite_retries,
};

use super::typedefs::block_info::BlockInfo;
const POST_BACKFILL_FREQUENCY: u64 = 10;
const PRE_BACKFILL_FREQUENCY: u64 = 10;

#[derive(FromQueryResult)]
pub struct OptionalContextModel {
    // Postgres and SQLite do not support u64 as return type. We need to use i64 and cast it to u64.
    pub slot: Option<i64>,
}

pub async fn fetch_last_indexed_slot_with_infinite_retry(
    db_conn: &DatabaseConnection,
) -> Option<i64> {
    loop {
        let context = blocks::Entity::find()
            .select_only()
            .column_as(Expr::col(blocks::Column::Slot).max(), "slot")
            .into_model::<OptionalContextModel>()
            .one(db_conn)
            .await;

        match context {
            Ok(context) => {
                return context
                    .expect("Always expected maximum query to return a result")
                    .slot
            }
            Err(e) => {
                log::error!("Failed to fetch current slot from database: {}", e);
                sleep(Duration::from_secs(5));
            }
        }
    }
}

pub async fn index_block_stream(
    block_stream: impl Stream<Item = Vec<BlockInfo>>,
    db: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    last_indexed_slot_at_start: u64,
    end_slot: Option<u64>,
) {
    pin_mut!(block_stream);
    let current_slot =
        end_slot.unwrap_or(fetch_current_slot_with_infinite_retry(&rpc_client).await);
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

    while let Some(blocks) = block_stream.next().await {
        let last_slot_in_block = blocks.last().unwrap().metadata.slot;
        index_block_batch_with_infinite_retries(db.as_ref(), blocks).await;

        for slot in (last_indexed_slot + 1)..(last_slot_in_block + 1) {
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
