use std::{sync::Arc, time::Duration};

use async_std::stream::StreamExt;
use futures::{pin_mut, Stream};
use log::info;
use sea_orm::{sea_query::Expr, DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::{dao::generated::blocks, ingester::index_block_batch_with_infinite_retries};

use super::typedefs::block_info::BlockInfo;
use super::{error::IngesterError, supervisor::record_commit};
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
            Ok(context) => return context.and_then(|context| context.slot),
            Err(e) => {
                log::error!("Failed to fetch current slot from database: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
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
) -> Result<(), IngesterError> {
    pin_mut!(block_stream);
    let current_slot = match end_slot {
        Some(slot) => slot,
        None => tokio::time::timeout(Duration::from_secs(5), rpc_client.get_slot())
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(last_indexed_slot_at_start),
    };
    let number_of_blocks_to_backfill = current_slot.saturating_sub(last_indexed_slot_at_start);
    info!(
        "Backfilling historical blocks. Current number of blocks to backfill: {}",
        number_of_blocks_to_backfill
    );
    let mut last_indexed_slot = last_indexed_slot_at_start;

    let mut finished_backfill_slot = None;

    while let Some(blocks) = block_stream.next().await {
        let Some(last_slot_in_block) = blocks.last().map(|block| block.metadata.slot) else {
            continue;
        };
        index_block_batch_with_infinite_retries(db.as_ref(), blocks, rpc_client.as_ref()).await?;
        record_commit();

        for slot in (last_indexed_slot + 1)..(last_slot_in_block + 1) {
            let blocks_indexed = slot - last_indexed_slot_at_start;
            if blocks_indexed < number_of_blocks_to_backfill {
                if blocks_indexed.is_multiple_of(PRE_BACKFILL_FREQUENCY) {
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
    Ok(())
}
