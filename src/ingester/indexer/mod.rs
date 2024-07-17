use std::{sync::Arc, thread::sleep, time::Duration};

use async_std::stream::StreamExt;
use futures::pin_mut;
use log::info;
use sea_orm::{sea_query::Expr, DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::{
    dao::generated::blocks, ingester::fetchers::poller::fetch_current_slot_with_infinite_retry,
};

use super::fetchers::BlockStreamConfig;
const POST_BACKFILL_FREQUENCY: u64 = 100;
const PRE_BACKFILL_FREQUENCY: u64 = 10;

#[derive(FromQueryResult)]
pub struct OptionalContextModel {
    // Postgres and SQLlite do not support u64 as return type. We need to use i64 and cast it to u64.
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

pub async fn continously_index_new_blocks(
    block_stream_config: BlockStreamConfig,
    rpc_client: Arc<RpcClient>,
    last_indexed_slot_at_start: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let block_stream = block_stream_config.load_block_stream();
        pin_mut!(block_stream);
        let current_slot = fetch_current_slot_with_infinite_retry(rpc_client.as_ref()).await;
        let number_of_blocks_to_backfill = current_slot - last_indexed_slot_at_start;
        info!(
            "Backfilling historical blocks. Current number of blocks to backfill: {}",
            number_of_blocks_to_backfill
        );
        let mut last_indexed_slot = last_indexed_slot_at_start;

        let mut finished_backfill = false;

        loop {
            let block = block_stream.next().await.unwrap();
            let slot_indexed = block.metadata.slot;

            if !finished_backfill {
                let blocks_indexed = slot_indexed - last_indexed_slot_at_start;
                if blocks_indexed <= number_of_blocks_to_backfill {
                    if blocks_indexed % PRE_BACKFILL_FREQUENCY as u64 == 0 {
                        info!(
                            "Backfilled {} / {} blocks",
                            blocks_indexed, number_of_blocks_to_backfill
                        );
                    }
                } else {
                    finished_backfill = true;
                    info!("Finished backfilling historical blocks!");
                }
            } else {
                for slot in last_indexed_slot..slot_indexed {
                    if slot % POST_BACKFILL_FREQUENCY == 0 {
                        info!("Indexed slot {}", slot);
                    }
                }
            }

            last_indexed_slot = slot_indexed;
        }
    })
}
