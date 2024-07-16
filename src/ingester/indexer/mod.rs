use std::{sync::Arc, thread::sleep, time::Duration};

use async_std::stream::StreamExt;
use log::info;
use sea_orm::{sea_query::Expr, DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::{
    dao::generated::blocks, ingester::fetchers::poller::fetch_current_slot_with_infinite_retry,
};

use super::typedefs::block_info::BlockInfo;

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
    mut block_stream: impl futures::Stream<Item = BlockInfo>
        + std::marker::Send
        + 'static
        + std::marker::Unpin,
    rpc_client: Arc<RpcClient>,
    start_slot: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let current_slot = fetch_current_slot_with_infinite_retry(rpc_client.as_ref()).await;
        let number_of_blocks_to_backfill = current_slot - start_slot;
        info!(
            "Backfilling historical blocks. Current number of blocks to backfill: {}",
            number_of_blocks_to_backfill
        );

        let mut finished_backfill = false;

        loop {
            let block = block_stream.next().await.unwrap();
            let slot_indexed = block.metadata.slot;
            let blocks_indexed = slot_indexed - start_slot + 1;

            info!(
                "Slot indexed: {} / {}",
                blocks_indexed, number_of_blocks_to_backfill,
            );

            if !finished_backfill {
                if blocks_indexed <= number_of_blocks_to_backfill {
                    if blocks_indexed % 10 as u64 == 0 {
                        info!(
                            "Backfilled {} / {} blocks",
                            blocks_indexed, number_of_blocks_to_backfill
                        );
                    }
                } else {
                    finished_backfill = true;
                    info!("Finished backfilling historical blocks!");
                }
            }
        }
    })
}
