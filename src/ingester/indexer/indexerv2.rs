use std::{sync::Arc, thread::sleep, time::Duration};

use async_std::stream::StreamExt;
use futures::pin_mut;
use log::info;
use sea_orm::{sea_query::Expr, DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::{
    api::method::get_indexer_health::HEALTH_CHECK_SLOT_DISTANCE,
    dao::generated::blocks,
    ingester::fetchers::poller::{fetch_current_slot_with_infinite_retry, get_block_stream},
};

use super::index_block_batch_with_infinite_retries;

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

async fn get_genesis_hash_with_infinite_retry(rpc_client: &RpcClient) -> String {
    loop {
        match rpc_client.get_genesis_hash().await {
            Ok(genesis_hash) => return genesis_hash.to_string(),
            Err(e) => {
                log::error!("Failed to fetch genesis hash: {}", e);
                sleep(Duration::from_secs(5));
            }
        }
    }
}

pub async fn continously_index_new_blocks(
    db: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    start_slot: Option<u64>,
    max_concurrent_block_fetches: usize,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut current_slot = fetch_current_slot_with_infinite_retry(rpc_client.as_ref()).await;

        // let mut start_slot = start_slot.unwrap_or(
        //     (fetch_last_indexed_slot_with_infinite_retry(db.as_ref())
        //         .await
        //         .unwrap_or({
        //             let genesis_hash =
        //                 get_genesis_hash_with_infinite_retry(rpc_client.as_ref()).await;
        //             match genesis_hash.as_str() {
        //                 "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG" => 310276132,
        //                 _ => 0,
        //             }
        //         })
        //         + 1) as u64,
        // );
        let mut start_slot = current_slot - 10;

        let mut number_of_blocks_to_backfill = current_slot - start_slot;
        info!(
            "Backfilling historical blocks. Current number of blocks to backfill: {}",
            number_of_blocks_to_backfill
        );

        let block_stream =
            get_block_stream(rpc_client.clone(), start_slot, max_concurrent_block_fetches);
        pin_mut!(block_stream);
        let mut finished_backfill = false;

        loop {
            let block = block_stream.next().await.unwrap();
            let slot_indexed = block.metadata.slot;
            let latest_current_slot =
                fetch_current_slot_with_infinite_retry(rpc_client.as_ref()).await;
            index_block_batch_with_infinite_retries(db.as_ref(), vec![block]).await;
            let health = latest_current_slot - slot_indexed;

            let blocks_indexed = slot_indexed - start_slot + 1;

            // info!(
            //     "Slot indexed: {} / {}",
            //     blocks_indexed, number_of_blocks_to_backfill,
            // );
            info!("Lagging behind by {} slots", health);
            // info!("Slot indexed: {}", slot_indexed);

            // if !finished_backfill {
            // if blocks_indexed <= number_of_blocks_to_backfill {
            //     if blocks_indexed % max_concurrent_block_fetches as u64 == 0 {
            //         info!(
            //             "Backfilled {} / {} blocks",
            //             blocks_indexed, number_of_blocks_to_backfill
            //         );
            //     }
            // }
        }
    })
}
