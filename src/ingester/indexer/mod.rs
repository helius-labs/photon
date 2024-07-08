use std::{ops::Deref, sync::Arc, thread::sleep, time::Duration};

use log::info;
use sea_orm::{sea_query::Expr, DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::Mutex;

use crate::{
    dao::generated::blocks,
    ingester::fetchers::poller::{fetch_current_slot_with_infinite_retry, Options},
};

use super::{fetchers::poller::TransactionPoller, index_block_batch_with_infinite_retries};

pub struct Indexer {
    db: Arc<DatabaseConnection>,
    poller: TransactionPoller,
    max_batch_size: usize,
}

pub struct BackfillInfo {
    pub num_blocks_to_index: usize,
}

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
                log::error!("Failed to fetch current slot from datab: {}", e);
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

impl Indexer {
    pub async fn new(
        db: Arc<DatabaseConnection>,
        rpc_client: Arc<RpcClient>,
        start_slot: Option<u64>,
        max_batch_size: usize,
    ) -> Self {
        let current_slot = fetch_current_slot_with_infinite_retry(rpc_client.as_ref()).await;

        let start_slot = start_slot.unwrap_or(
            (fetch_last_indexed_slot_with_infinite_retry(db.as_ref())
                .await
                .unwrap_or({
                    let genesis_hash =
                        get_genesis_hash_with_infinite_retry(rpc_client.as_ref()).await;
                    match genesis_hash.as_str() {
                        "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG" => 310276132,
                        _ => 0,
                    }
                })
                + 1) as u64,
        );
        let poller = TransactionPoller::new(rpc_client, Options { start_slot }).await;
        let number_of_blocks_to_backfill = current_slot - start_slot;
        info!(
            "Backfilling historical blocks. Current number of blocks to backfill: {}",
            number_of_blocks_to_backfill
        );
        let mut indexer = Self {
            db,
            poller,
            max_batch_size,
        };

        indexer
            .index_latest_blocks(Some(BackfillInfo {
                num_blocks_to_index: number_of_blocks_to_backfill as usize,
            }))
            .await;

        indexer
    }

    pub async fn index_latest_blocks(&mut self, backfill: Option<BackfillInfo>) {
        let mut finished_initial_backfill = false;
        let mut blocks_indexed = 0;

        loop {
            let blocks = self.poller.fetch_new_block_batch(self.max_batch_size).await;
            if blocks.is_empty() {
                break;
            }
            blocks_indexed += blocks.len();
            index_block_batch_with_infinite_retries(self.db.as_ref(), blocks).await;

            if let Some(backfill) = &backfill {
                if blocks_indexed <= backfill.num_blocks_to_index {
                    info!(
                        "Backfilled {} / {} blocks",
                        blocks_indexed, backfill.num_blocks_to_index
                    );
                } else {
                    if !finished_initial_backfill {
                        info!("Backfilling new blocks since backfill started...");
                        finished_initial_backfill = true;
                    }
                    info!("Backfilled {} blocks", blocks_indexed);
                }
            }
        }
    }
}

pub async fn continously_run_indexer(indexer: Arc<Mutex<Indexer>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            indexer.deref().lock().await.index_latest_blocks(None).await;
            sleep(Duration::from_millis(100));
        }
    })
}
