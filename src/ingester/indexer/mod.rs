use std::{ops::Deref, sync::Arc, thread::sleep, time::Duration};

use log::info;
use sea_orm::DatabaseConnection;
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::Mutex;

use crate::ingester::fetchers::poller::{fetch_current_slot_with_infinite_retry, Options};

use super::{fetchers::poller::TransactionPoller, index_block_batch_with_infinite_retries};

pub struct Indexer {
    db: Arc<DatabaseConnection>,
    poller: TransactionPoller,
    max_batch_size: usize,
}

pub struct BackfillInfo {
    pub num_blocks_to_index: usize,
}

impl Indexer {
    pub async fn new(
        db: Arc<DatabaseConnection>,
        rpc_client: Arc<RpcClient>,
        is_localnet: bool,
        start_slot: Option<u64>,
        max_batch_size: usize,
    ) -> Self {
        let current_slot = fetch_current_slot_with_infinite_retry(rpc_client.as_ref()).await;
        let start_slot = match (start_slot, is_localnet) {
            (Some(start_slot), _) => start_slot,
            // Start indexing from the first slot for localnet.
            (None, true) => 0,
            (None, false) => current_slot,
        };
        let poller = TransactionPoller::new(rpc_client, Options { start_slot }).await;
        let number_of_blocks_to_backfill = current_slot - start_slot;
        info!(
            "Backfilling historical blocks. Current number of blocks to backfill: {}",
            number_of_blocks_to_backfill
        );
        if number_of_blocks_to_backfill > 10_000 && is_localnet {
            info!("Backfilling a large number of blocks. This may take a while. Considering restarting local validator or specifying a start slot.");
        }
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
                if blocks_indexed <= (backfill.num_blocks_to_index as usize) {
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
    let handle = tokio::spawn(async move {
        loop {
            indexer.deref().lock().await.index_latest_blocks(None).await;
            sleep(Duration::from_millis(20));
        }
    });
    handle
}
