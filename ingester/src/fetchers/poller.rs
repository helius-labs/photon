use std::{sync::Arc, thread::sleep, time::Duration};

use futures::{stream, StreamExt};
use log::info;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_transaction_status::EncodedConfirmedBlock;

use crate::transaction_info::{parse_instruction_groups, TransactionInfo};

struct TransactionPoller {
    client: Arc<RpcClient>,
    slot: u64,
    max_concurrency: usize,
}

impl TransactionPoller {
    #[allow(dead_code)]
    pub async fn new(client: RpcClient, max_concurrency: usize) -> Self {
        let slot = client.get_slot().await.unwrap();
        Self {
            client: Arc::new(client),
            slot,
            max_concurrency,
        }
    }

    async fn fetch_new_blocks(&mut self) -> Vec<(EncodedConfirmedBlock, u64)> {
        let new_slot = self.client.get_slot().await.unwrap();
        let slots: Vec<_> = (self.slot..new_slot).collect();

        let blocks = stream::iter(slots)
            .map(|slot| {
                let slot = slot.clone();
                let client = self.client.clone();
                // TODO: Add retries for failed requests
                async move { (client.as_ref().get_block(slot).await.unwrap(), slot) }
            })
            .buffer_unordered(self.max_concurrency)
            .collect()
            .await;

        self.slot = new_slot;
        blocks
    }

    #[allow(dead_code)]
    pub async fn fetch_new_transactions(&mut self) -> Vec<TransactionInfo> {
        let mut transactions = vec![];
        let new_blocks = self.fetch_new_blocks().await;

        if new_blocks.len() == 0 {
            info!("No new blocks found");
            sleep(Duration::from_millis(100));
        }

        for (block, slot) in new_blocks {
            let block_time = block.block_time;
            for transaction in block.transactions {
                let instruction_groups = parse_instruction_groups(transaction);
                match instruction_groups {
                    Err(e) => {
                        log::error!("Failed to parse transaction: {}", e);
                    }
                    Ok(instruction_groups) => {
                        transactions.push(TransactionInfo {
                            slot,
                            block_time,
                            instruction_groups,
                        });
                    }
                }
            }
        }
        transactions
    }
}
