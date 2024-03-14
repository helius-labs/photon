use std::{sync::Arc, thread::sleep, time::Duration};

use futures::{stream, StreamExt};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

use crate::ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo};

pub struct TransactionPoller {
    client: Arc<RpcClient>,
    slot: u64,
}

pub struct Options {
    pub start_slot: u64,
    pub max_block_fetching_concurrency: usize,
}

pub async fn fetch_current_slot_with_infinite_retry(client: &RpcClient) -> u64 {
    loop {
        match client.get_slot().await {
            Ok(slot) => return slot,
            Err(e) => {
                log::error!("Failed to fetch current slot: {}", e);
                sleep(Duration::from_secs(5));
            }
        }
    }
}

pub async fn fetch_block_with_infinite_retry(client: &RpcClient, slot: u64) -> BlockInfo {
    loop {
        match client
            .get_block_with_config(
                slot,
                RpcBlockConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    transaction_details: Some(TransactionDetails::Full),
                    rewards: None,
                    commitment: Some(CommitmentConfig::confirmed()),
                    max_supported_transaction_version: Some(0),
                    ..RpcBlockConfig::default()
                },
            )
            .await
        {
            // Panic if RPC does not return blocks in the expected format
            Ok(block) => return parse_ui_confirmed_blocked(block, slot).unwrap(),
            Err(e) => {
                log::error!("Failed to fetch block: {}", e);
                sleep(Duration::from_secs(1));
            }
        }
    }
}

impl TransactionPoller {
    pub async fn new(client: Arc<RpcClient>, options: Options) -> Self {
        Self {
            client,
            slot: options.start_slot,
        }
    }

    pub async fn fetch_new_block_batch(&mut self, batch_size: usize) -> Vec<BlockInfo> {
        let new_slot = fetch_current_slot_with_infinite_retry(self.client.as_ref()).await;
        let slots: Vec<_> = (self.slot..new_slot).collect();

        let mut blocks: Vec<BlockInfo> = stream::iter(slots)
            .map(|slot| {
                let slot = slot.clone();
                let client = self.client.clone();
                async move { fetch_block_with_infinite_retry(client.as_ref(), slot).await }
            })
            .buffer_unordered(batch_size)
            .collect()
            .await;
        blocks.sort_by(|a, b| a.slot.cmp(&b.slot));

        self.slot = new_slot;
        blocks
    }
}
