use std::{cmp::min, sync::Arc, thread::sleep, time::Duration};

use futures::{stream, StreamExt};
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig, rpc_request::RpcError,
};

use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

use crate::ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo};

const SKIPPED_BLOCK_ERROR: i64 = -32007;

pub struct TransactionPoller {
    client: Arc<RpcClient>,
    next_slot: u64,
}

pub struct Options {
    pub start_slot: u64,
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

pub async fn fetch_block_with_infinite_retry(client: &RpcClient, slot: u64) -> Option<BlockInfo> {
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
                },
            )
            .await
        {
            // Panic if RPC does not return blocks in the expected format
            Ok(block) => return Some(parse_ui_confirmed_blocked(block, slot).unwrap()),
            Err(e) => {
                if let solana_client::client_error::ClientErrorKind::RpcError(
                    RpcError::RpcResponseError {
                        code: SKIPPED_BLOCK_ERROR,
                        ..
                    },
                ) = e.kind
                {
                    log::warn!("Block skipped: {}", slot);
                    return None;
                }
                log::error!("Failed to fetch block: {}. {}", slot, e.to_string());
                sleep(Duration::from_secs(5));
            }
        }
    }
}

impl TransactionPoller {
    pub async fn new(client: Arc<RpcClient>, options: Options) -> Self {
        Self {
            client,
            next_slot: options.start_slot,
        }
    }

    pub async fn fetch_new_block_batch(&mut self, max_batch_size: usize) -> Vec<BlockInfo> {
        let current_slot = fetch_current_slot_with_infinite_retry(self.client.as_ref()).await;
        let new_next_slot = min(current_slot + 1, self.next_slot + max_batch_size as u64);
        let slots: Vec<_> = ((self.next_slot)..(new_next_slot)).collect();

        let mut blocks: Vec<BlockInfo> = stream::iter(slots)
            .map(|slot| {
                let client = self.client.clone();
                async move { fetch_block_with_infinite_retry(client.as_ref(), slot).await }
            })
            .buffer_unordered(max_batch_size)
            .filter_map(|block| async move { block })
            .collect()
            .await;

        blocks.sort_by(|a, b| a.metadata.slot.cmp(&b.metadata.slot));

        self.next_slot = new_next_slot;
        blocks
    }
}
