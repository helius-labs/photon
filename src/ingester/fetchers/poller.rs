use std::{sync::Arc, thread::sleep, time::Duration};

use anyhow::{anyhow, Result};
use futures::{stream, StreamExt};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_sdk::{
    clock::UnixTimestamp, commitment_config::CommitmentConfig, transaction::VersionedTransaction,
};
use solana_transaction_status::{
    EncodedTransactionWithStatusMeta, TransactionDetails, UiConfirmedBlock, UiTransactionEncoding,
};

use super::super::transaction_info::{parse_instruction_groups, TransactionInfo};

pub struct TransactionPoller {
    client: Arc<RpcClient>,
    slot: u64,
    max_block_fetching_concurrency: usize,
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

pub async fn fetch_block_with_infinite_retry(client: &RpcClient, slot: u64) -> UiConfirmedBlock {
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
            Ok(block) => return block,
            Err(e) => {
                log::error!("Failed to fetch block: {}", e);
                sleep(Duration::from_secs(1));
            }
        }
    }
}

impl TransactionPoller {
    #[allow(dead_code)]
    pub async fn new(client: Arc<RpcClient>, options: Options) -> Self {
        Self {
            client,
            slot: options.start_slot,
            max_block_fetching_concurrency: options.max_block_fetching_concurrency,
        }
    }

    async fn fetch_new_blocks(&mut self) -> Vec<(UiConfirmedBlock, u64)> {
        let new_slot = fetch_current_slot_with_infinite_retry(self.client.as_ref()).await;
        let slots: Vec<_> = (self.slot..new_slot).collect();

        let blocks = stream::iter(slots)
            .map(|slot| {
                let slot = slot.clone();
                let client = self.client.clone();
                // TODO: Add retries for failed requests
                async move {
                    (
                        fetch_block_with_infinite_retry(client.as_ref(), slot).await,
                        slot,
                    )
                }
            })
            .buffer_unordered(self.max_block_fetching_concurrency)
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
            sleep(Duration::from_millis(100));
        }

        for (block, slot) in new_blocks {
            let block_time = block.block_time;
            for transaction in block.transactions.unwrap_or(Vec::new()) {
                let parsed_transaction = _parse_transaction(transaction, slot, block_time);
                match parsed_transaction {
                    Ok(transaction) => {
                        transactions.push(transaction);
                    }
                    Err(e) => {
                        log::error!("Failed to parse transaction: {}", e);
                    }
                }
            }
        }
        transactions
    }
}

fn _parse_transaction(
    transaction: EncodedTransactionWithStatusMeta,
    slot: u64,
    block_time: Option<UnixTimestamp>,
) -> Result<TransactionInfo> {
    let EncodedTransactionWithStatusMeta {
        transaction, meta, ..
    } = transaction;
    let versioned_transaction: VersionedTransaction = transaction
        .decode()
        .ok_or(anyhow!("Transaction cannot be decoded".to_string()))?;

    let signature = versioned_transaction.signatures[0];
    let instruction_groups = parse_instruction_groups(versioned_transaction, meta)?;
    Ok(TransactionInfo {
        slot: slot,
        block_time: block_time,
        instruction_groups,
        signature,
    })
}
