use std::{
    sync::Arc,
    thread::sleep,
    time::Duration,
};

use async_stream::stream;
use futures::StreamExt;
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig, rpc_request::RpcError,
};

use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

use crate::{
    common::typedefs::rpc_client_with_uri::RpcClientWithUri,
    ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo},
};

const SKIPPED_BLOCK_ERRORS: [i64; 2] = [-32007, -32009];
const FAILED_BLOCK_LOGGING_FREQUENCY: u64 = 100;

pub fn get_poller_block_stream(
    client: Arc<RpcClientWithUri>,
    last_indexed_slot: u64,
    max_concurrent_block_fetches: usize,
    end_block_slot: Option<u64>,
) -> impl futures::Stream<Item = BlockInfo> {
    stream! {
        let mut current_slot_to_fetch = match last_indexed_slot {
            0 => 0,
            last_indexed_slot => last_indexed_slot + 1
        };
        let polls_forever = end_block_slot.is_none();
        let mut end_block_slot = end_block_slot.unwrap_or(fetch_current_slot_with_infinite_retry(&client.client).await);

        loop {
            if current_slot_to_fetch > end_block_slot  && !polls_forever {
                break;
            }

            while current_slot_to_fetch > end_block_slot {
                end_block_slot = fetch_current_slot_with_infinite_retry(&client.client).await;
                if end_block_slot <= current_slot_to_fetch {
                    sleep(Duration::from_millis(10));
                }
            }

            let mut block_fetching_futures_batch = vec![];
            while (block_fetching_futures_batch.len() < max_concurrent_block_fetches) && current_slot_to_fetch <= end_block_slot  {
                let client = client.clone();
                block_fetching_futures_batch.push(fetch_block_with_infinite_retry_using_arc(
                    client.clone(),
                    current_slot_to_fetch,
                ));
                current_slot_to_fetch += 1;
            }
            let blocks_to_yield = futures::future::join_all(block_fetching_futures_batch).await;
            let mut blocks_to_yield: Vec<_> = blocks_to_yield.into_iter().filter_map(|block| block).collect();

            blocks_to_yield.sort_by_key(|block| block.metadata.slot);
            for block in blocks_to_yield.drain(..) {
                yield block;
            }

        }
    }
}

pub async fn fetch_current_slot_with_infinite_retry(client: &RpcClient) -> u64 {
    loop {
        match client.get_slot().await {
            Ok(slot) => {
                return slot;
            }
            Err(e) => {
                log::error!("Failed to fetch current slot: {}", e);
                sleep(Duration::from_secs(5));
            }
        }
    }
}

pub async fn fetch_block_with_infinite_retry(
    client: &RpcClientWithUri,
    slot: u64,
) -> Option<BlockInfo> {
    let mut attempt_counter = 0;
    loop {
        let timeout_sec = if attempt_counter == 0 { 1 } else { 20 };
        let client = RpcClient::new_with_timeout_and_commitment(
            client.uri.clone(),
            Duration::from_secs(timeout_sec),
            CommitmentConfig::confirmed(),
        );
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
            Ok(block) => {
                return Some(parse_ui_confirmed_blocked(block, slot).unwrap());
            }
            Err(e) => {
                if let solana_client::client_error::ClientErrorKind::RpcError(
                    RpcError::RpcResponseError { code, .. },
                ) = e.kind
                {
                    if SKIPPED_BLOCK_ERRORS.contains(&code) {
                        log::warn!("Skipped block: {}", slot);
                        return None;
                    }
                }
                log::warn!("Failed to fetch block: {}. {}", slot, e.to_string());
                attempt_counter += 1;
            }
        }
    }
}

fn fetch_block_with_infinite_retry_using_arc(
    client: Arc<RpcClientWithUri>,
    slot: u64,
) -> impl futures::Future<Output = Option<BlockInfo>> {
    async move { fetch_block_with_infinite_retry(client.as_ref(), slot).await }
}
