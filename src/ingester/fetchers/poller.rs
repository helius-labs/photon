use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    thread::sleep,
    time::Duration,
};

use async_stream::stream;
use futures::{stream::FuturesUnordered, StreamExt};
use solana_client::{
    client_error, nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig,
    rpc_request::RpcError,
};

use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

use crate::{
    common::typedefs::rpc_client_with_uri::RpcClientWithUri,
    ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo},
};

const SKIPPED_BLOCK_ERRORS: [i64; 2] = [-32007, -32009];
const RETRIES: u64 = 3;
const INFINITY: u64 = u64::MAX;

pub fn get_poller_block_stream(
    client: Arc<RpcClientWithUri>,
    mut last_indexed_slot: u64,
    max_concurrent_block_fetches: usize,
) -> impl futures::Stream<Item = Vec<BlockInfo>> {
    stream! {
        let max_concurrent_block_fetches = 5;
        let mut block_fetching_futures = FuturesUnordered::new();
        let mut end_block_slot = fetch_current_slot_with_infinite_retry(&client.client).await;


        loop {
            let mut current_slot_to_fetch = match last_indexed_slot {
                0 => 0,
                last_indexed_slot => last_indexed_slot + 1
            };
            while current_slot_to_fetch > end_block_slot {
                end_block_slot = fetch_current_slot_with_infinite_retry(&client.client).await;
            }


            let mut in_process_slots = HashSet::new();
            while (block_fetching_futures.len() < max_concurrent_block_fetches) && current_slot_to_fetch <= end_block_slot  {
                let client = client.clone();
                block_fetching_futures.push(fetch_block(
                    client.uri.clone(),
                    current_slot_to_fetch,
                    RETRIES
                ));
                in_process_slots.insert(current_slot_to_fetch);
                current_slot_to_fetch += 1;
            }

            let mut block_cache: BTreeMap<u64, BlockInfo> = BTreeMap::new();
            while let Some(block) = block_fetching_futures.next().await {
                println!("In process slots: {:?}", in_process_slots);
                println!("Block cache: {:?}", block_cache.keys());
                let (block_result, slot) = block;
                println!("Block slot: {:?}", slot);
                println!("Last processed slot: {:?}", last_indexed_slot);
                let parent_slot = block_result.as_ref().map(|block| block.metadata.parent_slot);
                println!("Parent slot: {:?}", parent_slot);
                in_process_slots.remove(&slot);
                if let Ok(block) = block_result {
                    for next_slot in (block.metadata.slot + 1)..(block.metadata.slot + max_concurrent_block_fetches as u64) {
                        if in_process_slots.len() >= max_concurrent_block_fetches {
                            break;
                        }
                        if !in_process_slots.contains(&next_slot) && !block_cache.contains_key(&next_slot) && next_slot > last_indexed_slot {
                            block_fetching_futures.push(fetch_block(
                                client.uri.clone(),
                                next_slot,
                                RETRIES
                                ));
                                in_process_slots.insert(next_slot);
                        }
                    }
                    if block.metadata.slot == 0 || block.metadata.parent_slot == last_indexed_slot {
                        last_indexed_slot = block.metadata.slot;
                        yield vec![block];

                        loop {
                            let min_slot = match block_cache.keys().min() {
                                Some(&slot) => slot,
                                None => break,
                            }.clone();
                            let block: &BlockInfo = block_cache.get(&min_slot).unwrap();
                            if block.metadata.parent_slot == last_indexed_slot {
                                last_indexed_slot = block.metadata.slot;
                                yield vec![block.clone()];
                            } else {
                                if min_slot < last_indexed_slot {
                                    panic!("Block is not indexed: {}", min_slot);
                                }
                            }
                            block_cache.remove(&min_slot);
                        }
                    }
                    else {
                        if !in_process_slots.contains(&block.metadata.parent_slot) && !block_cache.contains_key(&block.metadata.parent_slot) && block.metadata.parent_slot > last_indexed_slot {
                            block_fetching_futures.push(fetch_block(
                                client.uri.clone(),
                                block.metadata.parent_slot,
                                INFINITY
                            ));
                            in_process_slots.insert(block.metadata.parent_slot);
                        }
                        block_cache.insert(block.metadata.slot, block.clone());

                    }
                }
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

pub async fn fetch_block(
    rpc_uri: String,
    slot: u64,
    retries: u64,
) -> (Result<BlockInfo, client_error::ClientError>, u64) {
    let mut attempt_counter = 0;
    loop {
        let timeout_sec = if attempt_counter == 0 { 3 } else { 20 };
        let client = RpcClient::new_with_timeout_and_commitment(
            rpc_uri.clone(),
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
                return (Ok(parse_ui_confirmed_blocked(block, slot).unwrap()), slot);
            }
            Err(e) => {
                if let solana_client::client_error::ClientErrorKind::RpcError(
                    RpcError::RpcResponseError { code, .. },
                ) = e.kind
                {
                    if SKIPPED_BLOCK_ERRORS.contains(&code) {
                        if retries == INFINITY {
                            log::error!(
                                "Got skipped block error for block that is supposed to exist: {}",
                                slot
                            );
                        } else {
                            log::info!("Skipped block: {}", slot);
                        }
                        return (Err(e), slot);
                    }
                }
                log::warn!("Failed to fetch block: {}. {}", slot, e.to_string());
                attempt_counter += 1;
                if attempt_counter >= retries {
                    return (Err(e), slot);
                }
            }
        }
    }
}
