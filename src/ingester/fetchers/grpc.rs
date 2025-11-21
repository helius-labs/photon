use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use async_stream::stream;
use cadence_macros::statsd_count;
use futures::future::{select, Either};
use futures::sink::SinkExt;
use futures::{pin_mut, Stream, StreamExt};
use log::info;
use rand::distributions::Alphanumeric;
use rand::Rng;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_pubkey::Pubkey;
use solana_pubkey::Pubkey as SdkPubkey;
use solana_signature::Signature;
use tokio::time::sleep;
use tracing::error;
use yellowstone_grpc_client::{GeyserGrpcBuilderResult, GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::convert_from::create_tx_error;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestPing,
};
use yellowstone_grpc_proto::geyser::{
    SubscribeRequestFilterBlocks, SubscribeUpdateBlock, SubscribeUpdateTransactionInfo,
};
use yellowstone_grpc_proto::solana::storage::confirmed_block::InnerInstructions;

use crate::api::method::get_indexer_health::HEALTH_CHECK_SLOT_DISTANCE;
use crate::common::typedefs::hash::Hash;
use crate::ingester::fetchers::poller::get_block_poller_stream;
use crate::ingester::typedefs::block_info::{
    BlockInfo, BlockMetadata, Instruction, InstructionGroup, TransactionInfo,
};

use crate::metric;
use crate::monitor::{start_latest_slot_updater, LATEST_SLOT};

pub fn get_grpc_stream_with_rpc_fallback(
    endpoint: String,
    auth_header: String,
    rpc_client: Arc<RpcClient>,
    mut last_indexed_slot: u64,
    max_concurrent_block_fetches: usize,
) -> impl Stream<Item = Vec<BlockInfo>> {
    stream! {
        start_latest_slot_updater(rpc_client.clone()).await;
        let grpc_stream = get_grpc_block_stream(endpoint, auth_header);
        pin_mut!(grpc_stream);
        let mut rpc_poll_stream:  Option<Pin<Box<dyn Stream<Item = Vec<BlockInfo>> + Send>>> = Some(
            Box::pin(get_block_poller_stream(
                rpc_client.clone(),
                last_indexed_slot,
                max_concurrent_block_fetches,
            ))
        );

        // Await either the gRPC stream or the RPC block fetching
        loop {
            match rpc_poll_stream.as_mut() {
                Some(rpc_poll_stream_value) => {
                    match select(grpc_stream.next(), rpc_poll_stream_value.next()).await {
                        Either::Left((Some(grpc_block), _)) => {
                            let slot = grpc_block.metadata.slot;
                            if grpc_block.metadata.parent_slot == last_indexed_slot {
                                last_indexed_slot = grpc_block.metadata.slot;
                                yield vec![grpc_block];
                                metric! {
                                    statsd_count!("grpc_block_indexed", 1);
                                }
                                if is_healthy(slot) {
                                    info!("Switching to gRPC block fetching since Photon is up-to-date");
                                    rpc_poll_stream = None;
                                }
                            }
                        }
                        Either::Left((None, _)) => {
                            panic!("gRPC stream ended unexpectedly");
                        }
                        Either::Right((Some(rpc_blocks), _)) => {
                            let rpc_blocks: Vec<BlockInfo> = rpc_blocks
                                .into_iter()
                                .filter(|b| b.metadata.slot > last_indexed_slot)
                                .collect();
                            if rpc_blocks.is_empty() {
                                continue;
                            }
                            let blocks_len = rpc_blocks.len();
                            let parent_slot = rpc_blocks.first().unwrap().metadata.parent_slot;
                            let last_slot = rpc_blocks.last().unwrap().metadata.slot;
                            if parent_slot == last_indexed_slot {
                                last_indexed_slot = last_slot;
                                yield rpc_blocks;
                                metric! {
                                    statsd_count!("rpc_block_indexed", blocks_len as i64);
                                }
                            }
                        }
                        Either::Right((None, _)) => {
                            panic!("RPC stream ended unexpectedly");
                        }
                    }
                }
                None => {
                    let block = match tokio::time::timeout(Duration::from_secs(5), grpc_stream.next()).await {
                        Ok(Some(block)) => block,
                        Ok(None) => panic!("gRPC stream ended unexpectedly"),
                        Err(_) => {
                            metric! {
                                statsd_count!("grpc_timeout", 1);
                            }
                            info!("gRPC stream timed out, enabling RPC block fetching");
                            rpc_poll_stream = Some(Box::pin(get_block_poller_stream(
                                rpc_client.clone(),
                                last_indexed_slot,
                                max_concurrent_block_fetches,
                            )));
                            continue;
                        }
                    };
                    let slot = block.metadata.slot;
                    if block.metadata.parent_slot == last_indexed_slot {
                        last_indexed_slot = block.metadata.slot;
                        yield vec![block];
                    } else {
                        metric! {
                            statsd_count!("grpc_out_of_order", 1);
                        }
                        info!("Switching to RPC block fetching");
                        rpc_poll_stream = Some(Box::pin(get_block_poller_stream(
                            rpc_client.clone(),
                            last_indexed_slot,
                            max_concurrent_block_fetches,
                        )));
                        continue;
                    }
                    if !is_healthy(slot) && rpc_poll_stream.is_none() {
                        info!("gRPC is unhealthy. Enabling RPC block fetching");
                        metric! {
                            statsd_count!("grpc_stale", 1);
                        }
                        rpc_poll_stream = Some(Box::pin(get_block_poller_stream(
                            rpc_client.clone(),
                            last_indexed_slot,
                            max_concurrent_block_fetches,
                        )));
                    }
                }
            }


        }
    }
}

fn is_healthy(slot: u64) -> bool {
    (LATEST_SLOT.load(Ordering::SeqCst) as i64 - slot as i64) <= HEALTH_CHECK_SLOT_DISTANCE
}

fn get_grpc_block_stream(endpoint: String, auth_header: String) -> impl Stream<Item = BlockInfo> {
    stream! {
        loop {
            let mut grpc_tx;
            let mut grpc_rx;
            {
                let grpc_client =
                    build_geyser_client(endpoint.clone(), auth_header.clone()).await;
                if let Err(e) = grpc_client {
                    error!("Error connecting to gRPC, waiting one second then retrying connect: {}", e);
                    metric! {
                        statsd_count!("grpc_connect_error", 1);
                    }
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                let subscription = grpc_client
                    .unwrap()
                    .subscribe_with_request(Some(get_block_subscribe_request()))
                    .await;
                if let Err(e) = subscription {
                    error!("Error subscribing to gRPC stream, waiting one second then retrying connect: {}", e);
                    metric! {
                        statsd_count!("grpc_subscribe_error", 1);
                    }
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                (grpc_tx, grpc_rx) = subscription.unwrap();
            }
            while let Some(message) = grpc_rx.next().await {
                match message {
                    Ok(message) => match message.update_oneof {
                        Some(UpdateOneof::Block(block)) => {
                            let block = parse_block(block);
                            metric! {
                                statsd_count!("grpc_block_emitted", 1);
                            }
                            yield block;
                        }
                        Some(UpdateOneof::Ping(_)) => {
                            // This is necessary to keep load balancers that expect client pings alive. If your load balancer doesn't
                            // require periodic client pings then this is unnecessary
                            let ping = grpc_tx.send(ping()).await;
                            if let Err(e) = ping {
                                error!("Error sending ping: {}", e);
                                metric! {
                                    statsd_count!("grpc_ping_error", 1);
                                }
                                break;
                            }
                        }
                        Some(UpdateOneof::Pong(_)) => {}
                        _ => {
                            error!("Unknown message: {:?}", message);
                        }
                    },
                    Err(error) => {
                        error!(
                            "error in block subscribe, resubscribing in 1 second: {error:?}"
                        );
                        metric! {
                            statsd_count!("grpc_resubscribe", 1);
                        }
                        break;
                    }
                }
            }
        sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn build_geyser_client(
    endpoint: String,
    auth_header: String,
) -> GeyserGrpcBuilderResult<GeyserGrpcClient<impl Interceptor>> {
    GeyserGrpcClient::build_from_shared(endpoint)?
        .x_token(Some(auth_header))?
        .connect_timeout(Duration::from_secs(10))
        .max_decoding_message_size(8388608)
        .timeout(Duration::from_secs(10))
        .connect()
        .await
}

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn get_block_subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        blocks: HashMap::from_iter(vec![(
            generate_random_string(20),
            SubscribeRequestFilterBlocks {
                account_include: vec![],
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(false),
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed.into()),
        ..Default::default()
    }
}

fn ping() -> SubscribeRequest {
    SubscribeRequest {
        ping: Some(SubscribeRequestPing { id: 1 }),
        ..Default::default()
    }
}

fn parse_block(block: SubscribeUpdateBlock) -> BlockInfo {
    let metadata = BlockMetadata {
        slot: block.slot,
        parent_slot: block.parent_slot,
        block_time: block.block_time.unwrap().timestamp,
        blockhash: Hash::try_from(block.blockhash.as_str()).unwrap(),
        parent_blockhash: Hash::try_from(block.parent_blockhash.as_str()).unwrap(),
        block_height: block.block_height.unwrap().block_height,
    };
    let transactions = block
        .transactions
        .into_iter()
        .map(parse_transaction)
        .collect();

    BlockInfo {
        metadata,
        transactions,
    }
}

fn parse_transaction(transaction: SubscribeUpdateTransactionInfo) -> TransactionInfo {
    let meta = transaction.meta.unwrap();
    let error = create_tx_error(meta.err.as_ref());
    if let Err(e) = &error {
        error!(
            "Error parsing transaction error: {}. Error bytes: {:?}",
            e, meta.err
        );
    }
    let error = error.unwrap();

    let error = error.map(|e| e.to_string());

    let signature = Signature::try_from(transaction.signature).unwrap();
    let message = transaction.transaction.unwrap().message.unwrap();
    let outer_intructions = message.instructions;
    let mut accounts = message.account_keys;
    for account in meta.loaded_writable_addresses {
        accounts.push(account);
    }
    for account in meta.loaded_readonly_addresses {
        accounts.push(account);
    }

    let mut instruction_groups: Vec<InstructionGroup> = outer_intructions
        .iter()
        .map(|ix| {
            let sdk_program_id =
                SdkPubkey::try_from(accounts[ix.program_id_index as usize].clone()).unwrap();
            let program_id = Pubkey::new_from_array(sdk_program_id.to_bytes());
            let data = ix.data.clone();
            let accounts: Vec<Pubkey> = ix
                .accounts
                .iter()
                .map(|account_index| {
                    let sdk_pubkey =
                        SdkPubkey::try_from(accounts[*account_index as usize].clone()).unwrap();
                    Pubkey::new_from_array(sdk_pubkey.to_bytes())
                })
                .collect();

            InstructionGroup {
                outer_instruction: Instruction {
                    program_id,
                    data,
                    accounts,
                },
                inner_instructions: Vec::new(),
            }
        })
        .collect();

    for inner_instruction_group in meta.inner_instructions {
        let InnerInstructions {
            index,
            instructions,
        } = inner_instruction_group;
        for instruction in instructions {
            let instruction_group = &mut instruction_groups[index as usize];
            let sdk_program_id =
                SdkPubkey::try_from(accounts[instruction.program_id_index as usize].clone())
                    .unwrap();
            let program_id = Pubkey::new_from_array(sdk_program_id.to_bytes());
            let data = instruction.data.clone();
            let accounts: Vec<Pubkey> = instruction
                .accounts
                .iter()
                .map(|account_index| {
                    let sdk_pubkey =
                        SdkPubkey::try_from(accounts[*account_index as usize].clone()).unwrap();
                    Pubkey::new_from_array(sdk_pubkey.to_bytes())
                })
                .collect();
            instruction_group.inner_instructions.push(Instruction {
                program_id,
                data,
                accounts,
            });
        }
    }

    TransactionInfo {
        instruction_groups,
        signature,
        error,
    }
}
