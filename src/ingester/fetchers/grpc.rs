use std::{collections::HashMap, time::Duration};

use async_stream::stream;
use cadence_macros::statsd_count;
use futures::executor::block_on;
use futures::sink::SinkExt;
use futures::{Stream, StreamExt};
use rand::distributions::Alphanumeric;
use rand::Rng;
use solana_sdk::address_lookup_table::instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use tokio::time::sleep;
use tracing::error;
use yellowstone_grpc_client::{GeyserGrpcBuilderResult, GeyserGrpcClient, Interceptor};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestPing,
};
use yellowstone_grpc_proto::geyser::{
    SubscribeRequestFilterBlocks, SubscribeUpdateBlock, SubscribeUpdateTransactionInfo,
};
use yellowstone_grpc_proto::solana::storage::confirmed_block::InnerInstructions;

use crate::common::typedefs::hash::Hash;
use crate::ingester::typedefs::block_info::{
    parse_instruction_groups, BlockInfo, BlockMetadata, Instruction, InstructionGroup,
    TransactionInfo,
};

const BLOCKHASH_VALID_SLOTS: u64 = 150;
const BLOCKHASH_MAX_TTL_SECONDS: u64 = 300;

fn get_grpc_block_stream(
    endpoint: String,
    auth_header: Option<String>,
    // Stream of BlockInfo
) -> impl Stream<Item = BlockInfo> {
    stream! {
        loop {
            let mut grpc_tx;
            let mut grpc_rx;
            {
                yield BlockInfo::default();
                let grpc_client =
                    build_geyser_client(endpoint.clone(), auth_header.clone()).await;
                if let Err(e) = grpc_client {
                    error!("Error connecting to gRPC, waiting one second then retrying connect: {}", e);
                    statsd_count!("grpc_connect_error", 1);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                let subscription = grpc_client
                    .unwrap()
                    .subscribe_with_request(Some(get_block_subscribe_request()))
                    .await;
                if let Err(e) = subscription {
                    error!("Error subscribing to gRPC stream, waiting one second then retrying connect: {}", e);
                    statsd_count!("grpc_subscribe_error", 1);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                (grpc_tx, grpc_rx) = subscription.unwrap();
            }
            while let Some(message) = grpc_rx.next().await {
                match message {
                    Ok(message) => match message.update_oneof {
                        Some(UpdateOneof::Block(block)) => {
                            yield parse_block(block);
                        }
                        Some(UpdateOneof::Ping(_)) => {
                            // This is necessary to keep load balancers that expect client pings alive. If your load balancer doesn't
                            // require periodic client pings then this is unnecessary
                            let ping = grpc_tx.send(ping()).await;
                            if let Err(e) = ping {
                                error!("Error sending ping: {}", e);
                                statsd_count!("grpc_ping_error", 1);
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
                        statsd_count!("grpc_resubscribe", 1);
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
    auth_header: Option<String>,
) -> GeyserGrpcBuilderResult<GeyserGrpcClient<impl Interceptor>> {
    GeyserGrpcClient::build_from_shared(endpoint)?
        .x_token(auth_header)?
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
    let signature = Signature::try_from(transaction.signature).unwrap();
    let success = meta.err.is_none();
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
            let program_id =
                Pubkey::try_from(accounts[ix.program_id_index as usize].clone()).unwrap();
            let data = ix.data.clone();
            let accounts: Vec<Pubkey> = ix
                .accounts
                .iter()
                .map(|account_index| {
                    Pubkey::try_from(accounts[*account_index as usize].clone()).unwrap()
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
            let program_id =
                Pubkey::try_from(accounts[instruction.program_id_index as usize].clone()).unwrap();
            let data = instruction.data.clone();
            let accounts: Vec<Pubkey> = instruction
                .accounts
                .iter()
                .map(|account_index| {
                    Pubkey::try_from(accounts[*account_index as usize].clone()).unwrap()
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
        success,
    }
}
