use std::{pin::Pin, thread::sleep, time::Duration};

use async_stream::stream;
use futures::Stream;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::transaction_info::{parse_instruction_groups, TransactionInfo};

#[allow(dead_code)]
fn get_block_poller_fetcher(client: RpcClient) -> Pin<Box<dyn Stream<Item = TransactionInfo>>> {
    Box::pin(stream! {
        let mut slot = client.get_slot().await.unwrap();
        loop {
            let new_slot = client.get_slot().await.unwrap();
            for slot in slot..new_slot {
                let block = client.get_block(slot).await.unwrap();
                let transactions = block.transactions;
                for transaction in transactions {
                    let instruction_groups = parse_instruction_groups(transaction);
                    match instruction_groups {
                        Err(e) => {
                            log::error!("Failed to parse transaction: {}", e);
                        }
                        Ok(instruction_groups) => {
                            yield TransactionInfo {
                                slot: slot,
                                block_time: block.block_time,
                                instruction_groups: instruction_groups,
                            };
                        }
                    }
                }
            }
            if slot == new_slot {
                log::info!("No new blocks found, sleeping...");
                sleep(Duration::from_millis(50));
            }
            slot = new_slot;
        }
    })
}
