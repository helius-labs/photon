use light_compressed_account::event::event_from_light_transaction;
use log::info;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use crate::common::typedefs::hash::Hash;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::{BatchPublicTransactionEvent, CompressedAccount, CompressedAccountData, MerkleTreeSequenceNumber, OutputCompressedAccountWithPackedContext, PublicTransactionEvent};
use crate::ingester::parser::parse_public_transaction_event;
use crate::ingester::parser::state_update::{AccountContext, StateUpdate};

pub fn parse_public_transaction_event_v2(instructions: &[Vec<u8>], accounts: Vec<Vec<Pubkey>>) -> Option<BatchPublicTransactionEvent> {
    let event = event_from_light_transaction(instructions, accounts);
    info!("Event from light transaction: {:?}", event);

    let event = event.unwrap_or_default();

    match event {
        Some(public_transaction_event) => {
            let event = PublicTransactionEvent {
                input_compressed_account_hashes: public_transaction_event.event.input_compressed_account_hashes,
                output_compressed_account_hashes: public_transaction_event.event.output_compressed_account_hashes,
                output_compressed_accounts: public_transaction_event.event.output_compressed_accounts.iter().map(|x| OutputCompressedAccountWithPackedContext {
                    compressed_account: CompressedAccount {
                        owner: x.compressed_account.owner,
                        lamports: x.compressed_account.lamports,
                        address: x.compressed_account.address,
                        data: x.compressed_account.data.as_ref().map(|d| CompressedAccountData {
                            discriminator: d.discriminator,
                            data: d.data.clone(),
                            data_hash: d.data_hash,
                        }),
                    },
                    merkle_tree_index: x.merkle_tree_index,
                }).collect(),
                output_leaf_indices: public_transaction_event.event.output_leaf_indices,
                sequence_numbers: public_transaction_event.event.sequence_numbers.iter().map(|x| MerkleTreeSequenceNumber {
                    pubkey: x.pubkey,
                    seq: x.seq,
                }).collect(),
                relay_fee: public_transaction_event.event.relay_fee,
                is_compress: public_transaction_event.event.is_compress,
                compression_lamports: public_transaction_event.event.compress_or_decompress_lamports,
                pubkey_array: public_transaction_event.event.pubkey_array,
                message: public_transaction_event.event.message,
            };
            let batch_public_transaction_event = BatchPublicTransactionEvent {
                event,
                new_addresses: public_transaction_event.new_addresses,
                input_sequence_numbers: public_transaction_event.input_sequence_numbers.iter().map(|x| MerkleTreeSequenceNumber {
                    pubkey: x.pubkey,
                    seq: x.seq,
                }).collect(),
                address_sequence_numbers: public_transaction_event.address_sequence_numbers.iter().map(|x| MerkleTreeSequenceNumber {
                    pubkey: x.pubkey,
                    seq: x.seq,
                }).collect(),
                nullifier_queue_indices: public_transaction_event.nullifier_queue_indices,
                tx_hash: public_transaction_event.tx_hash,
                nullifiers: public_transaction_event.nullifiers,
            };
            Some(batch_public_transaction_event)
        }
        None => {
            None
        }
    }
}

pub fn parse_batch_public_transaction_event(
    tx: Signature,
    slot: u64,
    transaction_event: BatchPublicTransactionEvent,
) -> Result<StateUpdate, IngesterError> {
    let mut state_update = parse_public_transaction_event(tx, slot, transaction_event.event)?;
    state_update.in_seq_numbers = transaction_event.input_sequence_numbers;

    let input_context = state_update
        .in_accounts
        .iter()
        .zip(transaction_event.nullifiers.iter())
        .zip(transaction_event.nullifier_queue_indices.iter())
        .map(|((account, nullifier), nullifier_queue_index)| AccountContext {
            account: account.clone(),
            tx_hash: Hash::new(&transaction_event.tx_hash).unwrap(),
            nullifier: Hash::new(nullifier).unwrap(),
            nullifier_queue_index: *nullifier_queue_index,
        })
        .collect::<Vec<_>>();
    println!("input context: {:?}", input_context);
    state_update.input_context.extend(input_context);
    Ok(state_update)
}