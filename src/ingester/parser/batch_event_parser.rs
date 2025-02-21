use crate::common::typedefs::hash::Hash;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::{
    BatchPublicTransactionEvent, CompressedAccount, CompressedAccountData,
    MerkleTreeSequenceNumber, OutputCompressedAccountWithPackedContext, PublicTransactionEvent,
};
use crate::ingester::parser::legacy::parse_legacy_merkle_tree_events;
use crate::ingester::parser::state_update::{AccountContext, StateUpdate};
use crate::ingester::parser::tx_event_parser::parse_public_transaction_event;
use crate::ingester::parser::{ACCOUNT_COMPRESSION_PROGRAM_ID, NOOP_PROGRAM_ID};
use crate::ingester::typedefs::block_info::{Instruction, TransactionInfo};
use borsh::BorshDeserialize;
use light_batched_merkle_tree::event::{
    BatchAppendEvent, BATCH_ADDRESS_APPEND_EVENT_DISCRIMINATOR, BATCH_APPEND_EVENT_DISCRIMINATOR,
    BATCH_NULLIFY_EVENT_DISCRIMINATOR,
};
use light_compressed_account::event::event_from_light_transaction;
use log::info;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::Signature;

pub fn parse_batch_merkle_tree_event(
    instruction: &Instruction,
    next_instruction: &Instruction,
    tx: &TransactionInfo,
) -> Result<Option<StateUpdate>, IngesterError> {
    if ACCOUNT_COMPRESSION_PROGRAM_ID == instruction.program_id
        && next_instruction.program_id == NOOP_PROGRAM_ID
        && tx.error.is_none()
    {
        info!("Parsing tx with signature: {}", tx.signature);

        // Try to parse as batch append/nullify event first
        if let Ok(batch_event) =
            BatchAppendEvent::deserialize(&mut next_instruction.data.as_slice())
        {
            let mut state_update = StateUpdate::new();

            match batch_event.discriminator {
                BATCH_APPEND_EVENT_DISCRIMINATOR => {
                    state_update.batch_append.push(batch_event);
                }
                BATCH_NULLIFY_EVENT_DISCRIMINATOR => {
                    state_update.batch_nullify.push(batch_event);
                }
                BATCH_ADDRESS_APPEND_EVENT_DISCRIMINATOR => {
                    // TODO: implement address append
                }
                _ => unimplemented!(),
            }

            return Ok(Some(state_update));
        }

        // If not batch event, try legacy events
        parse_legacy_merkle_tree_events(tx.signature, next_instruction).map(Some)
    } else {
        Ok(None)
    }
}

pub fn parse_public_transaction_event_v2(
    instructions: &[Vec<u8>],
    accounts: Vec<Vec<Pubkey>>,
) -> Option<BatchPublicTransactionEvent> {
    let event = event_from_light_transaction(instructions, accounts).ok()?;
    match event {
        Some(public_transaction_event) => {
            let event = PublicTransactionEvent {
                input_compressed_account_hashes: public_transaction_event
                    .event
                    .input_compressed_account_hashes,
                output_compressed_account_hashes: public_transaction_event
                    .event
                    .output_compressed_account_hashes,
                output_compressed_accounts: public_transaction_event
                    .event
                    .output_compressed_accounts
                    .iter()
                    .map(|x| OutputCompressedAccountWithPackedContext {
                        compressed_account: CompressedAccount {
                            owner: x.compressed_account.owner,
                            lamports: x.compressed_account.lamports,
                            address: x.compressed_account.address,
                            data: x.compressed_account.data.as_ref().map(|d| {
                                CompressedAccountData {
                                    discriminator: d.discriminator,
                                    data: d.data.clone(),
                                    data_hash: d.data_hash,
                                }
                            }),
                        },
                        merkle_tree_index: x.merkle_tree_index,
                    })
                    .collect(),
                output_leaf_indices: public_transaction_event.event.output_leaf_indices,
                sequence_numbers: public_transaction_event
                    .event
                    .sequence_numbers
                    .iter()
                    .map(|x| MerkleTreeSequenceNumber {
                        pubkey: x.pubkey,
                        seq: x.seq,
                    })
                    .collect(),
                relay_fee: public_transaction_event.event.relay_fee,
                is_compress: public_transaction_event.event.is_compress,
                compression_lamports: public_transaction_event
                    .event
                    .compress_or_decompress_lamports,
                pubkey_array: public_transaction_event.event.pubkey_array,
                message: public_transaction_event.event.message,
            };
            let batch_public_transaction_event = BatchPublicTransactionEvent {
                event,
                new_addresses: public_transaction_event.new_addresses,
                input_sequence_numbers: public_transaction_event
                    .input_sequence_numbers
                    .iter()
                    .map(|x| MerkleTreeSequenceNumber {
                        pubkey: x.pubkey,
                        seq: x.seq,
                    })
                    .collect(),
                address_sequence_numbers: public_transaction_event
                    .address_sequence_numbers
                    .iter()
                    .map(|x| MerkleTreeSequenceNumber {
                        pubkey: x.pubkey,
                        seq: x.seq,
                    })
                    .collect(),
                nullifier_queue_indices: public_transaction_event.nullifier_queue_indices,
                tx_hash: public_transaction_event.tx_hash,
                nullifiers: public_transaction_event.nullifiers,
            };
            Some(batch_public_transaction_event)
        }
        None => None,
    }
}

pub fn parse_batch_public_transaction_event(
    tx: Signature,
    slot: u64,
    transaction_event: BatchPublicTransactionEvent,
) -> Result<StateUpdate, IngesterError> {
    let mut state_update = parse_public_transaction_event(tx, slot, transaction_event.event)?;
    state_update.in_seq_numbers = transaction_event.input_sequence_numbers;

    // Context required for nullifier queue insertions of batched trees.
    let input_context = state_update
        .in_accounts
        .iter()
        .zip(transaction_event.nullifiers.iter())
        .zip(transaction_event.nullifier_queue_indices.iter())
        .map(
            |((account, nullifier), nullifier_queue_index)| AccountContext {
                account: account.clone(),
                tx_hash: Hash::new(&transaction_event.tx_hash).unwrap(),
                nullifier: Hash::new(nullifier).unwrap(),
                nullifier_queue_index: *nullifier_queue_index,
            },
        )
        .collect::<Vec<_>>();
    state_update.input_context.extend(input_context);
    Ok(state_update)
}
