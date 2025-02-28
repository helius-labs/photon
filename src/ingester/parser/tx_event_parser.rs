use crate::common::typedefs::account::AccountWithContext;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::PublicTransactionEvent;
use crate::ingester::parser::state_update::{AccountTransaction, StateUpdate};
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::parser::{ACCOUNT_COMPRESSION_PROGRAM_ID, NOOP_PROGRAM_ID, SYSTEM_PROGRAM};
use crate::ingester::typedefs::block_info::{Instruction, TransactionInfo};
use anchor_lang::AnchorDeserialize;
use light_merkle_tree_metadata::merkle_tree::TreeType;
use log::info;
use solana_sdk::signature::Signature;
use std::collections::HashMap;

pub fn parse_legacy_public_transaction_event(
    tx: &TransactionInfo,
    slot: u64,
    instruction: &Instruction,
    next_instruction: &Instruction,
    next_next_instruction: &Instruction,
) -> Result<Option<StateUpdate>, IngesterError> {
    if ACCOUNT_COMPRESSION_PROGRAM_ID == instruction.program_id
        && next_instruction.program_id == SYSTEM_PROGRAM
        && next_next_instruction.program_id == NOOP_PROGRAM_ID
        && tx.error.is_none()
    {
        info!(
            "Indexing transaction with slot {} and id {}",
            slot, tx.signature
        );

        let public_transaction_event =
            PublicTransactionEvent::deserialize(&mut next_next_instruction.data.as_slice())
                .map_err(|e| {
                    IngesterError::ParserError(format!(
                        "Failed to deserialize PublicTransactionEvent: {}",
                        e
                    ))
                })?;

        parse_public_transaction_event(tx.signature, slot, public_transaction_event).map(Some)
    } else {
        Ok(None)
    }
}

pub fn parse_public_transaction_event(
    tx: Signature,
    slot: u64,
    transaction_event: PublicTransactionEvent,
) -> Result<StateUpdate, IngesterError> {
    let PublicTransactionEvent {
        input_compressed_account_hashes,
        output_compressed_account_hashes,
        output_compressed_accounts,
        pubkey_array,
        sequence_numbers,
        ..
    } = transaction_event;

    let mut state_update = StateUpdate::new();

    let mut has_batched_instructions = false;
    let mut tree_to_seq_number = HashMap::new();

    for seq in sequence_numbers.iter() {
        if let Some(tree_info) = TreeInfo::get(&seq.pubkey.to_string()) {
            if tree_info.tree_type == TreeType::BatchedState
                || tree_info.tree_type == TreeType::BatchedAddress
            {
                tree_to_seq_number.insert(tree_info.tree, seq.seq);
                has_batched_instructions = true;
            }
        }
    }

    if !has_batched_instructions {
        tree_to_seq_number = sequence_numbers
            .iter()
            .map(|seq| (seq.pubkey, seq.seq))
            .collect::<HashMap<_, _>>();
    }

    for hash in input_compressed_account_hashes {
        state_update.in_accounts.insert(hash.into());
    }

    for ((out_account, hash), leaf_index) in output_compressed_accounts
        .into_iter()
        .zip(output_compressed_account_hashes)
        .zip(transaction_event.output_leaf_indices.iter())
    {
        let tree = pubkey_array[out_account.merkle_tree_index as usize];
        let tree_and_queue = TreeInfo::get(&tree.to_string())
            .ok_or(IngesterError::ParserError("Missing queue".to_string()))?
            .clone();

        let mut seq = None;
        if tree_and_queue.tree_type == TreeType::State {
            seq = Some(*tree_to_seq_number.get(&tree).ok_or_else(|| {
                IngesterError::ParserError("Missing sequence number".to_string())
            })?);

            let seq = tree_to_seq_number
                .get_mut(&tree)
                .ok_or_else(|| IngesterError::ParserError("Missing sequence number".to_string()))?;
            *seq += 1;
        }

        let in_output_queue = tree_and_queue.tree_type == TreeType::BatchedState;
        let enriched_account = AccountWithContext::new(
            out_account.compressed_account,
            hash,
            tree_and_queue.tree,
            tree_and_queue.queue,
            *leaf_index,
            slot,
            seq,
            in_output_queue,
            false,
            None,
            None,
            tree_and_queue.tree_type,
        );

        state_update.out_accounts.push(enriched_account);
    }

    state_update
        .account_transactions
        .extend(
            state_update
                .in_accounts
                .iter()
                .map(|hash| AccountTransaction {
                    hash: hash.clone(),
                    signature: tx,
                }),
        );

    state_update
        .account_transactions
        .extend(
            state_update
                .out_accounts
                .iter()
                .map(|a| AccountTransaction {
                    hash: a.account.hash.clone(),
                    signature: tx,
                }),
        );

    Ok(state_update)
}
