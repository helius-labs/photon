use crate::ingester::error::IngesterError;
use crate::ingester::parser::batch_event_parser::parse_batch_merkle_tree_event;
use crate::ingester::parser::indexer_events::{
    IndexedMerkleTreeEvent, MerkleTreeEvent, NullifierEvent, PublicTransactionEvent,
};
use crate::ingester::parser::state_update::{
    IndexedTreeLeafUpdate, LeafNullification, StateUpdate,
};
use crate::ingester::parser::tx_event_parser::parse_public_transaction_event;
use crate::ingester::parser::{ACCOUNT_COMPRESSION_PROGRAM_ID, NOOP_PROGRAM_ID, SYSTEM_PROGRAM};
use crate::ingester::typedefs::block_info::{Instruction, TransactionInfo};
use borsh::BorshDeserialize;
use log::info;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::Signature;

pub fn parse_legacy_merkle_tree_events(
    signature: Signature,
    instruction: &Instruction,
) -> Result<StateUpdate, IngesterError> {
    let merkle_tree_event = MerkleTreeEvent::deserialize(&mut instruction.data.as_slice())
        .map_err(|e| {
            IngesterError::ParserError(format!("Failed to deserialize MerkleTreeEvent: {}", e))
        })?;

    match merkle_tree_event {
        MerkleTreeEvent::V2(nullifier_event) => parse_nullifier_event(signature, nullifier_event),
        MerkleTreeEvent::V3(indexed_merkle_tree_event) => {
            parse_indexed_merkle_tree_update(indexed_merkle_tree_event)
        }
        _ => Err(IngesterError::ParserError(
            "Expected nullifier event or merkle tree update".to_string(),
        )),
    }
}

fn parse_legacy_public_transaction_event(
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

pub fn parse_legacy_instructions(
    ordered_instructions: &[Instruction],
    tx: &TransactionInfo,
    slot: u64,
    state_updates: &mut Vec<StateUpdate>,
    is_compression_transaction: &mut bool,
) -> Result<(), IngesterError> {
    for (index, _) in ordered_instructions.iter().enumerate() {
        if ordered_instructions.len() - index > 3 {
            if let Some(state_update) = parse_legacy_public_transaction_event(
                tx,
                slot,
                &ordered_instructions[index],
                &ordered_instructions[index + 1],
                &ordered_instructions[index + 2],
            )? {
                *is_compression_transaction = true;
                state_updates.push(state_update);
            }
        }

        if ordered_instructions.len() - index > 1 {
            if let Some(state_update) = parse_batch_merkle_tree_event(
                &ordered_instructions[index],
                &ordered_instructions[index + 1],
                tx,
            )? {
                *is_compression_transaction = true;
                state_updates.push(state_update);
            }
        }
    }

    Ok(())
}

fn parse_nullifier_event(
    tx: Signature,
    nullifier_event: NullifierEvent,
) -> Result<StateUpdate, IngesterError> {
    let NullifierEvent {
        id,
        nullified_leaves_indices,
        seq,
    } = nullifier_event;

    let mut state_update = StateUpdate::new();

    for (i, leaf_index) in nullified_leaves_indices.iter().enumerate() {
        let leaf_nullification: LeafNullification = {
            LeafNullification {
                tree: Pubkey::from(id),
                leaf_index: *leaf_index,
                seq: seq + i as u64,
                signature: tx,
            }
        };
        state_update.leaf_nullifications.insert(leaf_nullification);
    }

    Ok(state_update)
}

fn parse_indexed_merkle_tree_update(
    indexed_merkle_tree_event: IndexedMerkleTreeEvent,
) -> Result<StateUpdate, IngesterError> {
    let IndexedMerkleTreeEvent {
        id,
        updates,
        mut seq,
    } = indexed_merkle_tree_event;
    let mut state_update = StateUpdate::new();

    for update in updates {
        for (leaf, hash) in [
            (update.new_low_element, update.new_low_element_hash),
            (update.new_high_element, update.new_high_element_hash),
        ]
        .iter()
        {
            let indexed_tree_leaf_update = IndexedTreeLeafUpdate {
                tree: Pubkey::from(id),
                hash: *hash,
                leaf: *leaf,
                seq,
            };
            seq += 1;
            state_update.indexed_merkle_tree_updates.insert(
                (indexed_tree_leaf_update.tree, leaf.index as u64),
                indexed_tree_leaf_update,
            );
        }
    }

    Ok(state_update)
}
