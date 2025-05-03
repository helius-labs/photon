use std::collections::HashMap;

use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::{
    IndexedMerkleTreeEvent, MerkleTreeEvent, NullifierEvent,
};
use crate::ingester::parser::state_update::{
    IndexedTreeLeafUpdate, LeafNullification, StateUpdate,
};
use crate::ingester::parser::{get_compression_program_id, NOOP_PROGRAM_ID};
use crate::ingester::typedefs::block_info::{Instruction, TransactionInfo};
use borsh::BorshDeserialize;
use solana_pubkey::Pubkey;
use solana_sdk::signature::Signature;

/// A map of merkle tree events and sequence numbers by merkle tree pubkey.
/// We keep sequence number to order the events.
pub type BatchMerkleTreeEvents = HashMap<[u8; 32], Vec<(u64, MerkleTreeEvent)>>;

pub fn parse_merkle_tree_event(
    instruction: &Instruction,
    next_instruction: &Instruction,
    tx: &TransactionInfo,
) -> Result<Option<StateUpdate>, IngesterError> {
    if get_compression_program_id() == instruction.program_id
        && next_instruction.program_id == NOOP_PROGRAM_ID
        && tx.error.is_none()
    {
        let merkle_tree_event = MerkleTreeEvent::deserialize(&mut next_instruction.data.as_slice());
        if let Ok(merkle_tree_event) = merkle_tree_event {
            let mut state_update = StateUpdate::new();
            let event = match merkle_tree_event {
                MerkleTreeEvent::V2(nullifier_event) => {
                    parse_nullifier_event_v1(tx.signature, nullifier_event)
                }
                MerkleTreeEvent::V3(indexed_merkle_tree_event) => {
                    parse_indexed_merkle_tree_update(indexed_merkle_tree_event)
                }
                MerkleTreeEvent::BatchAppend(batch_event) => {
                    state_update
                        .batch_merkle_tree_events
                        .entry(batch_event.merkle_tree_pubkey)
                        .or_default()
                        .push((
                            batch_event.sequence_number,
                            MerkleTreeEvent::BatchAppend(batch_event),
                        ));
                    state_update
                }
                MerkleTreeEvent::BatchNullify(batch_event) => {
                    state_update
                        .batch_merkle_tree_events
                        .entry(batch_event.merkle_tree_pubkey)
                        .or_default()
                        .push((
                            batch_event.sequence_number,
                            MerkleTreeEvent::BatchNullify(batch_event),
                        ));
                    state_update
                }
                MerkleTreeEvent::BatchAddressAppend(batch_event) => {
                    state_update
                        .batch_merkle_tree_events
                        .entry(batch_event.merkle_tree_pubkey)
                        .or_default()
                        .push((
                            batch_event.sequence_number,
                            MerkleTreeEvent::BatchAddressAppend(batch_event),
                        ));
                    state_update
                }
                _ => Err(IngesterError::ParserError(
                    "Expected nullifier event or merkle tree update".to_string(),
                ))?,
            };
            Ok(Some(event))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/// Parse a V1 state tree nullifier event.
fn parse_nullifier_event_v1(tx: Signature, nullifier_event: NullifierEvent) -> StateUpdate {
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

    state_update
}

fn parse_indexed_merkle_tree_update(
    indexed_merkle_tree_event: IndexedMerkleTreeEvent,
) -> StateUpdate {
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

    state_update
}
