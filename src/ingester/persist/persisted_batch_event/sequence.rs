use log::debug;
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};

use crate::dao::generated::state_trees;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::BatchEvent;

/// Checks if an event should be processed based on its sequence number.
/// Returns Ok(true) if the event should be processed, Ok(false) if it should be skipped.
pub async fn should_process_event(
    txn: &DatabaseTransaction,
    event: &BatchEvent,
) -> Result<bool, IngesterError> {
    // Get the current sequence number for this tree
    let current_sequence = get_current_tree_sequence(txn, &event.merkle_tree_pubkey).await?;

    if event.sequence_number <= current_sequence {
        debug!(
            "Skipping event: tree {} already at sequence {} (event sequence: {})",
            bs58::encode(&event.merkle_tree_pubkey).into_string(),
            current_sequence,
            event.sequence_number
        );
        return Ok(false);
    }

    // Check for sequence gaps - we should process events in order
    if event.sequence_number > current_sequence + 1 {
        return Err(IngesterError::ParserError(format!(
            "Sequence gap detected: tree {} is at sequence {}, but received event with sequence {}",
            bs58::encode(&event.merkle_tree_pubkey).into_string(),
            current_sequence,
            event.sequence_number
        )));
    }

    Ok(true)
}

/// Gets the current sequence number for a tree from the root node
pub async fn get_current_tree_sequence(
    txn: &DatabaseTransaction,
    tree_pubkey: &[u8; 32],
) -> Result<u64, IngesterError> {
    let root = get_root_node(txn, tree_pubkey).await?;
    Ok(root.and_then(|r| r.seq).map(|seq| seq as u64).unwrap_or(0))
}

/// Updates only the sequence number of the root node without changing its hash.
/// This is used when a batch append processes only already-nullified accounts.
/// The on-chain program still increments the sequence in this case, so we must too.
pub async fn update_root_sequence(
    txn: &DatabaseTransaction,
    tree_pubkey: &[u8; 32],
    sequence_number: u64,
) -> Result<(), IngesterError> {
    let root_node = get_root_node(txn, tree_pubkey).await?;

    if let Some(root) = root_node {
        // Only update if the new sequence is greater than the current one
        // This handles re-indexing scenarios where the sequence was already updated
        let current_seq = root.seq.unwrap_or(0) as u64;
        if sequence_number > current_seq {
            // Update only the sequence number, keeping the same hash
            let mut active_model: state_trees::ActiveModel = root.into();
            active_model.seq = Set(Some(sequence_number as i64));
            active_model.update(txn).await?;
        } else {
            // Sequence already up to date or ahead - skip update
            debug!(
                "Skipping sequence update for tree {:?}: current seq {} >= new seq {}",
                bs58::encode(tree_pubkey).into_string(),
                current_seq,
                sequence_number
            );
        }
    } else {
        // If no root exists yet, this is an error - we should have a root by now
        return Err(IngesterError::ParserError(
            "No root node found when updating sequence for batch append with all nullified accounts".to_string()
        ));
    }

    Ok(())
}

/// Helper to get the root node for a tree
async fn get_root_node(
    txn: &DatabaseTransaction,
    tree_pubkey: &[u8; 32],
) -> Result<Option<state_trees::Model>, IngesterError> {
    state_trees::Entity::find()
        .filter(state_trees::Column::Tree.eq(tree_pubkey.to_vec()))
        .filter(state_trees::Column::NodeIdx.eq(1i64)) // Root node
        .one(txn)
        .await
        .map_err(Into::into)
}
