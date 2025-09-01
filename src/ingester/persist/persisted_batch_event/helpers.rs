use std::collections::HashSet;

use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::MerkleTreeEvent;
use crate::ingester::persist::leaf_node::{persist_leaf_nodes, LeafNode, STATE_TREE_HEIGHT_V2};
use crate::ingester::persist::MAX_SQL_INSERTS;
use log::{debug, warn};
use sea_orm::{
    sea_query::SimpleExpr, ColumnTrait, DatabaseTransaction, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder,
};

pub const ZKP_BATCH_SIZE: usize = 500;

/// Validates that the old_next_index in a batch event matches the current state.
/// Returns Ok(true) if processing should continue, Ok(false) if already processed (re-indexing),
/// or Err if validation fails.
pub fn validate_batch_index(
    old_next_index: u64,
    current_index: u64,
    event_type: &str,
) -> Result<bool, IngesterError> {
    if old_next_index > current_index {
        return Err(IngesterError::ParserError(format!(
            "Batch {} old_next_index {} is greater than current index {}",
            event_type, old_next_index, current_index
        )));
    } else if old_next_index < current_index {
        // Re-indexing scenario - events already processed
        debug!(
            "Batch {} re-indexing detected: old_next_index {} < current_index {}",
            event_type, old_next_index, current_index
        );
        return Ok(false);
    }
    Ok(true)
}

/// Creates a leaf node from an account for batch operations
pub fn create_account_leaf_node(
    account: &accounts::Model,
    tree_pubkey: &[u8],
    sequence_number: u64,
    use_nullifier: bool,
) -> Result<LeafNode, IngesterError> {
    let hash = if use_nullifier {
        let nullifier = account
            .nullifier
            .as_ref()
            .ok_or_else(|| IngesterError::ParserError("Nullifier is missing".to_string()))?;
        Hash::new(nullifier.as_slice())
    } else {
        if account.hash.is_empty() {
            return Err(IngesterError::ParserError(
                "Account hash is missing".to_string(),
            ));
        }
        Hash::new(account.hash.as_slice())
    }
    .map_err(|_| {
        IngesterError::ParserError(format!(
            "Failed to convert {} to Hash",
            if use_nullifier {
                "nullifier"
            } else {
                "account hash"
            }
        ))
    })?;

    Ok(LeafNode {
        tree: SerializablePubkey::try_from(tree_pubkey.to_vec()).map_err(|_| {
            IngesterError::ParserError("Failed to convert tree to SerializablePubkey".to_string())
        })?,
        seq: Some(sequence_number as u32),
        leaf_index: account.leaf_index as u32,
        hash,
    })
}

/// Deduplicates events by sequence number, keeping first occurrence
pub fn deduplicate_events(events: &mut Vec<(u64, MerkleTreeEvent)>) {
    let mut seen = HashSet::new();
    let original_count = events.len();

    events.retain(|(seq, _)| {
        if !seen.insert(*seq) {
            warn!("Skipping duplicate event with sequence {}", seq);
            false
        } else {
            true
        }
    });

    if events.len() < original_count {
        debug!("Removed {} duplicate events", original_count - events.len());
    }
}

/// Persists leaf nodes, chunking if the batch is too large
pub async fn persist_leaf_nodes_chunked(
    txn: &DatabaseTransaction,
    leaf_nodes: Vec<LeafNode>,
) -> Result<(), IngesterError> {
    if leaf_nodes.is_empty() {
        return Ok(());
    }

    if leaf_nodes.len() <= MAX_SQL_INSERTS {
        persist_leaf_nodes(txn, leaf_nodes, STATE_TREE_HEIGHT_V2 + 1).await
    } else {
        for chunk in leaf_nodes.chunks(MAX_SQL_INSERTS) {
            persist_leaf_nodes(txn, chunk.to_vec(), STATE_TREE_HEIGHT_V2 + 1).await?;
        }
        Ok(())
    }
}

/// Checks if batch nullify accounts are already processed (re-indexing)
pub async fn check_nullify_already_processed(
    txn: &DatabaseTransaction,
    tree: &[u8],
    queue_start: i64,
    queue_end: i64,
) -> Result<bool, IngesterError> {
    let expected = queue_end.saturating_sub(queue_start) as u64;
    let nullified = accounts::Entity::find()
        .filter(
            accounts::Column::NullifierQueueIndex
                .gte(queue_start)
                .and(accounts::Column::NullifierQueueIndex.lt(queue_end))
                .and(accounts::Column::Tree.eq(tree.to_vec()))
                .and(accounts::Column::NullifiedInTree.eq(true)),
        )
        .count(txn)
        .await?;
    Ok(nullified == expected)
}

/// Counts how many accounts are still in the output queue (spent but not yet removed from queue)
pub async fn count_nullify_accounts_in_output_queue(
    txn: &DatabaseTransaction,
    tree: &[u8],
    queue_start: i64,
    queue_end: i64,
) -> Result<u64, IngesterError> {
    accounts::Entity::find()
        .filter(
            accounts::Column::NullifierQueueIndex
                .gte(queue_start)
                .and(accounts::Column::NullifierQueueIndex.lt(queue_end))
                .and(accounts::Column::Tree.eq(tree.to_vec()))
                .and(accounts::Column::Spent.eq(true))
                .and(accounts::Column::InOutputQueue.eq(true)),
        )
        .count(txn)
        .await
        .map_err(Into::into)
}

/// Helper to build the common filter for accounts ready to nullify
fn build_nullify_ready_filter(tree: &[u8], queue_start: i64, queue_end: i64) -> SimpleExpr {
    accounts::Column::NullifierQueueIndex
        .gte(queue_start)
        .and(accounts::Column::NullifierQueueIndex.lt(queue_end))
        .and(accounts::Column::Tree.eq(tree.to_vec()))
        .and(accounts::Column::Spent.eq(true))
        .and(accounts::Column::NullifiedInTree.eq(false))
}

/// Counts accounts that are spent but not yet nullified in tree
pub async fn count_accounts_ready_to_nullify(
    txn: &DatabaseTransaction,
    tree: &[u8],
    queue_start: i64,
    queue_end: i64,
) -> Result<u64, IngesterError> {
    accounts::Entity::find()
        .filter(build_nullify_ready_filter(tree, queue_start, queue_end))
        .count(txn)
        .await
        .map_err(Into::into)
}

/// Fetches accounts from the nullifier queue that need to be nullified
pub async fn fetch_accounts_to_nullify(
    txn: &DatabaseTransaction,
    tree: &[u8],
    queue_start: i64,
    queue_end: i64,
) -> Result<Vec<accounts::Model>, IngesterError> {
    accounts::Entity::find()
        .filter(build_nullify_ready_filter(tree, queue_start, queue_end))
        .order_by_asc(accounts::Column::NullifierQueueIndex)
        .all(txn)
        .await
        .map_err(Into::into)
}

/// Gets the current next index for a tree (highest leaf_index + 1)
pub async fn get_tree_next_index(
    txn: &DatabaseTransaction,
    tree: &[u8],
) -> Result<u64, IngesterError> {
    Ok(accounts::Entity::find()
        .filter(
            accounts::Column::Tree
                .eq(tree.to_vec())
                .and(accounts::Column::InOutputQueue.eq(false)),
        )
        .order_by_desc(accounts::Column::LeafIndex)
        .one(txn)
        .await?
        .map(|acc| (acc.leaf_index + 1) as u64)
        .unwrap_or(0))
}

/// Fetches accounts from the output queue for batch append
pub async fn fetch_accounts_to_append(
    txn: &DatabaseTransaction,
    tree: &[u8],
    start_index: i64,
    end_index: i64,
) -> Result<Vec<accounts::Model>, IngesterError> {
    accounts::Entity::find()
        .filter(
            accounts::Column::LeafIndex
                .gte(start_index)
                .and(accounts::Column::LeafIndex.lt(end_index))
                .and(accounts::Column::Tree.eq(tree.to_vec()))
                .and(accounts::Column::InOutputQueue.eq(true)),
        )
        .order_by_asc(accounts::Column::LeafIndex)
        .all(txn)
        .await
        .map_err(Into::into)
}
