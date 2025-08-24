use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::{accounts, address_queues, indexed_trees};
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::BatchEvent;
use crate::ingester::parser::{
    indexer_events::MerkleTreeEvent, merkle_tree_events_parser::BatchMerkleTreeEvents,
};
use crate::ingester::persist::leaf_node::{persist_leaf_nodes, LeafNode, STATE_TREE_HEIGHT_V2};
use crate::ingester::persist::persisted_indexed_merkle_tree::multi_append;
use crate::ingester::persist::MAX_SQL_INSERTS;
use crate::migration::Expr;
use light_batched_merkle_tree::constants::DEFAULT_BATCH_ADDRESS_TREE_HEIGHT;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QueryTrait,
};

const ZKP_BATCH_SIZE: usize = 500;

/// Validates that the old_next_index in a batch event matches the current state.
/// Returns Ok(true) if processing should continue, Ok(false) if already processed (re-indexing),
/// or Err if validation fails.
fn validate_batch_index(
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
        tracing::debug!(
            "Batch {} re-indexing detected: old_next_index {} < current_index {}",
            event_type,
            old_next_index,
            current_index
        );
        return Ok(false);
    }
    Ok(true)
}

/// We need to find the events of the same tree:
/// - order them by sequence number and execute them in order
///     HashMap<pubkey, Vec<Event(BatchAppendEvent, seq)>>
/// - execute a single function call to persist all changed nodes
pub async fn persist_batch_events(
    txn: &DatabaseTransaction,
    mut events: BatchMerkleTreeEvents,
) -> Result<(), IngesterError> {
    for (_, events) in events.iter_mut() {
        events.sort_by(|a, b| a.0.cmp(&b.0));

        // Process each event in sequence
        for (_, event) in events.iter() {
            // Batch size is 500 for batched State Merkle trees.
            let mut leaf_nodes = Vec::with_capacity(ZKP_BATCH_SIZE);
            match event {
                MerkleTreeEvent::BatchNullify(batch_nullify_event) => {
                    persist_batch_nullify_event(txn, batch_nullify_event, &mut leaf_nodes).await
                }
                MerkleTreeEvent::BatchAppend(batch_append_event) => {
                    persist_batch_append_event(txn, batch_append_event, &mut leaf_nodes).await
                }
                MerkleTreeEvent::BatchAddressAppend(batch_address_append_event) => {
                    persist_batch_address_append_event(txn, batch_address_append_event).await
                }
                _ => Err(IngesterError::InvalidEvent),
            }?;

            if leaf_nodes.len() <= MAX_SQL_INSERTS {
                persist_leaf_nodes(txn, leaf_nodes, STATE_TREE_HEIGHT_V2 + 1).await?;
            } else {
                // Currently not used but a safeguard in case the batch size changes.
                for leaf_nodes_chunk in leaf_nodes.chunks(MAX_SQL_INSERTS) {
                    persist_leaf_nodes(txn, leaf_nodes_chunk.to_vec(), STATE_TREE_HEIGHT_V2 + 1)
                        .await?;
                }
            }
        }
    }
    Ok(())
}

/// Persists a batch append event.
/// 1. Create leaf nodes with the account hash as leaf.
/// 2. Remove inserted elements from the database output queue.
async fn persist_batch_append_event(
    txn: &DatabaseTransaction,
    batch_append_event: &BatchEvent,
    leaf_nodes: &mut Vec<LeafNode>,
) -> Result<(), IngesterError> {
    let expected_count =
        (batch_append_event.new_next_index - batch_append_event.old_next_index) as usize;

    // Validate old_next_index matches the current state of the tree
    let current_next_index = accounts::Entity::find()
        .filter(
            accounts::Column::Tree
                .eq(batch_append_event.merkle_tree_pubkey.to_vec())
                .and(accounts::Column::InOutputQueue.eq(false)),
        )
        .order_by_desc(accounts::Column::LeafIndex)
        .one(txn)
        .await?
        .map(|acc| (acc.leaf_index + 1) as u64)
        .unwrap_or(0);

    if !validate_batch_index(
        batch_append_event.old_next_index,
        current_next_index,
        "append",
    )? {
        return Ok(());
    }

    let accounts = accounts::Entity::find()
        .filter(
            accounts::Column::LeafIndex
                .gte(batch_append_event.old_next_index as i64)
                .and(accounts::Column::LeafIndex.lt(batch_append_event.new_next_index as i64))
                .and(accounts::Column::Tree.eq(batch_append_event.merkle_tree_pubkey.to_vec()))
                .and(accounts::Column::InOutputQueue.eq(true)),
        )
        .order_by_asc(accounts::Column::LeafIndex)
        .all(txn)
        .await?;

    // If we got the expected count, proceed
    if accounts.len() == expected_count {
        // Validate sequential indices and process accounts
        let mut expected_leaf_index = batch_append_event.old_next_index;

        for account in &accounts {
            if account.leaf_index != expected_leaf_index as i64 {
                return Err(IngesterError::ParserError(format!(
                    "Gap in leaf indices: expected {}, got {}",
                    expected_leaf_index, account.leaf_index
                )));
            }
            expected_leaf_index += 1;

            if account.hash.is_empty() {
                return Err(IngesterError::ParserError(
                    "Account hash is missing".to_string(),
                ));
            }

            leaf_nodes.push(LeafNode {
                tree: SerializablePubkey::try_from(account.tree.clone()).map_err(|_| {
                    IngesterError::ParserError(
                        "Failed to convert tree to SerializablePubkey".to_string(),
                    )
                })?,
                seq: Some(batch_append_event.sequence_number as u32),
                leaf_index: account.leaf_index as u32,
                hash: Hash::new(account.hash.as_slice()).map_err(|_| {
                    IngesterError::ParserError("Failed to convert account hash to Hash".to_string())
                })?,
            });
        }
    } else if accounts.is_empty() {
        // Check if already processed (re-indexing scenario)
        let already_processed = accounts::Entity::find()
            .filter(
                accounts::Column::LeafIndex
                    .gte(batch_append_event.old_next_index as i64)
                    .and(accounts::Column::LeafIndex.lt(batch_append_event.new_next_index as i64))
                    .and(accounts::Column::Tree.eq(batch_append_event.merkle_tree_pubkey.to_vec()))
                    .and(accounts::Column::InOutputQueue.eq(false)),
            )
            .count(txn)
            .await?;

        if already_processed == expected_count as u64 {
            tracing::debug!(
                "Batch append already processed: {} accounts already in tree for range [{}, {})",
                already_processed,
                batch_append_event.old_next_index,
                batch_append_event.new_next_index
            );
            return Ok(());
        }

        return Err(IngesterError::ParserError(format!(
            "Expected {} accounts in append batch, found 0 in queue, {} already processed",
            expected_count, already_processed
        )));
    } else {
        return Err(IngesterError::ParserError(format!(
            "Expected {} accounts in append batch, found {}",
            expected_count,
            accounts.len()
        )));
    }

    // 2. Remove inserted elements from the output queue.
    let query = accounts::Entity::update_many()
        .col_expr(accounts::Column::InOutputQueue, Expr::value(false))
        .filter(
            accounts::Column::LeafIndex
                .gte(batch_append_event.old_next_index as i64)
                .and(accounts::Column::LeafIndex.lt(batch_append_event.new_next_index as i64))
                .and(accounts::Column::Tree.eq(batch_append_event.merkle_tree_pubkey.to_vec())),
        )
        .build(txn.get_database_backend());

    txn.execute(query).await?;

    Ok(())
}

/// Persists a batch nullify event.
/// 1. Create leaf nodes with nullifier as leaf.
/// 2. Mark elements as nullified in tree
///     and remove them from the database nullifier queue.
async fn persist_batch_nullify_event(
    txn: &DatabaseTransaction,
    batch_nullify_event: &BatchEvent,
    leaf_nodes: &mut Vec<LeafNode>,
) -> Result<(), IngesterError> {
    let expected_count =
        (batch_nullify_event.new_next_index - batch_nullify_event.old_next_index) as usize;

    // For nullify events, we don't validate against the tree's next index
    // because nullify events update existing leaves, they don't append new ones.
    // Instead, we check if the accounts have already been nullified.

    let queue_start = batch_nullify_event.old_next_index as i64;
    let queue_end = batch_nullify_event.new_next_index as i64;

    let accounts = accounts::Entity::find()
        .filter(
            accounts::Column::NullifierQueueIndex
                .gte(queue_start)
                .and(accounts::Column::NullifierQueueIndex.lt(queue_end))
                .and(accounts::Column::Tree.eq(batch_nullify_event.merkle_tree_pubkey.to_vec()))
                .and(accounts::Column::Spent.eq(true)),
        )
        .order_by_asc(accounts::Column::NullifierQueueIndex)
        .all(txn)
        .await?;

    if accounts.is_empty() {
        // No accounts found in the nullifier queue for this range
        return Err(IngesterError::ParserError(format!(
            "Expected {} accounts in nullifier batch queue range [{}, {}), found 0",
            expected_count, queue_start, queue_end
        )));
    } else if accounts.len() != expected_count {
        return Err(IngesterError::ParserError(format!(
            "Expected {} accounts in nullifier batch, found {}",
            expected_count,
            accounts.len()
        )));
    }

    let mut expected_index = queue_start; // Use the queue start for validation

    for account in &accounts {
        // Queue indices must be sequential with no gaps
        let queue_index = account.nullifier_queue_index.ok_or_else(|| {
            IngesterError::ParserError("Missing nullifier queue index".to_string())
        })?;
        if queue_index != expected_index {
            return Err(IngesterError::ParserError(format!(
                "Gap in nullifier queue: expected {}, got {}",
                expected_index, queue_index
            )));
        }
        expected_index += 1;

        // Nullifier exists - Each account must have a non-null nullifier
        let nullifier = account
            .nullifier
            .as_ref()
            .ok_or_else(|| IngesterError::ParserError("Nullifier is missing".to_string()))?;

        leaf_nodes.push(LeafNode {
            tree: SerializablePubkey::try_from(account.tree.clone()).map_err(|_| {
                IngesterError::ParserError(
                    "Failed to convert tree to SerializablePubkey".to_string(),
                )
            })?,
            seq: Some(batch_nullify_event.sequence_number as u32),
            leaf_index: account.leaf_index as u32,
            hash: Hash::new(nullifier.as_slice()).map_err(|_| {
                IngesterError::ParserError("Failed to convert nullifier to Hash".to_string())
            })?,
        });
    }

    // 2. Mark elements as nullified in tree.
    //      We keep the NullifierQueueIndex to support re-indexing scenarios.
    let query = accounts::Entity::update_many()
        .col_expr(accounts::Column::NullifiedInTree, Expr::value(true))
        .filter(
            accounts::Column::NullifierQueueIndex
                .gte(queue_start)
                .and(accounts::Column::NullifierQueueIndex.lt(queue_end))
                .and(accounts::Column::Tree.eq(batch_nullify_event.merkle_tree_pubkey.to_vec())),
        )
        .build(txn.get_database_backend());

    txn.execute(query).await?;
    Ok(())
}

/// Persists a batch address append event.
/// 1. Create leaf nodes with the address value as leaf.
/// 2. Remove inserted elements from the database address queue.
async fn persist_batch_address_append_event(
    txn: &DatabaseTransaction,
    batch_address_append_event: &BatchEvent,
) -> Result<(), IngesterError> {
    let expected_count = (batch_address_append_event.new_next_index
        - batch_address_append_event.old_next_index) as usize;

    // Validate old_next_index matches the current state of the address tree
    let current_next_index = indexed_trees::Entity::find()
        .filter(
            indexed_trees::Column::Tree.eq(batch_address_append_event.merkle_tree_pubkey.to_vec()),
        )
        .order_by_desc(indexed_trees::Column::LeafIndex)
        .one(txn)
        .await?
        .map(|tree| (tree.leaf_index + 1) as u64)
        .unwrap_or(1); // Address tree has zeroeth element

    if !validate_batch_index(
        batch_address_append_event.old_next_index,
        current_next_index,
        "address append",
    )? {
        return Ok(());
    }

    // Address queue indices are 0-based, but batch updates use 1-based indices
    // (because address trees have a pre-initialized zeroth element)
    // So we need to offset by -1 when querying the queue
    let queue_start = (batch_address_append_event.old_next_index as i64) - 1;
    let queue_end = (batch_address_append_event.new_next_index as i64) - 1;

    let addresses = address_queues::Entity::find()
        .filter(
            address_queues::Column::QueueIndex
                .gte(queue_start)
                .and(address_queues::Column::QueueIndex.lt(queue_end))
                .and(
                    address_queues::Column::Tree
                        .eq(batch_address_append_event.merkle_tree_pubkey.to_vec()),
                ),
        )
        .order_by_asc(address_queues::Column::QueueIndex)
        .all(txn)
        .await?;

    if addresses.is_empty() {
        // Check if already processed (re-indexing scenario)
        let already_indexed = indexed_trees::Entity::find()
            .filter(
                indexed_trees::Column::Tree
                    .eq(batch_address_append_event.merkle_tree_pubkey.to_vec())
                    .and(
                        indexed_trees::Column::LeafIndex
                            .gte(batch_address_append_event.old_next_index as i64),
                    )
                    .and(
                        indexed_trees::Column::LeafIndex
                            .lt(batch_address_append_event.new_next_index as i64),
                    ),
            )
            .count(txn)
            .await?;

        if already_indexed >= expected_count as u64 {
            tracing::info!(
                "Address batch already processed: {} addresses already in indexed tree",
                already_indexed
            );
            return Ok(());
        }

        return Err(IngesterError::ParserError(format!(
            "Expected {} addresses in address append batch, found 0 in queue",
            expected_count
        )));
    } else if addresses.len() != expected_count {
        return Err(IngesterError::ParserError(format!(
            "Expected {} addresses in address append batch, found {}",
            expected_count,
            addresses.len()
        )));
    }

    // Process addresses and perform per-address validations
    let mut expected_queue_index = queue_start; // Use the offset queue index
    let mut address_values = Vec::with_capacity(expected_count);

    for address in &addresses {
        // Queue indices must be sequential with no gaps
        if address.queue_index != expected_queue_index {
            return Err(IngesterError::ParserError(format!(
                "Gap in address queue indices: expected {}, got {}",
                expected_queue_index, address.queue_index
            )));
        }
        expected_queue_index += 1;

        // Address exists - Each address must have a non-empty value
        if address.address.is_empty() {
            return Err(IngesterError::ParserError(
                "Address value is missing".to_string(),
            ));
        }

        address_values.push(address.address.clone());
    }

    // 1. Append the addresses to the indexed merkle tree.
    multi_append(
        txn,
        address_values,
        batch_address_append_event.merkle_tree_pubkey.to_vec(),
        DEFAULT_BATCH_ADDRESS_TREE_HEIGHT + 1,
        Some(batch_address_append_event.sequence_number as u32),
    )
    .await?;

    // 2. Remove inserted elements from the database address queue.
    address_queues::Entity::delete_many()
        .filter(
            address_queues::Column::QueueIndex
                .gte(queue_start)
                .and(address_queues::Column::QueueIndex.lt(queue_end))
                .and(
                    address_queues::Column::Tree
                        .eq(batch_address_append_event.merkle_tree_pubkey.to_vec()),
                ),
        )
        .exec(txn)
        .await?;

    Ok(())
}
