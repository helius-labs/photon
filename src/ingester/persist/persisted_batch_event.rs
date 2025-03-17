use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::{accounts, address_queue};
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::BatchEvent;
use crate::ingester::parser::{
    indexer_events::MerkleTreeEvent, merkle_tree_events_parser::IndexedBatchEvents,
};
use crate::ingester::persist::leaf_node::{persist_leaf_nodes, LeafNode};
use crate::ingester::persist::persisted_indexed_merkle_tree::multi_append;
use crate::ingester::persist::MAX_SQL_INSERTS;
use crate::migration::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder,
    QueryTrait,
};

/// We need to find the events of the same tree:
/// - order them by sequence number and execute them in order
///     HashMap<pubkey, Vec<Event(BatchAppendEvent, seq)>>
/// - execute a single function call to persist all changed nodes
pub async fn persist_batch_events(
    txn: &DatabaseTransaction,
    mut events: IndexedBatchEvents,
) -> Result<(), IngesterError> {
    for (_, events) in events.iter_mut() {
        events.sort_by(|a, b| a.0.cmp(&b.0));

        // Process each event in sequence
        for (_, event) in events.iter() {
            // Batch size is 500 for batched State Merkle trees.
            let mut leaf_nodes = Vec::with_capacity(500);
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
                persist_leaf_nodes(txn, leaf_nodes).await?;
            } else {
                // Currently not used but a safeguard in case the batch size changes.
                for leaf_nodes_chunk in leaf_nodes.chunks(MAX_SQL_INSERTS) {
                    persist_leaf_nodes(txn, leaf_nodes_chunk.to_vec()).await?;
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
    // 1. Create leaf nodes with the account hash as leaf.
    //      Leaf indices are used as output queue indices.
    //      The leaf index range of the batch append event is
    //      [old_next_index, new_next_index).
    let accounts = accounts::Entity::find()
        .filter(
            accounts::Column::LeafIndex
                .gte(batch_append_event.old_next_index as i64)
                .and(accounts::Column::LeafIndex.lt(batch_append_event.new_next_index as i64))
                .and(accounts::Column::NullifiedInTree.eq(false))
                .and(accounts::Column::Tree.eq(batch_append_event.merkle_tree_pubkey.to_vec())),
        )
        .order_by_asc(accounts::Column::LeafIndex)
        .all(txn)
        .await?;
    accounts
        .iter()
        .try_for_each(|account| -> Result<(), IngesterError> {
            leaf_nodes.push(LeafNode {
                tree: SerializablePubkey::try_from(account.tree.clone()).map_err(|_| {
                    IngesterError::ParserError(
                        "Failed to convert tree to SerializablePubkey".to_string(),
                    )
                })?,
                seq: Some(batch_append_event.sequence_number as u32),
                leaf_index: account.leaf_index as u32,
                hash: Hash::new(account.hash.as_slice()).map_err(|_| {
                    IngesterError::ParserError("Failed to convert nullifier to Hash".to_string())
                })?,
            });

            Ok(())
        })?;

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
    // 1. Create leaf nodes with nullifier as leaf.
    //      Nullifier queue index is continuously incremented by 1
    //      with each element insertion into the nullifier queue.
    let accounts = accounts::Entity::find()
        .filter(
            accounts::Column::NullifierQueueIndex
                .gte(batch_nullify_event.old_next_index)
                .and(accounts::Column::NullifierQueueIndex.lt(batch_nullify_event.new_next_index)),
        )
        .order_by_asc(accounts::Column::NullifierQueueIndex)
        .all(txn)
        .await?;
    accounts
        .iter()
        .try_for_each(|account| -> Result<(), IngesterError> {
            leaf_nodes.push(LeafNode {
                tree: SerializablePubkey::try_from(account.tree.clone()).map_err(|_| {
                    IngesterError::ParserError(
                        "Failed to convert tree to SerializablePubkey".to_string(),
                    )
                })?,
                seq: Some(batch_nullify_event.sequence_number as u32),
                leaf_index: account.leaf_index as u32,
                hash: Hash::new(
                    account
                        .nullifier
                        .as_ref()
                        .ok_or(IngesterError::ParserError(
                            "Nullifier is missing".to_string(),
                        ))?
                        .as_slice(),
                )
                .map_err(|_| {
                    IngesterError::ParserError("Failed to convert nullifier to Hash".to_string())
                })?,
            });

            Ok(())
        })?;

    // 2. Mark elements as nullified in tree and
    //      remove them from the database nullifier queue.
    let query = accounts::Entity::update_many()
        .col_expr(
            accounts::Column::NullifierQueueIndex,
            Expr::value(Option::<i64>::None),
        )
        .col_expr(accounts::Column::NullifiedInTree, Expr::value(true))
        .filter(
            accounts::Column::NullifierQueueIndex
                .gte(batch_nullify_event.old_next_index)
                .and(accounts::Column::NullifierQueueIndex.lt(batch_nullify_event.new_next_index)),
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
    let addresses = address_queue::Entity::find()
        .filter(
            address_queue::Column::QueueIndex
                .lt(batch_address_append_event.new_next_index as i64 - 1)
                .and(
                    address_queue::Column::Tree
                        .eq(batch_address_append_event.merkle_tree_pubkey.to_vec()),
                ),
        )
        .order_by_asc(address_queue::Column::QueueIndex)
        .all(txn)
        .await?;

    let address_values = addresses
        .iter()
        .map(|address| address.address.clone())
        .collect::<Vec<_>>();
    multi_append(
        txn,
        address_values,
        batch_address_append_event.merkle_tree_pubkey.to_vec(),
    )
    .await?;
    address_queue::Entity::delete_many()
        .filter(
            address_queue::Column::QueueIndex
                .lt(batch_address_append_event.new_next_index as i64 - 1)
                .and(
                    address_queue::Column::Tree
                        .eq(batch_address_append_event.merkle_tree_pubkey.to_vec()),
                ),
        )
        .exec(txn)
        .await?;

    Ok(())
}
