use std::collections::HashMap;

use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::ingester::error::IngesterError;
use crate::ingester::persist::leaf_node::{persist_leaf_nodes, LeafNode};
use crate::migration::Expr;
use light_batched_merkle_tree::event::{BatchAppendEvent, BatchNullifyEvent};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryTrait,
};

pub enum BatchEvent<'a> {
    BatchAppend(&'a BatchAppendEvent),
    BatchNullify(&'a BatchNullifyEvent),
}

/// We need to find the events of the same tree:
/// - order them by sequence number and execute them in order
/// HashMap<pubkey, Vec<Event(BatchAppendEvent, seq)>>
/// - execute a single function call to persist all changed nodes
pub async fn persist_batch_events(
    txn: &DatabaseTransaction,
    batch_append: Vec<BatchAppendEvent>,
    batch_nullify: Vec<BatchNullifyEvent>,
) -> Result<(), IngesterError> {
    let mut leaf_nodes = Vec::new();

    let mut trees = HashMap::new();
    for batch_append_event in batch_append.iter() {
        let tree = batch_append_event.merkle_tree_pubkey;
        let seq = batch_append_event.sequence_number;
        let events = trees.entry(tree).or_insert_with(Vec::new);
        events.push((BatchEvent::BatchAppend(batch_append_event), seq));
    }
    for batch_nullify_event in batch_nullify.iter() {
        let tree = batch_nullify_event.merkle_tree_pubkey;
        let seq = batch_nullify_event.sequence_number;
        let events = trees.entry(tree).or_insert_with(Vec::new);
        events.push((BatchEvent::BatchNullify(batch_nullify_event), seq));
    }
    for (_, events) in trees.iter_mut() {
        events.sort_by(|a, b| a.1.cmp(&b.1));
        match events.first().unwrap().0 {
            BatchEvent::BatchNullify(batch_nullify_event) => {
                persist_batch_nullify_event(txn, batch_nullify_event, &mut leaf_nodes).await?
            }
            BatchEvent::BatchAppend(batch_append_event) => {
                persist_batch_append_event(txn, batch_append_event, &mut leaf_nodes).await?;
            }
        };
    }
    log::info!("persist_leaf_nodes {:?}", leaf_nodes);

    persist_leaf_nodes(txn, leaf_nodes).await?;
    Ok(())
}

/// Persists a batch append event.
/// 1. Create leaf nodes with the account hash as leaf.
/// 2. Remove inserted elements from the output queue.
async fn persist_batch_append_event<'a>(
    txn: &DatabaseTransaction,
    batch_append_event: &'a BatchAppendEvent,
    leaf_nodes: &mut Vec<LeafNode>,
) -> Result<(), IngesterError> {
    // Leaf indices are used as output queue indices.
    // The leaf index range of the batch append event is
    // [old_next_index, new_next_index).
    let accounts = accounts::Entity::find()
        .filter(
            accounts::Column::LeafIndex
                .gte(batch_append_event.old_next_index as i64)
                .and(accounts::Column::LeafIndex.lt(batch_append_event.new_next_index as i64))
                .and(accounts::Column::NullifiedInTree.eq(0))
                .and(accounts::Column::Tree.eq(batch_append_event.merkle_tree_pubkey.to_vec())),
        )
        .all(txn)
        .await?;
    accounts.iter().for_each(|account| {
        leaf_nodes.push(LeafNode {
            tree: SerializablePubkey::try_from(account.tree.clone()).unwrap(),
            seq: Some(batch_append_event.sequence_number as u32),
            leaf_index: account.leaf_index as u32,
            hash: Hash::try_from(account.hash.clone()).unwrap(),
        })
    });

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
///     and remove them from the nullifier queue.
async fn persist_batch_nullify_event<'a>(
    txn: &DatabaseTransaction,
    batch_nullify_event: &'a BatchNullifyEvent,
    leaf_nodes: &mut Vec<LeafNode>,
) -> Result<(), IngesterError> {
    // Nullifier queue index is continously incremented by 1
    // with each element insertion into the nullifier queue.
    // The batch event sequence number is incremented by 1
    // with each batch update which creates a batch nullify event.
    // -> The nullifier queue index range of the batch nullify event is
    // [sequence_number * batch_size, (sequence_number + 1) * batch_size)
    let accounts = accounts::Entity::find()
        .filter(
            accounts::Column::NullifierQueueIndex
                .gte(
                    batch_nullify_event.sequence_number as i64
                        * batch_nullify_event.batch_size as i64,
                )
                .and(
                    accounts::Column::NullifierQueueIndex
                        .lt((batch_nullify_event.sequence_number + 1) as i64
                            * batch_nullify_event.batch_size as i64),
                ),
        )
        .all(txn)
        .await?;
    accounts.iter().for_each(|account| {
        leaf_nodes.push(LeafNode {
            tree: SerializablePubkey::try_from(account.tree.clone()).unwrap(),
            seq: Some(batch_nullify_event.sequence_number as u32),
            leaf_index: account.leaf_index as u32,
            hash: Hash::new(account.nullifier.as_ref().unwrap().as_slice()).unwrap(),
        })
    });

    // 3. Mark elements as nullified in tree and remove them from the nullifier queue.
    let query = accounts::Entity::update_many()
        .col_expr(
            accounts::Column::NullifierQueueIndex,
            Expr::value(Option::<i64>::None),
        )
        .col_expr(accounts::Column::NullifiedInTree, Expr::value(true))
        .filter(
            accounts::Column::NullifierQueueIndex
                .gte(
                    batch_nullify_event.sequence_number as i64
                        * batch_nullify_event.batch_size as i64,
                )
                .and(
                    accounts::Column::NullifierQueueIndex
                        .lt((batch_nullify_event.sequence_number + 1) as i64
                            * batch_nullify_event.batch_size as i64),
                ),
        )
        .build(txn.get_database_backend());
    txn.execute(query).await?;
    Ok(())
}
