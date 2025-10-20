use crate::dao::generated::{address_queues, indexed_trees};
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::BatchEvent;
use crate::ingester::persist::persisted_indexed_merkle_tree::multi_append;
use light_batched_merkle_tree::constants::DEFAULT_BATCH_ADDRESS_TREE_HEIGHT;
use log::debug;
use sea_orm::{
    ColumnTrait, DatabaseTransaction, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};

use super::helpers::validate_batch_index;

/// Persists a batch address append event.
/// 1. Create leaf nodes with the address value as leaf.
/// 2. Remove inserted elements from the database address queue.
pub async fn persist_batch_address_append_event(
    txn: &DatabaseTransaction,
    batch_address_append_event: &BatchEvent,
    tree_info_cache: &std::collections::HashMap<
        solana_pubkey::Pubkey,
        crate::ingester::parser::tree_info::TreeInfo,
    >,
) -> Result<(), IngesterError> {
    let expected_count = batch_address_append_event
        .new_next_index
        .saturating_sub(batch_address_append_event.old_next_index)
        as usize;

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
        let queue_end = (batch_address_append_event.new_next_index as i64) - 1;
        cleanup_stale_address_queue_entries(txn, batch_address_append_event, queue_end).await?;

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
            debug!(
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
    let address_values = validate_and_collect_addresses(&addresses, queue_start)?;

    // 1. Append the addresses to the indexed merkle tree.
    multi_append(
        txn,
        address_values,
        batch_address_append_event.merkle_tree_pubkey.to_vec(),
        DEFAULT_BATCH_ADDRESS_TREE_HEIGHT + 1,
        Some(batch_address_append_event.sequence_number as u32),
        tree_info_cache,
    )
    .await?;

    // 2. Remove inserted elements from the database address queue.
    address_queues::Entity::delete_many()
        .filter(address_queues::Column::QueueIndex.lt(queue_end).and(
            address_queues::Column::Tree.eq(batch_address_append_event.merkle_tree_pubkey.to_vec()),
        ))
        .exec(txn)
        .await?;

    Ok(())
}

async fn cleanup_stale_address_queue_entries(
    txn: &DatabaseTransaction,
    batch_address_append_event: &BatchEvent,
    queue_end: i64,
) -> Result<(), IngesterError> {
    address_queues::Entity::delete_many()
        .filter(address_queues::Column::QueueIndex.lt(queue_end).and(
            address_queues::Column::Tree.eq(batch_address_append_event.merkle_tree_pubkey.to_vec()),
        ))
        .exec(txn)
        .await?;

    Ok(())
}

fn validate_and_collect_addresses(
    addresses: &[address_queues::Model],
    queue_start: i64,
) -> Result<Vec<Vec<u8>>, IngesterError> {
    let mut expected_queue_index = queue_start;
    let mut address_values = Vec::with_capacity(addresses.len());

    for address in addresses {
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

    Ok(address_values)
}
