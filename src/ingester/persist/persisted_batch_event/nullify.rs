use crate::dao::generated::accounts;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::BatchEvent;
use crate::ingester::persist::leaf_node::LeafNode;
use crate::migration::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryTrait,
};

use super::helpers::{
    check_nullify_already_processed, count_accounts_ready_to_nullify,
    count_nullify_accounts_in_output_queue, create_account_leaf_node, fetch_accounts_to_nullify,
};
use super::sequence::update_root_sequence;

/// Persists a batch nullify event.
/// 1. Create leaf nodes with nullifier as leaf.
/// 2. Mark elements as nullified in tree
///     and remove them from the database nullifier queue.
pub async fn persist_batch_nullify_event(
    txn: &DatabaseTransaction,
    batch_nullify_event: &BatchEvent,
    leaf_nodes: &mut Vec<LeafNode>,
) -> Result<(), IngesterError> {
    let expected_count = batch_nullify_event
        .new_next_index
        .saturating_sub(batch_nullify_event.old_next_index) as usize;

    // For nullify events, we don't validate against the tree's next index
    // because nullify events update existing leaves, they don't append new ones.
    // Instead, we check if the accounts have already been nullified.

    let queue_start = batch_nullify_event.old_next_index as i64;
    let queue_end = batch_nullify_event.new_next_index as i64;
    let tree_pubkey_str = bs58::encode(&batch_nullify_event.merkle_tree_pubkey).into_string();

    // Count accounts that are spent but not yet nullified in tree
    let queue_count = count_accounts_ready_to_nullify(
        txn,
        &batch_nullify_event.merkle_tree_pubkey.to_vec(),
        queue_start,
        queue_end,
    )
    .await?;

    // Three possible scenarios:
    // 1. Expected count matches - normal processing
    // 2. No accounts ready but all already nullified - re-indexing scenario
    // 3. Partial/missing accounts - error condition

    if queue_count == expected_count as u64 {
        // Normal case: we have exactly the accounts we expect to nullify
        // Continue with normal processing below
    } else if queue_count == 0 {
        // Check if this is a re-indexing scenario (accounts already nullified)
        let already_processed = check_nullify_already_processed(
            txn,
            &batch_nullify_event.merkle_tree_pubkey.to_vec(),
            queue_start,
            queue_end,
        )
        .await?;

        if already_processed {
            // Re-indexing scenario: all accounts already nullified
            // Still need to update sequence to match on-chain behavior
            log::debug!(
                "Batch nullify re-indexing: accounts already nullified for range [{}, {}), updating sequence",
                batch_nullify_event.old_next_index,
                batch_nullify_event.new_next_index
            );

            update_root_sequence(
                txn,
                &batch_nullify_event.merkle_tree_pubkey,
                batch_nullify_event.sequence_number,
            )
            .await?;

            return Ok(());
        }

        // No accounts found and not a re-indexing scenario - this is an error
        return Err(IngesterError::ParserError(format!(
            "Batch nullify failed: expected {} accounts in range [{}, {}), found none",
            expected_count, queue_start, queue_end
        )));
    } else {
        // Partial accounts found - gather more info for error message
        let in_queue_count = count_nullify_accounts_in_output_queue(
            txn,
            &batch_nullify_event.merkle_tree_pubkey.to_vec(),
            queue_start,
            queue_end,
        )
        .await?;

        let already_nullified = expected_count as u64 - queue_count - in_queue_count;

        return Err(IngesterError::ParserError(format!(
            "Batch nullify partial state: expected {} accounts for tree {} range [{}, {}). Found: {} ready, {} already nullified, {} in output queue",
            expected_count,
            tree_pubkey_str,
            queue_start,
            queue_end,
            queue_count,
            already_nullified,
            in_queue_count
        )));
    }

    let accounts = fetch_accounts_to_nullify(
        txn,
        &batch_nullify_event.merkle_tree_pubkey.to_vec(),
        queue_start,
        queue_end,
    )
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

    process_nullify_accounts(&accounts, batch_nullify_event, leaf_nodes)?;

    // 2. Mark elements as nullified in tree.
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

fn process_nullify_accounts(
    accounts: &[accounts::Model],
    batch_nullify_event: &BatchEvent,
    leaf_nodes: &mut Vec<LeafNode>,
) -> Result<(), IngesterError> {
    let mut expected_index = batch_nullify_event.old_next_index as i64;

    for account in accounts {
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

        // Create leaf node with nullifier
        leaf_nodes.push(create_account_leaf_node(
            account,
            &account.tree,
            batch_nullify_event.sequence_number,
            true, // use nullifier
        )?);
    }

    Ok(())
}
