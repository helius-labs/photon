use crate::dao::generated::accounts;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::BatchEvent;
use crate::ingester::persist::leaf_node::LeafNode;
use crate::migration::Expr;
use log::debug;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryTrait,
};

use super::helpers::{create_account_leaf_node, fetch_accounts_to_append, get_tree_next_index};
use super::sequence::update_root_sequence;

/// Persists a batch append event.
/// 1. Create leaf nodes with the account hash as leaf.
/// 2. Remove inserted elements from the database output queue.
pub async fn persist_batch_append_event(
    txn: &DatabaseTransaction,
    batch_append_event: &BatchEvent,
    leaf_nodes: &mut Vec<LeafNode>,
) -> Result<(), IngesterError> {
    let expected_count = batch_append_event
        .new_next_index
        .saturating_sub(batch_append_event.old_next_index) as usize;

    let current_next_index =
        get_tree_next_index(txn, batch_append_event.merkle_tree_pubkey.as_ref()).await?;

    // If old_next_index doesn't match current state, check if already processed
    if batch_append_event.old_next_index != current_next_index {
        return if batch_append_event.old_next_index < current_next_index {
            // This event has already been processed (re-indexing scenario)
            debug!(
                "Batch append already processed: old_next_index {} < current_index {}",
                batch_append_event.old_next_index, current_next_index
            );
            // Still update sequence to ensure it's correct
            update_root_sequence(
                txn,
                &batch_append_event.merkle_tree_pubkey,
                batch_append_event.sequence_number,
            )
            .await?;
            Ok(())
        } else {
            // Gap in processing - this is an error
            Err(IngesterError::ParserError(format!(
                "Batch append gap: expected old_next_index {}, got {}",
                current_next_index, batch_append_event.old_next_index
            )))
        };
    }

    let accounts = fetch_accounts_to_append(
        txn,
        batch_append_event.merkle_tree_pubkey.as_ref(),
        batch_append_event.old_next_index as i64,
        batch_append_event.new_next_index as i64,
    )
    .await?;

    // Process accounts based on what we found
    if !accounts.is_empty() && accounts.len() == expected_count {
        process_append_accounts(&accounts, batch_append_event, leaf_nodes)?;

        // If all accounts were nullified_in_tree and we created no leaf nodes,
        // we still need to update the sequence number to match on-chain behavior.
        if leaf_nodes.is_empty() && !accounts.is_empty() {
            update_root_sequence(
                txn,
                &batch_append_event.merkle_tree_pubkey,
                batch_append_event.sequence_number,
            )
            .await?;
        }
    } else if accounts.is_empty() {
        // No accounts found in queue - this is an error since we already handled re-indexing above
        return Err(IngesterError::ParserError(format!(
            "Expected {} accounts in append batch, found 0 in queue",
            expected_count
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

fn process_append_accounts(
    accounts: &[accounts::Model],
    batch_append_event: &BatchEvent,
    leaf_nodes: &mut Vec<LeafNode>,
) -> Result<(), IngesterError> {
    // Based on the on-chain circuit logic:
    // ALL accounts get processed at their sequential positions
    // Spent accounts keep their nullifier hash (old value)
    // Non-spent accounts get their account hash (new value)
    let mut expected_leaf_index = batch_append_event.old_next_index;

    for account in accounts {
        // Validate indices are sequential
        if account.leaf_index != expected_leaf_index as i64 {
            return Err(IngesterError::ParserError(format!(
                "Gap in leaf indices: expected {}, got {}",
                expected_leaf_index, account.leaf_index
            )));
        }

        // Skip accounts that have already been nullified in the tree
        if account.nullified_in_tree {
            // This account was already processed by a batch nullify event
            // The tree already has the nullifier at this position
            // Skip it entirely - the circuit will keep the old value
            expected_leaf_index += 1;
            continue;
        }

        // For all other accounts (spent or not, but not yet in tree), use the account hash
        leaf_nodes.push(create_account_leaf_node(
            account,
            &account.tree,
            batch_append_event.sequence_number,
            false, // use account hash
        )?);

        expected_leaf_index += 1;
    }

    Ok(())
}
