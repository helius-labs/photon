use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::ingester::error::IngesterError;
use crate::ingester::persist::persisted_state_tree::{persist_leaf_nodes, LeafNode};
use crate::ingester::persist::{
    execute_account_update_query_and_update_balances, AccountType, ModificationType,
};
use crate::migration::Expr;
use light_batched_merkle_tree::event::BatchAppendEvent;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryTrait,
};

pub async fn persist_batch_append(
    txn: &DatabaseTransaction,
    batch_append: Vec<BatchAppendEvent>,
) -> Result<(), IngesterError> {
    for batch_append_event in batch_append {
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

        persist_leaf_nodes(
            txn,
            accounts
                .iter()
                .map(|account| LeafNode {
                    tree: SerializablePubkey::try_from(account.tree.clone()).unwrap(),
                    seq: Some(batch_append_event.sequence_number as u32),
                    leaf_index: account.leaf_index as u32,
                    hash: Hash::try_from(account.hash.clone()).unwrap(),
                })
                .collect(),
        )
        .await?;

        let query = accounts::Entity::update_many()
            .col_expr(accounts::Column::InOutputQueue, Expr::value(false))
            .filter(
                accounts::Column::LeafIndex
                    .gte(batch_append_event.old_next_index as i64)
                    .and(accounts::Column::LeafIndex.lt(batch_append_event.new_next_index as i64))
                    .and(accounts::Column::Tree.eq(batch_append_event.merkle_tree_pubkey.to_vec())),
            )
            .build(txn.get_database_backend());
        execute_account_update_query_and_update_balances(
            txn,
            query,
            AccountType::Account,
            ModificationType::Spend,
        )
        .await?;
    }
    Ok(())
}
