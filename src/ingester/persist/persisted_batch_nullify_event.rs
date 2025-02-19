use light_batched_merkle_tree::event::BatchNullifyEvent;
use log::info;
use sea_orm::{ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryTrait};
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::ingester::error::IngesterError;
use crate::ingester::persist::{execute_account_update_query_and_update_balances, AccountType, ModificationType, BATCH_STATE_TREE_HEIGHT};
use crate::ingester::persist::persisted_state_tree::{persist_leaf_nodes, LeafNode};
use crate::migration::Expr;

pub async fn persist_batch_nullify(txn: &DatabaseTransaction, batch_nullify: Vec<BatchNullifyEvent>) -> Result<(), IngesterError> {
    for batch_nullify_event in batch_nullify {
        info!("Filtering accounts by batch_index: {}, batch_size: {}", batch_nullify_event.zkp_batch_index, batch_nullify_event.batch_size);
        let accounts = accounts::Entity::find()
            .filter(
                accounts::Column::NullifierQueueIndex
                    .gte(
                        batch_nullify_event.zkp_batch_index as i64
                            * batch_nullify_event.batch_size as i64,
                    )
                    .and(
                        accounts::Column::NullifierQueueIndex
                            .lt((batch_nullify_event.zkp_batch_index + 1) as i64
                                * batch_nullify_event.batch_size as i64),
                    ),
            )
            .all(txn)
            .await?;
        info!(
            "Batch nullify event: {:?}, accounts: {:?}",
            batch_nullify_event, accounts
        );

        let query = accounts::Entity::update_many()
            .col_expr(accounts::Column::NullifierQueueIndex, Expr::value(Option::<i64>::None))
            .col_expr(accounts::Column::NullifiedInTree, Expr::value(true))
            .filter(
                accounts::Column::NullifierQueueIndex
                    .gte(
                        batch_nullify_event.zkp_batch_index as i64
                            * batch_nullify_event.batch_size as i64,
                    )
                    .and(
                        accounts::Column::NullifierQueueIndex
                            .lt((batch_nullify_event.zkp_batch_index + 1) as i64
                                * batch_nullify_event.batch_size as i64),
                    ),
            )
            .build(txn.get_database_backend());
        execute_account_update_query_and_update_balances(
            txn,
            query,
            AccountType::Account,
            ModificationType::Spend,
        )
            .await?;


        persist_leaf_nodes(
            txn,
            accounts
                .iter()
                .map(|account| LeafNode {
                    tree: SerializablePubkey::try_from(account.tree.clone()).unwrap(),
                    seq: Some(batch_nullify_event.sequence_number as u32),
                    leaf_index: account.leaf_index as u32,
                    hash: Hash::try_from(account.nullifier.clone().unwrap().clone()).unwrap(),
                })
                .collect(),
            BATCH_STATE_TREE_HEIGHT,
        )
            .await?;
    }
    Ok(())
}