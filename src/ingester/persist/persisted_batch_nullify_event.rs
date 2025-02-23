use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::ingester::error::IngesterError;
use crate::ingester::persist::leaf_node::{persist_leaf_nodes, LeafNode};
use crate::migration::Expr;
use light_batched_merkle_tree::event::BatchNullifyEvent;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryTrait,
};

pub async fn persist_batch_nullify(
    txn: &DatabaseTransaction,
    batch_nullify: Vec<BatchNullifyEvent>,
) -> Result<(), IngesterError> {
    for batch_nullify_event in batch_nullify {
        let accounts = accounts::Entity::find()
            .filter(
                accounts::Column::NullifierQueueIndex
                    .gte(
                        batch_nullify_event.zkp_batch_index as i64
                            * batch_nullify_event.batch_size as i64,
                    )
                    .and(
                        accounts::Column::NullifierQueueIndex.lt((batch_nullify_event
                            .zkp_batch_index
                            + 1)
                            as i64
                            * batch_nullify_event.batch_size as i64),
                    ),
            )
            .all(txn)
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
        )
        .await?;

        let query = accounts::Entity::update_many()
            .col_expr(
                accounts::Column::NullifierQueueIndex,
                Expr::value(Option::<i64>::None),
            )
            .col_expr(accounts::Column::NullifiedInTree, Expr::value(true))
            .filter(
                accounts::Column::NullifierQueueIndex
                    .gte(
                        batch_nullify_event.zkp_batch_index as i64
                            * batch_nullify_event.batch_size as i64,
                    )
                    .and(
                        accounts::Column::NullifierQueueIndex.lt((batch_nullify_event
                            .zkp_batch_index
                            + 1)
                            as i64
                            * batch_nullify_event.batch_size as i64),
                    ),
            )
            .build(txn.get_database_backend());
        txn.execute(query).await?;
    }
    Ok(())
}
