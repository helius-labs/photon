use crate::common::typedefs::hash::Hash;
use crate::dao::generated::{accounts, token_accounts};
use crate::ingester::error::IngesterError;
use crate::ingester::persist::{
    execute_account_update_query_and_update_balances, AccountType, ModificationType,
};
use crate::migration::Expr;
use light_event::event::BatchNullifyContext;
use sea_orm::QueryFilter;
use sea_orm::{ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryTrait};

/// 1. Mark the input accounts as spent.
///     (From both V1 and V2 (batched) trees)
/// 2. Update account compressed sol balances.
/// 3. Update compressed token account balances.
pub async fn spend_input_accounts(
    txn: &DatabaseTransaction,
    in_accounts: &[Hash],
) -> Result<(), IngesterError> {
    // Perform the update operation on the identified records
    let query = accounts::Entity::update_many()
        .col_expr(accounts::Column::Spent, Expr::value(true))
        .col_expr(
            accounts::Column::PrevSpent,
            Expr::col(accounts::Column::Spent).into(),
        )
        .filter(
            accounts::Column::Hash.is_in(
                in_accounts
                    .iter()
                    .map(|account| account.to_vec())
                    .collect::<Vec<Vec<u8>>>(),
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

    let query = token_accounts::Entity::update_many()
        .col_expr(token_accounts::Column::Spent, Expr::value(true))
        .col_expr(
            token_accounts::Column::PrevSpent,
            Expr::col(token_accounts::Column::Spent).into(),
        )
        .filter(
            token_accounts::Column::Hash.is_in(
                in_accounts
                    .iter()
                    .map(|account| account.to_vec())
                    .collect::<Vec<Vec<u8>>>(),
            ),
        )
        .build(txn.get_database_backend());

    execute_account_update_query_and_update_balances(
        txn,
        query,
        AccountType::TokenAccount,
        ModificationType::Spend,
    )
    .await?;
    Ok(())
}

/// Update the nullifier queue index and nullifier of the input accounts in batched trees.
pub async fn spend_input_accounts_batched(
    txn: &DatabaseTransaction,
    accounts: &[BatchNullifyContext],
    slot: u64,
    tree_info_cache: &std::collections::HashMap<
        solana_pubkey::Pubkey,
        crate::ingester::parser::tree_info::TreeInfo,
    >,
) -> Result<(), IngesterError> {
    if accounts.is_empty() {
        return Ok(());
    }

    // Track nullifier counts per tree for event publishing
    let mut tree_nullifier_counts: std::collections::HashMap<solana_pubkey::Pubkey, usize> =
        std::collections::HashMap::new();

    for account in accounts {
        accounts::Entity::update_many()
            .filter(accounts::Column::Hash.eq(account.account_hash.to_vec()))
            .col_expr(
                accounts::Column::NullifierQueueIndex,
                Expr::value(account.nullifier_queue_index as i64),
            )
            .col_expr(
                accounts::Column::Nullifier,
                Expr::value(account.nullifier.to_vec()),
            )
            .col_expr(
                accounts::Column::TxHash,
                Expr::value(account.tx_hash.to_vec()),
            )
            .exec(txn)
            .await?;

        if let Some(account_model) = accounts::Entity::find()
            .filter(accounts::Column::Hash.eq(account.account_hash.to_vec()))
            .one(txn)
            .await?
        {
            if let Ok(tree_pubkey) = solana_pubkey::Pubkey::try_from(account_model.tree.as_slice())
            {
                *tree_nullifier_counts.entry(tree_pubkey).or_insert(0) += 1;
            }
        }
    }

    for (tree, count) in tree_nullifier_counts {
        if let Some(tree_info) = tree_info_cache.get(&tree) {
            crate::events::publish(crate::events::IngestionEvent::NullifierQueueInsert {
                tree,
                queue: tree_info.queue,
                count,
                slot,
            });
        }
    }

    Ok(())
}
