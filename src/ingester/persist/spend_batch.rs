use crate::dao::generated::{accounts, token_accounts};
use crate::ingester::error::IngesterError;
use crate::ingester::parser::state_update::AccountContext;
use crate::ingester::persist::{
    execute_account_update_query_and_update_balances, AccountType, ModificationType,
    MAX_SQL_INSERTS,
};
use crate::migration::Expr;
use sea_orm::QueryFilter;
use sea_orm::{ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryTrait};
use std::collections::HashMap;

pub async fn spend_input_accounts_batched(
    txn: &DatabaseTransaction,
    accounts: &[AccountContext],
) -> Result<(), IngesterError> {
    if accounts.is_empty() {
        return Ok(());
    }
    let account_hashes: Vec<Vec<u8>> = accounts
        .iter()
        .map(|account| account.account.to_vec())
        .collect();

    let account_context_map: HashMap<Vec<u8>, &AccountContext> = accounts
        .iter()
        .map(|ctx| (ctx.account.to_vec(), ctx))
        .collect();

    let accounts_to_update = accounts::Entity::find()
        .filter(accounts::Column::Hash.is_in(account_hashes.clone()))
        .all(txn)
        .await?;

    for chunk in accounts_to_update.chunks(MAX_SQL_INSERTS) {
        let mut update_many = accounts::Entity::update_many()
            .col_expr(accounts::Column::Spent, Expr::value(true))
            .col_expr(
                accounts::Column::PrevSpent,
                Expr::col(accounts::Column::Spent).into(),
            );

        for account in chunk {
            if let Some(ctx) = account_context_map.get(&account.hash) {
                update_many = update_many.filter(accounts::Column::Hash.eq(account.hash.clone()));

                update_many = update_many
                    .col_expr(
                        accounts::Column::NullifierQueueIndex,
                        Expr::value(ctx.nullifier_queue_index as i64),
                    )
                    .col_expr(
                        accounts::Column::Nullifier,
                        Expr::value(ctx.nullifier.to_vec()),
                    )
                    .col_expr(accounts::Column::TxHash, Expr::value(ctx.tx_hash.to_vec()));
            }
        }

        let query = update_many.build(txn.get_database_backend());

        execute_account_update_query_and_update_balances(
            txn,
            query,
            AccountType::Account,
            ModificationType::Spend,
        )
        .await?;
    }

    let token_query = token_accounts::Entity::update_many()
        .col_expr(token_accounts::Column::Spent, Expr::value(true))
        .col_expr(
            token_accounts::Column::PrevSpent,
            Expr::col(token_accounts::Column::Spent).into(),
        )
        .filter(token_accounts::Column::Hash.is_in(account_hashes))
        .build(txn.get_database_backend());

    execute_account_update_query_and_update_balances(
        txn,
        token_query,
        AccountType::TokenAccount,
        ModificationType::Spend,
    )
    .await?;

    Ok(())
}
