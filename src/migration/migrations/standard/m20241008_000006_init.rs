use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

use crate::migration::model::table::TokenOwnerBalances;

#[derive(DeriveMigrationName)]
pub struct Migration;

async fn execute_sql(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute(Statement::from_string(
            manager.get_database_backend(),
            sql.to_string(),
        ))
        .await?;
    Ok(())
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.get_database_backend() == DatabaseBackend::Postgres {
            // Create index concurrently for Postgres
            execute_sql(
                manager,
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS token_holder_mint_balance_owner_idx ON token_owner_balances (mint, amount, owner);",
            )
            .await?;
        } else {
            // For other databases, create index normally
            execute_sql(
                manager,
                "CREATE INDEX IF NOT EXISTS token_holder_mint_balance_owner_idx ON token_owner_balances (mint, amount, owner);",
            )
            .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("token_holder_mint_balance_owner_idx")
                    .table(TokenOwnerBalances::Table)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
