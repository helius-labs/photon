use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

use crate::migration::model::table::AccountTransactions;

#[derive(DeriveMigrationName)]
pub struct Migration;

async fn execute_sql<'a>(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
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
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS account_transactions_signature_idx ON account_transactions (signature);",
            )
            .await?;
        } else {
            // For other databases, create index normally
            manager
                .create_index(
                    Index::create()
                        .name("account_transactions_signature_idx")
                        .table(AccountTransactions::Table)
                        .col(AccountTransactions::Signature)
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("account_transactions_signature_idx")
                    .table(AccountTransactions::Table)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
