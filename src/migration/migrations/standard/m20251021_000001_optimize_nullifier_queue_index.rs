use sea_orm::{ConnectionTrait, Statement};
use sea_orm_migration::prelude::*;

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
        execute_sql(
            manager,
            "DROP INDEX IF EXISTS idx_accounts_nullifier_queue_range;",
        )
        .await?;

        match manager.get_database_backend() {
            sea_orm::DatabaseBackend::Postgres => {
                execute_sql(
                    manager,
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_accounts_nullifier_queue_optimized \
                     ON accounts (tree, nullifier_queue_index) \
                     WHERE nullified_in_tree = false AND nullifier_queue_index IS NOT NULL;",
                )
                .await?;
            }
            sea_orm::DatabaseBackend::Sqlite => {
                execute_sql(
                    manager,
                    "CREATE INDEX IF NOT EXISTS idx_accounts_nullifier_queue_optimized \
                     ON accounts (tree, nullifier_queue_index) \
                     WHERE nullified_in_tree = 0 AND nullifier_queue_index IS NOT NULL;",
                )
                .await?;
            }
            _ => return Err(DbErr::Custom("Unsupported database backend".to_string())),
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_sql(
            manager,
            "DROP INDEX IF EXISTS idx_accounts_nullifier_queue_optimized;",
        )
        .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_accounts_nullifier_queue_range")
                    .table(Alias::new("accounts"))
                    .col(Alias::new("tree"))
                    .col(Alias::new("nullifier_queue_index"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
