use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

const INDEX_NAME: &str = "accounts_owner_history_idx";

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        let concurrently = if backend == DatabaseBackend::Postgres {
            "CONCURRENTLY "
        } else {
            ""
        };
        let connection = manager.get_connection();
        connection
            .execute(Statement::from_string(
                backend,
                format!(
                    "CREATE INDEX {concurrently}IF NOT EXISTS {INDEX_NAME} ON accounts (owner, hash)"
                ),
            ))
            .await?;

        if backend == DatabaseBackend::Postgres {
            // A failed concurrent build leaves an invalid index that IF NOT EXISTS would skip.
            let index = connection
                .query_one(Statement::from_string(
                    backend,
                    format!(
                        "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('{INDEX_NAME}')"
                    ),
                ))
                .await?
                .ok_or_else(|| DbErr::Custom(format!("Index {INDEX_NAME} was not created")))?;
            if !index.try_get::<bool>("", "indisvalid")? {
                return Err(DbErr::Custom(format!(
                    "Index {INDEX_NAME} is invalid; drop it concurrently before retrying this migration"
                )));
            }
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        let concurrently = if backend == DatabaseBackend::Postgres {
            "CONCURRENTLY "
        } else {
            ""
        };
        manager
            .get_connection()
            .execute(Statement::from_string(
                backend,
                format!("DROP INDEX {concurrently}IF EXISTS {INDEX_NAME}"),
            ))
            .await?;
        Ok(())
    }
}
