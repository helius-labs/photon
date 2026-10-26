use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

const INDEXES: [(&str, &str); 2] = [
    (
        "accounts_active_owner_leaf_hash_idx",
        "(owner, leaf_index, hash) WHERE spent = false",
    ),
    ("accounts_address_spent_lookup_idx", "(address, spent)"),
];

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
        for (name, columns) in INDEXES {
            connection
                .execute(Statement::from_string(
                    backend,
                    format!(
                        "CREATE INDEX {concurrently}IF NOT EXISTS {name} ON accounts {columns}"
                    ),
                ))
                .await?;
            if backend == DatabaseBackend::Postgres {
                let row = connection
                    .query_one(Statement::from_sql_and_values(
                        backend,
                        "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)",
                        [name.into()],
                    ))
                    .await?
                    .ok_or_else(|| DbErr::Custom(format!("Index {name} was not created")))?;
                if !row.try_get::<bool>("", "indisvalid")? {
                    return Err(DbErr::Custom(format!(
                        "Index {name} is invalid; drop it concurrently before retrying this migration"
                    )));
                }
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
        for (name, _) in INDEXES.into_iter().rev() {
            manager
                .get_connection()
                .execute(Statement::from_string(
                    backend,
                    format!("DROP INDEX {concurrently}IF EXISTS {name}"),
                ))
                .await?;
        }
        Ok(())
    }
}
