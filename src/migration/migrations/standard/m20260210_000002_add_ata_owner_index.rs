use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

const ATA_OWNER_INDEX_NAME: &str = "token_accounts_spent_ata_owner_idx";

fn create_index_sql(backend: DatabaseBackend) -> Result<String, DbErr> {
    match backend {
        DatabaseBackend::Postgres => Ok(format!(
            "CREATE INDEX IF NOT EXISTS {ATA_OWNER_INDEX_NAME} \
             ON token_accounts (spent, ata_owner) \
             WHERE ata_owner IS NOT NULL;"
        )),
        DatabaseBackend::Sqlite => Ok(format!(
            "CREATE INDEX IF NOT EXISTS {ATA_OWNER_INDEX_NAME} \
             ON token_accounts (spent, ata_owner);"
        )),
        _ => Err(DbErr::Custom("Unsupported database backend".to_string())),
    }
}

fn drop_index_sql(backend: DatabaseBackend) -> Result<String, DbErr> {
    match backend {
        DatabaseBackend::Postgres | DatabaseBackend::Sqlite => {
            Ok(format!("DROP INDEX IF EXISTS {ATA_OWNER_INDEX_NAME};"))
        }
        _ => Err(DbErr::Custom("Unsupported database backend".to_string())),
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        let sql = create_index_sql(backend)?;
        manager
            .get_connection()
            .execute(Statement::from_string(backend, sql))
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        let sql = drop_index_sql(backend)?;
        manager
            .get_connection()
            .execute(Statement::from_string(backend, sql))
            .await?;
        Ok(())
    }
}
