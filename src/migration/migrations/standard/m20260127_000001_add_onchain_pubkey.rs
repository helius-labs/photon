use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, Statement},
};

use super::super::super::model::table::Accounts;

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
        manager
            .alter_table(
                Table::alter()
                    .table(Accounts::Table)
                    .add_column(ColumnDef::new(Accounts::OnchainPubkey).binary().null())
                    .to_owned(),
            )
            .await?;

        execute_sql(
            manager,
            "CREATE INDEX accounts_onchain_pubkey_idx ON accounts (onchain_pubkey) WHERE NOT spent AND onchain_pubkey IS NOT NULL;",
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        execute_sql(manager, "DROP INDEX IF EXISTS accounts_onchain_pubkey_idx;").await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Accounts::Table)
                    .drop_column(Accounts::OnchainPubkey)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
