use sea_orm_migration::prelude::*;

use crate::migration::model::table::TokenAccounts;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(TokenAccounts::Table)
                    .add_column(ColumnDef::new(TokenAccounts::Tlv).binary().null())
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(TokenAccounts::Table)
                    .drop_column(TokenAccounts::Tlv)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}
