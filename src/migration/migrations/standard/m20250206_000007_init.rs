use sea_orm_migration::prelude::*;

use crate::migration::model::table::Accounts;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Accounts::Table)
                    .add_column(ColumnDef::new(Accounts::QueueIndex).big_integer().null())
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Accounts::Table)
                    .add_column(ColumnDef::new(Accounts::Queue).binary().null())
                    .to_owned(),
            )
            .await?;


        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Accounts::Table)
                    .drop_column(Accounts::QueueIndex)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Accounts::Table)
                    .drop_column(Accounts::Queue)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
