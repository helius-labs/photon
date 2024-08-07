use sea_orm_migration::prelude::*;

use super::model::table::StateTreeHistories;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(StateTreeHistories::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(StateTreeHistories::Tree).binary().not_null())
                    .col(
                        ColumnDef::new(StateTreeHistories::Seq)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(StateTreeHistories::LeafIdx)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(StateTreeHistories::TransactionSignature)
                            .binary()
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .name("pk_state_tree_histories")
                            .col(StateTreeHistories::Tree)
                            .col(StateTreeHistories::Seq),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(StateTreeHistories::Table).to_owned())
            .await?;
        Ok(())
    }
}
