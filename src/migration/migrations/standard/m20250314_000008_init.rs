use crate::migration::model::table::AddressQueues;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(AddressQueues::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(AddressQueues::Tree).binary().not_null())
                    .col(ColumnDef::new(AddressQueues::Address).binary().not_null())
                    .col(
                        ColumnDef::new(AddressQueues::QueueIndex)
                            .big_integer()
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .name("pk_address_queue_elements")
                            .col(AddressQueues::Address),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("address_queue_elements_tree_value_idx")
                    .table(AddressQueues::Table)
                    .col(AddressQueues::Tree)
                    .col(AddressQueues::Address)
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(AddressQueues::Table).to_owned())
            .await?;

        Ok(())
    }
}
