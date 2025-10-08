use sea_orm::DeriveIden;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(TreeMetadata::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(TreeMetadata::TreePubkey)
                            .binary_len(32)
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(TreeMetadata::QueuePubkey)
                            .binary_len(32)
                            .not_null(),
                    )
                    .col(ColumnDef::new(TreeMetadata::TreeType).integer().not_null())
                    .col(ColumnDef::new(TreeMetadata::Height).integer().not_null())
                    .col(
                        ColumnDef::new(TreeMetadata::RootHistoryCapacity)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TreeMetadata::SequenceNumber)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TreeMetadata::NextIndex)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TreeMetadata::LastSyncedSlot)
                            .big_integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await?;

        // Create index for queue pubkey lookups
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_tree_metadata_queue_pubkey")
                    .table(TreeMetadata::Table)
                    .col(TreeMetadata::QueuePubkey)
                    .to_owned(),
            )
            .await?;

        // Create index for tree type queries
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_tree_metadata_tree_type")
                    .table(TreeMetadata::Table)
                    .col(TreeMetadata::TreeType)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(TreeMetadata::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum TreeMetadata {
    Table,
    TreePubkey,
    QueuePubkey,
    TreeType,
    Height,
    RootHistoryCapacity,
    SequenceNumber,
    NextIndex,
    LastSyncedSlot,
}
