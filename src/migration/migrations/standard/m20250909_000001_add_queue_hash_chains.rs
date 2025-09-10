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
                    .table(QueueHashChains::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(QueueHashChains::TreePubkey)
                            .binary_len(32)
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(QueueHashChains::QueueType)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(QueueHashChains::BatchStartIndex)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(QueueHashChains::ZkpBatchIndex)
                            .integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(QueueHashChains::StartOffset)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(QueueHashChains::HashChain)
                            .binary_len(32)
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .col(QueueHashChains::TreePubkey)
                            .col(QueueHashChains::QueueType)
                            .col(QueueHashChains::BatchStartIndex)
                            .col(QueueHashChains::ZkpBatchIndex),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_queue_hash_chains_tree_queue")
                    .table(QueueHashChains::Table)
                    .col(QueueHashChains::TreePubkey)
                    .col(QueueHashChains::QueueType)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(QueueHashChains::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum QueueHashChains {
    Table,
    TreePubkey,
    QueueType,
    BatchStartIndex,
    ZkpBatchIndex,
    StartOffset,
    HashChain,
}
