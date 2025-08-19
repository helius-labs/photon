use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Index for batch nullification checks
        // Used in queries filtering by (tree, nullified_in_tree, spent)
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_accounts_tree_nullified_spent")
                    .table(Alias::new("accounts"))
                    .col(Alias::new("tree"))
                    .col(Alias::new("nullified_in_tree"))
                    .col(Alias::new("spent"))
                    .to_owned(),
            )
            .await?;

        // Index for nullifier queue range queries
        // Used in batch nullification to find accounts in queue index range
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_accounts_tree_nullifier_queue")
                    .table(Alias::new("accounts"))
                    .col(Alias::new("tree"))
                    .col(Alias::new("nullifier_queue_index"))
                    .to_owned(),
            )
            .await?;

        // Index for address queue queries
        // Used in batch address append operations
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_address_queues_tree_queue_index")
                    .table(Alias::new("address_queues"))
                    .col(Alias::new("tree"))
                    .col(Alias::new("queue_index"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_address_queues_tree_queue_index")
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("idx_accounts_tree_nullifier_queue")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_index(
                Index::drop()
                    .name("idx_accounts_tree_nullified_spent")
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
