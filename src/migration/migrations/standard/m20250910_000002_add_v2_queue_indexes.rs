use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Index for v2 output queue monitoring - fetching accounts by tree and leaf_index range
        // Used in fetch_all_output_queue_elements for OutputStateV2 monitoring
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_accounts_tree_leaf_index")
                    .table(Alias::new("accounts"))
                    .col(Alias::new("tree"))
                    .col(Alias::new("leaf_index"))
                    .to_owned(),
            )
            .await?;

        // Composite index for nullifier queue queries with range filtering
        // Optimizes the query: WHERE tree = ? AND nullifier_queue_index IS NOT NULL
        // AND nullifier_queue_index >= ? AND nullifier_queue_index < ?
        manager
            .create_index(
                Index::create()
                    .if_not_exists()
                    .name("idx_accounts_nullifier_queue_range")
                    .table(Alias::new("accounts"))
                    .col(Alias::new("tree"))
                    .col(Alias::new("nullifier_queue_index"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_accounts_nullifier_queue_range")
                    .to_owned(),
            )
            .await?;

        manager
            .drop_index(
                Index::drop()
                    .name("idx_accounts_tree_leaf_index")
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
