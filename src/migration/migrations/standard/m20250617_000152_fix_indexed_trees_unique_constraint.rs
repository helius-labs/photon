use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the existing unique index on value only
        manager
            .drop_index(
                Index::drop()
                    .name("indexed_trees_value_idx")
                    .table(IndexedTrees::Table)
                    .to_owned(),
            )
            .await?;

        // Create new unique index on (tree, value)
        manager
            .create_index(
                Index::create()
                    .name("indexed_trees_tree_value_idx")
                    .table(IndexedTrees::Table)
                    .col(IndexedTrees::Tree)
                    .col(IndexedTrees::Value)
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the new index
        manager
            .drop_index(
                Index::drop()
                    .name("indexed_trees_tree_value_idx")
                    .table(IndexedTrees::Table)
                    .to_owned(),
            )
            .await?;

        // Recreate the original unique index on value only
        manager
            .create_index(
                Index::create()
                    .name("indexed_trees_value_idx")
                    .table(IndexedTrees::Table)
                    .col(IndexedTrees::Value)
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(Iden)]
enum IndexedTrees {
    Table,
    Tree,
    Value,
}
