use sea_orm_migration::prelude::*;

use crate::model::table::{StateTrees, UTXOs};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(StateTrees::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(StateTrees::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(StateTrees::Tree).binary().not_null())
                    .col(ColumnDef::new(StateTrees::NodeIdx).big_integer().not_null())
                    .col(ColumnDef::new(StateTrees::LeafIdx).big_integer())
                    .col(ColumnDef::new(StateTrees::Level).big_integer().not_null())
                    .col(ColumnDef::new(StateTrees::Hash).binary().not_null())
                    .col(ColumnDef::new(StateTrees::Seq).big_integer().not_null())
                    .col(
                        ColumnDef::new(StateTrees::SlotUpdated)
                            .big_integer()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("state_trees_tree_node_idx")
                    .table(StateTrees::Table)
                    .col(StateTrees::Tree)
                    .col(StateTrees::NodeIdx)
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("state_trees_tree_leaf_idx")
                    .table(StateTrees::Table)
                    .col(StateTrees::Tree)
                    .col(StateTrees::LeafIdx)
                    .unique()
                    .to_owned(),
            )
            .await?;

        // TODO: Convert to sparse index.
        // We only need to index the leaf nodes.
        manager
            .create_index(
                Index::create()
                    .name("state_trees_hash_sparse_idx")
                    .table(StateTrees::Table)
                    .col(StateTrees::Hash)
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(UTXOs::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(UTXOs::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(UTXOs::Hash).binary().not_null())
                    .col(ColumnDef::new(UTXOs::Data).binary().not_null())
                    .col(ColumnDef::new(UTXOs::Account).binary())
                    .col(ColumnDef::new(UTXOs::Owner).binary().not_null())
                    .col(ColumnDef::new(UTXOs::Lamports).big_integer())
                    .col(ColumnDef::new(UTXOs::Tree).binary().not_null())
                    .col(ColumnDef::new(UTXOs::Seq).big_integer().not_null())
                    .col(ColumnDef::new(UTXOs::SlotUpdated).big_integer().not_null())
                    .col(ColumnDef::new(UTXOs::Spent).boolean().not_null())
                    .col(
                        ColumnDef::new(UTXOs::CreatedAt)
                            .timestamp()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("utxos_hash_idx")
                    .table(UTXOs::Table)
                    .col(UTXOs::Hash)
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("utxos_account_idx")
                    .table(UTXOs::Table)
                    .col(UTXOs::Account)
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(StateTrees::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(UTXOs::Table).to_owned())
            .await?;

        Ok(())
    }
}
