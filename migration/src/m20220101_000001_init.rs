use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
};

use crate::model::table::{StateTrees, TokenOwnership, UTXOs};

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

        // We only need to index the hash values for leaf nodes (UTXO identifier).
        manager
            .get_connection()
            .execute(Statement::from_string(
                DatabaseBackend::Postgres,
                "CREATE UNIQUE INDEX state_trees_hash_sparse_idx ON state_trees (hash) WHERE level = 0;
                ".to_string(),
            ))
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

        manager
            .create_table(
                Table::create()
                    .table(TokenOwnership::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(TokenOwnership::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(TokenOwnership::Owner).binary().not_null())
                    .col(ColumnDef::new(TokenOwnership::Mint).binary().not_null())
                    // TODO: Change this to a u64 here to avoid balance overflow.
                    .col(
                        ColumnDef::new(TokenOwnership::Amount)
                            .big_integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(TokenOwnership::Delegate).binary())
                    .col(ColumnDef::new(TokenOwnership::Frozen).boolean().not_null())
                    // TODO: Change this to a u64 here to avoid balance overflow.
                    .col(ColumnDef::new(TokenOwnership::IsNative).big_integer())
                    // TODO: Change this to a u64 here to avoid balance overflow.
                    .col(
                        ColumnDef::new(TokenOwnership::DelegatedAmount)
                            .big_integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(TokenOwnership::CloseAuthority).binary())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("token_ownership_owner_mint_idx")
                    .table(TokenOwnership::Table)
                    .col(TokenOwnership::Owner)
                    .col(TokenOwnership::Mint)
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

        manager
            .drop_table(Table::drop().table(TokenOwnership::Table).to_owned())
            .await?;

        Ok(())
    }
}
