use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, Statement},
};

use super::super::super::model::table::{Accounts, Mints};

#[derive(DeriveMigrationName)]
pub struct Migration;

async fn execute_sql<'a>(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute(Statement::from_string(
            manager.get_database_backend(),
            sql.to_string(),
        ))
        .await?;
    Ok(())
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create the mints table
        manager
            .create_table(
                Table::create()
                    .table(Mints::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Mints::Hash).binary().not_null())
                    .col(ColumnDef::new(Mints::Address).binary().not_null())
                    .col(ColumnDef::new(Mints::MintPda).binary().not_null())
                    .col(ColumnDef::new(Mints::MintSigner).binary().not_null())
                    .col(ColumnDef::new(Mints::MintAuthority).binary())
                    .col(ColumnDef::new(Mints::FreezeAuthority).binary())
                    .col(ColumnDef::new(Mints::Supply).big_integer().not_null())
                    .col(ColumnDef::new(Mints::Decimals).small_integer().not_null())
                    .col(ColumnDef::new(Mints::Version).small_integer().not_null())
                    .col(
                        ColumnDef::new(Mints::MintDecompressed)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(ColumnDef::new(Mints::Extensions).binary())
                    .col(ColumnDef::new(Mints::Spent).boolean().not_null())
                    .col(ColumnDef::new(Mints::PrevSpent).boolean())
                    .foreign_key(
                        ForeignKey::create()
                            .name("mints_hash_fk")
                            .from(Mints::Table, Mints::Hash)
                            .to(Accounts::Table, Accounts::Hash)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .primary_key(Index::create().name("pk_mints").col(Mints::Hash))
                    .to_owned(),
            )
            .await?;

        // Create index on address for lookups by compressed address
        manager
            .create_index(
                Index::create()
                    .name("mints_address_idx")
                    .table(Mints::Table)
                    .col(Mints::Address)
                    .unique()
                    .to_owned(),
            )
            .await?;

        // Create index on mint_pda for lookups by Solana PDA
        execute_sql(
            manager,
            "CREATE INDEX mints_mint_pda_idx ON mints (mint_pda) WHERE NOT spent;",
        )
        .await?;

        // Create index for queries by mint_authority
        manager
            .create_index(
                Index::create()
                    .name("mints_authority_hash_idx")
                    .table(Mints::Table)
                    .col(Mints::Spent)
                    .col(Mints::MintAuthority)
                    .col(Mints::Hash)
                    .to_owned(),
            )
            .await?;

        // Create index for queries by freeze_authority
        manager
            .create_index(
                Index::create()
                    .name("mints_freeze_authority_hash_idx")
                    .table(Mints::Table)
                    .col(Mints::Spent)
                    .col(Mints::FreezeAuthority)
                    .col(Mints::Hash)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Mints::Table).to_owned())
            .await?;

        Ok(())
    }
}
