use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
};

use super::super::super::model::table::{Accounts, StateTrees, TokenAccounts};

use super::super::super::model::table::{
    AccountTransactions, Blocks, IndexedTrees, OwnerBalances, TokenOwnerBalances, Transactions,
};

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
        if manager.get_database_backend() == DatabaseBackend::Postgres {
            // Max balance of an individual account or token account is 2**64. bigint2 is 5000x larger than that to account for batch updates where update the database with new account balances
            // before closing spent accounts and extreme cases where a single wallet owns an excessive amount of account / tokens with high number of lamports.
            //
            // We go a bit overboard here, but it's ok because bigint2 is not the storage bottleneck.
            execute_sql(
                manager,
                "
                DO $$
                DECLARE
                    type_exists BOOLEAN := EXISTS (SELECT 1 FROM pg_type WHERE typname = 'bigint2');
                BEGIN
                    IF NOT type_exists THEN
                        CREATE DOMAIN bigint2 AS numeric(23, 0);
                    END IF;
                END $$;
                ",
            )
            .await?;
        }

        manager
            .create_table(
                Table::create()
                    .table(StateTrees::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(StateTrees::Tree).binary().not_null())
                    .col(ColumnDef::new(StateTrees::NodeIdx).big_integer().not_null())
                    .col(ColumnDef::new(StateTrees::LeafIdx).big_integer())
                    .col(ColumnDef::new(StateTrees::Level).big_integer().not_null())
                    .col(ColumnDef::new(StateTrees::Hash).binary().not_null())
                    .col(ColumnDef::new(StateTrees::Seq).big_integer().not_null())
                    .primary_key(
                        Index::create()
                            .name("pk_state_trees")
                            .col(StateTrees::Tree)
                            .col(StateTrees::NodeIdx),
                    )
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

        // We only need to index the hash values for leaf nodes (Account identifier).
        execute_sql(
            manager,
            "CREATE INDEX state_trees_hash_idx ON state_trees (hash) WHERE level = 0;",
        )
        .await?;

        manager
            .create_table(
                Table::create()
                    .table(Accounts::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Accounts::Hash).binary().not_null())
                    .col(ColumnDef::new(Accounts::Data).binary())
                    .col(ColumnDef::new(Accounts::DataHash).binary())
                    .col(ColumnDef::new(Accounts::Address).binary())
                    .col(ColumnDef::new(Accounts::Owner).binary().not_null())
                    .col(ColumnDef::new(Accounts::Tree).binary().not_null())
                    .col(ColumnDef::new(Accounts::LeafIndex).big_integer().not_null())
                    .col(ColumnDef::new(Accounts::Seq).big_integer().not_null())
                    .col(
                        ColumnDef::new(Accounts::SlotCreated)
                            .big_integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Accounts::Spent).boolean().not_null())
                    .col(ColumnDef::new(Accounts::PrevSpent).boolean())
                    .primary_key(Index::create().name("pk_accounts").col(Accounts::Hash))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("accounts_address_spent_idx")
                    .table(Accounts::Table)
                    .col(Accounts::Address)
                    .col(Accounts::Seq)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("accounts_owner_hash_idx")
                    .table(Accounts::Table)
                    .col(Accounts::Spent)
                    .col(Accounts::Owner)
                    .col(Accounts::Hash) // For pagination
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(TokenAccounts::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(TokenAccounts::Hash).binary().not_null())
                    .col(ColumnDef::new(TokenAccounts::Owner).binary().not_null())
                    .col(ColumnDef::new(TokenAccounts::Mint).binary().not_null())
                    .col(ColumnDef::new(TokenAccounts::Delegate).binary())
                    // TODO: We use an int to represent the state enum since SQL-lite does not
                    //       support enums.
                    .col(ColumnDef::new(TokenAccounts::State).integer().not_null())
                    .col(ColumnDef::new(TokenAccounts::Spent).boolean().not_null())
                    .col(ColumnDef::new(TokenAccounts::PrevSpent).boolean())
                    .foreign_key(
                        ForeignKey::create()
                            .name("token_accounts_hash_fk")
                            .from(TokenAccounts::Table, TokenAccounts::Hash)
                            .to(Accounts::Table, Accounts::Hash)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .primary_key(
                        Index::create()
                            .name("pk_token_accounts")
                            .col(TokenAccounts::Hash),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(OwnerBalances::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(OwnerBalances::Owner).binary().not_null())
                    .primary_key(
                        Index::create()
                            .name("pk_owner_balances")
                            .col(OwnerBalances::Owner),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(TokenOwnerBalances::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(TokenOwnerBalances::Owner)
                            .binary()
                            .not_null(),
                    )
                    .col(ColumnDef::new(TokenOwnerBalances::Mint).binary().not_null())
                    .primary_key(
                        Index::create()
                            .name("pk_token_owner_balances")
                            .col(TokenOwnerBalances::Owner)
                            .col(TokenOwnerBalances::Mint),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(IndexedTrees::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(IndexedTrees::Tree).binary().not_null())
                    .col(
                        ColumnDef::new(IndexedTrees::LeafIndex)
                            .big_integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(IndexedTrees::Value).binary().not_null())
                    .col(
                        ColumnDef::new(IndexedTrees::NextIndex)
                            .big_integer()
                            .not_null(),
                    )
                    .col(ColumnDef::new(IndexedTrees::NextValue).binary().not_null())
                    .col(ColumnDef::new(IndexedTrees::Seq).big_integer().not_null())
                    .primary_key(
                        Index::create()
                            .name("pk_indexed_trees")
                            .col(IndexedTrees::Tree)
                            .col(IndexedTrees::LeafIndex),
                    )
                    .to_owned(),
            )
            .await?;

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

        match manager.get_database_backend() {
            DatabaseBackend::Postgres => {
                execute_sql(
                    manager,
                    "ALTER TABLE accounts ADD COLUMN lamports bigint2 NOT NULL;",
                )
                .await?;

                execute_sql(
                    manager,
                    "ALTER TABLE accounts ADD COLUMN discriminator bigint2;",
                )
                .await?;

                execute_sql(
                    manager,
                    "ALTER TABLE token_accounts ADD COLUMN amount bigint2 NOT NULL;",
                )
                .await?;

                execute_sql(
                    manager,
                    "ALTER TABLE owner_balances ADD COLUMN lamports bigint2 NOT NULL;",
                )
                .await?;

                execute_sql(
                    manager,
                    "ALTER TABLE token_owner_balances ADD COLUMN amount bigint2 NOT NULL;",
                )
                .await?;
            }
            DatabaseBackend::Sqlite => {
                // HACK: SQLx Decimal is not compatible with INTEGER so we use REAL instead.
                execute_sql(manager, "ALTER TABLE accounts ADD COLUMN lamports REAL;").await?;

                execute_sql(
                    manager,
                    "ALTER TABLE accounts ADD COLUMN discriminator REAL;",
                )
                .await?;

                execute_sql(
                    manager,
                    "ALTER TABLE token_accounts ADD COLUMN amount REAL;",
                )
                .await?;

                execute_sql(
                    manager,
                    "ALTER TABLE owner_balances ADD COLUMN lamports REAL;",
                )
                .await?;

                execute_sql(
                    manager,
                    "ALTER TABLE token_owner_balances ADD COLUMN amount REAL;",
                )
                .await?;
            }
            _ => {
                unimplemented!("Unsupported database type")
            }
        }

        manager
            .create_index(
                Index::create()
                    .name("token_accounts_owner_mint_hash_idx")
                    .table(TokenAccounts::Table)
                    .col(TokenAccounts::Spent)
                    .col(TokenAccounts::Owner)
                    .col(TokenAccounts::Mint)
                    .col(TokenAccounts::Hash) // For pagination
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("token_accounts_delegate_mint_hash_idx")
                    .table(TokenAccounts::Table)
                    .col(TokenAccounts::Spent)
                    .col(TokenAccounts::Delegate)
                    .col(TokenAccounts::Mint)
                    .col(TokenAccounts::Hash) // For pagination
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Blocks::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Blocks::Slot).big_integer().not_null())
                    .col(ColumnDef::new(Blocks::ParentSlot).big_integer().not_null())
                    .col(ColumnDef::new(Blocks::ParentBlockhash).binary().not_null())
                    .col(ColumnDef::new(Blocks::Blockhash).binary().not_null())
                    .col(ColumnDef::new(Blocks::BlockHeight).big_integer().not_null())
                    .col(ColumnDef::new(Blocks::BlockTime).big_integer().not_null())
                    .primary_key(Index::create().name("pk_blocks").col(Blocks::Slot))
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Transactions::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Transactions::Signature).binary().not_null())
                    .col(ColumnDef::new(Transactions::Slot).big_integer().not_null())
                    .col(
                        ColumnDef::new(Transactions::UsesCompression)
                            .boolean()
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .name("pk_transactions")
                            .col(Transactions::Signature),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("transactions_slot_fk")
                            .from(Transactions::Table, Transactions::Slot)
                            .to(Blocks::Table, Blocks::Slot)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("transactions_slot_signature_idx")
                    .table(Transactions::Table)
                    .col(Transactions::Slot)
                    .col(Transactions::Signature)
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("transactions_uses_compression_slot_signature_idx")
                    .table(Transactions::Table)
                    .col(Transactions::UsesCompression)
                    .col(Transactions::Slot)
                    .col(Transactions::Signature)
                    .unique()
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(AccountTransactions::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(AccountTransactions::Hash)
                            .binary()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(AccountTransactions::Signature)
                            .binary()
                            .not_null(),
                    )
                    .primary_key(
                        Index::create()
                            .name("pk_account_transaction_history")
                            .col(AccountTransactions::Hash)
                            .col(AccountTransactions::Signature),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("account_transactions_hash_fk")
                            .from(AccountTransactions::Table, AccountTransactions::Hash)
                            .to(Accounts::Table, Accounts::Hash)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("account_transactions_signature_fk")
                            .from(AccountTransactions::Table, AccountTransactions::Signature)
                            .to(Transactions::Table, Transactions::Signature)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
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
            .drop_table(Table::drop().table(Accounts::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(TokenAccounts::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(Blocks::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(Transactions::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(AccountTransactions::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(OwnerBalances::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(TokenOwnerBalances::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(IndexedTrees::Table).to_owned())
            .await?;
        Ok(())
    }
}
