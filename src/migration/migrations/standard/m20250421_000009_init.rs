use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
};

use crate::migration::model::table::Accounts;

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
        match manager.get_database_backend() {
            DatabaseBackend::Postgres => {
                // Convert discriminator field to use bigint2 domain to handle larger values
                execute_sql(
                    manager,
                    "ALTER TABLE accounts ALTER COLUMN discriminator TYPE bigint2;",
                )
                    .await?;
            },
            DatabaseBackend::Sqlite => {
                // For SQLite, we need to recreate the table since SQLite doesn't support ALTER COLUMN directly
                execute_sql(
                    manager,
                    r#"
                    -- Create a new accounts table with the desired schema
                    CREATE TABLE accounts_new (
                        hash BLOB NOT NULL PRIMARY KEY,
                        data BLOB,
                        data_hash BLOB,
                        address BLOB,
                        owner BLOB NOT NULL,
                        tree BLOB NOT NULL,
                        queue BLOB NOT NULL,
                        leaf_index BIGINT NOT NULL,
                        seq BIGINT,
                        slot_created BIGINT NOT NULL,
                        spent BOOLEAN NOT NULL,
                        prev_spent BOOLEAN,
                        lamports REAL,
                        discriminator TEXT, -- Store as TEXT to handle larger values
                        in_output_queue BOOLEAN NOT NULL DEFAULT TRUE,
                        nullifier BLOB,
                        tx_hash BLOB,
                        nullifier_queue_index BIGINT NULL,
                        nullified_in_tree BOOLEAN NOT NULL DEFAULT FALSE,
                        tree_type INTEGER NULL
                    );

                    -- Copy data from old table to new table
                    INSERT INTO accounts_new
                    SELECT * FROM accounts;

                    -- Drop old table
                    DROP TABLE accounts;

                    -- Rename new table to the original name
                    ALTER TABLE accounts_new RENAME TO accounts;

                    -- Recreate indices
                    CREATE INDEX accounts_address_spent_idx ON accounts (address, seq);
                    CREATE UNIQUE INDEX accounts_owner_hash_idx ON accounts (spent, owner, hash);
                    CREATE INDEX accounts_queue_idx ON accounts (tree, in_output_queue, leaf_index) WHERE in_output_queue = 1;
                    "#,
                )
                    .await?;
            },
            _ => {
                return Err(DbErr::Migration("Unsupported database backend".to_string()));
            }
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        match manager.get_database_backend() {
            DatabaseBackend::Postgres => {
                // Revert discriminator field back to DECIMAL
                // Note: This might fail if there are values too large for the original type
                execute_sql(
                    manager,
                    "ALTER TABLE accounts ALTER COLUMN discriminator TYPE DECIMAL;",
                )
                    .await?;
            },
            DatabaseBackend::Sqlite => {
                // For SQLite, we need to recreate the table again
                execute_sql(
                    manager,
                    r#"
                    -- Create a new accounts table with the original schema
                    CREATE TABLE accounts_new (
                        hash BLOB NOT NULL PRIMARY KEY,
                        data BLOB,
                        data_hash BLOB,
                        address BLOB,
                        owner BLOB NOT NULL,
                        tree BLOB NOT NULL,
                        queue BLOB NOT NULL,
                        leaf_index BIGINT NOT NULL,
                        seq BIGINT,
                        slot_created BIGINT NOT NULL,
                        spent BOOLEAN NOT NULL,
                        prev_spent BOOLEAN,
                        lamports REAL,
                        discriminator REAL, -- Back to REAL type
                        in_output_queue BOOLEAN NOT NULL DEFAULT TRUE,
                        nullifier BLOB,
                        tx_hash BLOB,
                        nullifier_queue_index BIGINT NULL,
                        nullified_in_tree BOOLEAN NOT NULL DEFAULT FALSE,
                        tree_type INTEGER NULL
                    );

                    -- Copy data from old table to new table
                    INSERT INTO accounts_new
                    SELECT * FROM accounts;

                    -- Drop old table
                    DROP TABLE accounts;

                    -- Rename new table to the original name
                    ALTER TABLE accounts_new RENAME TO accounts;

                    -- Recreate indices
                    CREATE INDEX accounts_address_spent_idx ON accounts (address, seq);
                    CREATE UNIQUE INDEX accounts_owner_hash_idx ON accounts (spent, owner, hash);
                    CREATE INDEX accounts_queue_idx ON accounts (tree, in_output_queue, leaf_index) WHERE in_output_queue = 1;
                    "#,
                )
                    .await?;
            },
            _ => {
                return Err(DbErr::Migration("Unsupported database backend".to_string()));
            }
        }

        Ok(())
    }
}