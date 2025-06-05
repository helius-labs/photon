use crate::migration::model::table::{Accounts, IndexedTrees, StateTrees};
use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
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
        if manager.get_database_backend() == DatabaseBackend::Sqlite {
            execute_sql(
                manager,
                r#"
                -- Recreate Accounts table
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
                    discriminator REAL,
                    in_output_queue BOOLEAN NOT NULL DEFAULT TRUE,
                    nullifier BLOB,
                    tx_hash BLOB,
                    nullifier_queue_index BIGINT NULL,
                    nullified_in_tree BOOLEAN NOT NULL DEFAULT FALSE,
                    tree_type INTEGER NULL
                );

                INSERT INTO accounts_new
                SELECT
                    hash, data, data_hash, address, owner, tree, NULL as queue, leaf_index, seq,
                    slot_created, spent, prev_spent, lamports, discriminator,
                    FALSE as in_output_queue, NULL as nullifier, NULL as tx_hash, NULL as nullifier_queue_index,
                    FALSE as nullified_in_tree, NULL as tree_type
                FROM accounts;

                DROP TABLE accounts;
                ALTER TABLE accounts_new RENAME TO accounts;

                -- Recreate state_trees table
                CREATE TABLE state_trees_new (
                    tree BLOB NOT NULL,
                    node_idx BIGINT NOT NULL,
                    leaf_idx BIGINT,
                    level BIGINT NOT NULL,
                    hash BLOB NOT NULL,
                    seq BIGINT,
                    PRIMARY KEY (tree, node_idx)
                );

                INSERT INTO state_trees_new
                SELECT tree, node_idx, leaf_idx, level, hash, seq
                FROM state_trees;

                DROP TABLE state_trees;
                ALTER TABLE state_trees_new RENAME TO state_trees;

                -- Recreate indexed_trees table
                CREATE TABLE indexed_trees_new (
                    tree BLOB NOT NULL,
                    leaf_index BIGINT NOT NULL,
                    value BLOB NOT NULL,
                    next_index BIGINT NOT NULL,
                    next_value BLOB NOT NULL,
                    seq BIGINT,
                    PRIMARY KEY (tree, leaf_index)
                );

                INSERT INTO indexed_trees_new
                SELECT tree, leaf_index, value, next_index, next_value, seq
                FROM indexed_trees;

                DROP TABLE indexed_trees;
                ALTER TABLE indexed_trees_new RENAME TO indexed_trees;

                -- Recreate indexes
                CREATE INDEX accounts_address_spent_idx ON accounts (address, seq);
                CREATE UNIQUE INDEX accounts_owner_hash_idx ON accounts (spent, owner, hash);
                CREATE UNIQUE INDEX state_trees_tree_leaf_idx ON state_trees (tree, leaf_idx);
                CREATE INDEX state_trees_hash_idx ON state_trees (hash);
                CREATE UNIQUE INDEX indexed_trees_value_idx ON indexed_trees (value);
                CREATE INDEX accounts_queue_idx ON accounts (tree, in_output_queue, leaf_index) WHERE in_output_queue = 1;
                "#,
            ).await?;
        } else {
            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .add_column(ColumnDef::new(Accounts::TreeType).integer().null())
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .add_column(
                            ColumnDef::new(Accounts::NullifiedInTree)
                                .boolean()
                                .not_null()
                                .default(false),
                        )
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .add_column(
                            ColumnDef::new(Accounts::NullifierQueueIndex)
                                .big_integer()
                                .null(),
                        )
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .add_column(
                            ColumnDef::new(Accounts::InOutputQueue)
                                .boolean()
                                .not_null()
                                .default(true),
                        )
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .add_column(ColumnDef::new(Accounts::Queue).binary().not_null())
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .add_column(ColumnDef::new(Accounts::Nullifier).binary().null())
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .add_column(ColumnDef::new(Accounts::TxHash).binary().null())
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .modify_column(ColumnDef::new(Accounts::Seq).big_integer().null())
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(StateTrees::Table)
                        .modify_column(ColumnDef::new(StateTrees::Seq).big_integer().null())
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(IndexedTrees::Table)
                        .modify_column(ColumnDef::new(IndexedTrees::Seq).big_integer().null())
                        .to_owned(),
                )
                .await?;

            execute_sql(
                manager,
                "CREATE INDEX accounts_queue_idx ON accounts (tree, in_output_queue, leaf_index) WHERE in_output_queue = true;",
            ).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.get_database_backend() == DatabaseBackend::Sqlite {
            execute_sql(
                manager,
                r#"
                -- Recreate Accounts table without new columns and with non-null seq
                CREATE TABLE accounts_new (
                    hash BLOB NOT NULL PRIMARY KEY,
                    data BLOB,
                    data_hash BLOB,
                    address BLOB,
                    owner BLOB NOT NULL,
                    tree BLOB NOT NULL,
                    leaf_index BIGINT NOT NULL,
                    seq BIGINT NOT NULL,
                    slot_created BIGINT NOT NULL,
                    spent BOOLEAN NOT NULL,
                    prev_spent BOOLEAN,
                    lamports REAL,
                    discriminator REAL
                );

                INSERT INTO accounts_new
                SELECT
                    hash, data, data_hash, address, owner, tree, leaf_index,
                    COALESCE(seq, 0) as seq,
                    slot_created, spent, prev_spent, lamports, discriminator
                FROM accounts;

                DROP TABLE accounts;
                ALTER TABLE accounts_new RENAME TO accounts;

                -- Recreate state_trees table
                CREATE TABLE state_trees_new (
                    tree BLOB NOT NULL,
                    node_idx BIGINT NOT NULL,
                    leaf_idx BIGINT,
                    level BIGINT NOT NULL,
                    hash BLOB NOT NULL,
                    seq BIGINT NOT NULL,
                    PRIMARY KEY (tree, node_idx)
                );

                INSERT INTO state_trees_new
                SELECT tree, node_idx, leaf_idx, level, hash, COALESCE(seq, 0)
                FROM state_trees;

                DROP TABLE state_trees;
                ALTER TABLE state_trees_new RENAME TO state_trees;

                -- Recreate indexed_trees table
                CREATE TABLE indexed_trees_new (
                    tree BLOB NOT NULL,
                    leaf_index BIGINT NOT NULL,
                    value BLOB NOT NULL,
                    next_index BIGINT NOT NULL,
                    next_value BLOB NOT NULL,
                    seq BIGINT NOT NULL,
                    PRIMARY KEY (tree, leaf_index)
                );

                INSERT INTO indexed_trees_new
                SELECT tree, leaf_index, value, next_index, next_value, COALESCE(seq, 0)
                FROM indexed_trees;

                DROP TABLE indexed_trees;
                ALTER TABLE indexed_trees_new RENAME TO indexed_trees;

                -- Recreate indexes
                CREATE INDEX accounts_address_spent_idx ON accounts (address, seq);
                CREATE UNIQUE INDEX accounts_owner_hash_idx ON accounts (spent, owner, hash);
                CREATE UNIQUE INDEX state_trees_tree_leaf_idx ON state_trees (tree, leaf_idx);
                CREATE INDEX state_trees_hash_idx ON state_trees (hash);
                CREATE UNIQUE INDEX indexed_trees_value_idx ON indexed_trees (value);
                "#,
            )
            .await?;
        } else {
            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .drop_column(Accounts::TreeType)
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .drop_column(Accounts::NullifiedInTree)
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .drop_column(Accounts::NullifierQueueIndex)
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .drop_column(Accounts::InOutputQueue)
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .drop_column(Accounts::Queue)
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .drop_column(Accounts::Nullifier)
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .drop_column(Accounts::TxHash)
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .modify_column(ColumnDef::new(Accounts::Seq).big_integer().not_null())
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(StateTrees::Table)
                        .modify_column(ColumnDef::new(StateTrees::Seq).big_integer().not_null())
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(IndexedTrees::Table)
                        .modify_column(ColumnDef::new(IndexedTrees::Seq).big_integer().not_null())
                        .to_owned(),
                )
                .await?;

            execute_sql(
                manager,
                "CREATE INDEX state_trees_hash_idx ON state_trees (hash) WHERE level = 0;",
            )
            .await?;
        }

        Ok(())
    }
}
