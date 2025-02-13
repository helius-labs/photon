use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
};
use crate::migration::model::table::{Accounts, StateTrees, IndexedTrees};

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
                -- Create sequence for queue position
                CREATE TABLE IF NOT EXISTS queue_position_seq(id INTEGER PRIMARY KEY AUTOINCREMENT);

                -- Recreate Accounts table
                CREATE TABLE accounts_new (
                    hash BLOB NOT NULL PRIMARY KEY,
                    data BLOB,
                    data_hash BLOB,
                    address BLOB,
                    owner BLOB NOT NULL,
                    tree BLOB NOT NULL,
                    queue BLOB NULL,
                    leaf_index BIGINT NOT NULL,
                    seq BIGINT,
                    slot_created BIGINT NOT NULL,
                    spent BOOLEAN NOT NULL,
                    prev_spent BOOLEAN,
                    lamports REAL,
                    discriminator REAL,
                    in_queue BOOLEAN NOT NULL DEFAULT TRUE,
                    nullifier BLOB,
                    tx_hash BLOB,
                    queue_position INTEGER
                );

                INSERT INTO accounts_new
                SELECT
                    hash, data, data_hash, address, owner, tree, NULL as queue, leaf_index, seq,
                    slot_created, spent, prev_spent, lamports, discriminator,
                    FALSE as in_queue, NULL as nullifier, NULL as tx_hash, NULL as queue_position
                FROM accounts;

                DROP TABLE accounts;
                ALTER TABLE accounts_new RENAME TO accounts;

                -- Create trigger for auto-incrementing queue_position on INSERT
                CREATE TRIGGER set_queue_position_on_insert
                AFTER INSERT ON accounts
                WHEN NEW.in_queue = 1
                BEGIN
                    INSERT INTO queue_position_seq (id) VALUES (NULL);
                    UPDATE accounts SET queue_position = (SELECT last_insert_rowid() FROM queue_position_seq)
                    WHERE hash = NEW.hash;
                END;

                -- Create trigger for auto-incrementing queue_position on UPDATE
                CREATE TRIGGER set_queue_position_on_update
                AFTER UPDATE OF in_queue ON accounts
                WHEN NEW.in_queue = 1 AND OLD.in_queue = 0
                BEGIN
                    INSERT INTO queue_position_seq (id) VALUES (NULL);
                    UPDATE accounts SET queue_position = (SELECT last_insert_rowid() FROM queue_position_seq)
                    WHERE hash = NEW.hash;
                END;

                -- Create trigger for nulling queue_position when removing from queue
                CREATE TRIGGER null_queue_position_on_update
                AFTER UPDATE OF in_queue ON accounts
                WHEN NEW.in_queue = 0 AND OLD.in_queue = 1
                BEGIN
                    UPDATE accounts SET queue_position = NULL
                    WHERE hash = NEW.hash;
                END;

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
                CREATE INDEX accounts_queue_idx ON accounts (tree, in_queue, queue_position) WHERE in_queue = 1;
                "#,
            ).await?;
        } else {
            // PostgreSQL migration
            execute_sql(
                manager,
                "CREATE SEQUENCE IF NOT EXISTS queue_position_seq;",
            ).await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .add_column(ColumnDef::new(Accounts::InQueue).boolean().not_null().default(true))
                        .to_owned(),
                )
                .await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .add_column(ColumnDef::new(Accounts::Queue).binary().null())
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

            // Add queue_position column
            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .add_column(ColumnDef::new(Accounts::QueuePosition).big_integer().null())
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

            // Create function and triggers for managing queue_position
            execute_sql(
                manager,
                r#"
                CREATE OR REPLACE FUNCTION manage_queue_position()
                RETURNS TRIGGER AS $$
                BEGIN
                    IF TG_OP = 'INSERT' THEN
                        IF NEW.in_queue = true THEN
                            NEW.queue_position := nextval('queue_position_seq');
                        END IF;
                    ELSIF TG_OP = 'UPDATE' THEN
                        IF NEW.in_queue = true AND OLD.in_queue = false THEN
                            NEW.queue_position := nextval('queue_position_seq');
                        ELSIF NEW.in_queue = false AND OLD.in_queue = true THEN
                            NEW.queue_position := NULL;
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS manage_queue_position_trigger ON accounts;
                CREATE TRIGGER manage_queue_position_trigger
                BEFORE INSERT OR UPDATE OF in_queue ON accounts
                FOR EACH ROW
                EXECUTE FUNCTION manage_queue_position();
                "#,
            ).await?;

            // Create indexes
            execute_sql(
                manager,
                "CREATE INDEX state_trees_hash_idx ON state_trees (hash) WHERE level = 0;",
            ).await?;

            execute_sql(
                manager,
                "CREATE INDEX accounts_queue_idx ON accounts (tree, in_queue, queue_position) WHERE in_queue = true;",
            ).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.get_database_backend() == DatabaseBackend::Sqlite {
            execute_sql(
                manager,
                r#"
                -- Remove triggers
                DROP TRIGGER IF EXISTS set_queue_position_on_insert;
                DROP TRIGGER IF EXISTS set_queue_position_on_update;
                DROP TRIGGER IF EXISTS null_queue_position_on_update;
                DROP TABLE IF EXISTS queue_position_seq;

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
            ).await?;
        } else {
            // PostgreSQL down migration
            execute_sql(
                manager,
                r#"
                DROP TRIGGER IF EXISTS manage_queue_position_trigger ON accounts;
                DROP FUNCTION IF EXISTS manage_queue_position();
                DROP SEQUENCE IF EXISTS queue_position_seq;
                "#,
            ).await?;

            manager
                .alter_table(
                    Table::alter()
                        .table(Accounts::Table)
                        .drop_column(Accounts::InQueue)
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
                        .drop_column(Accounts::QueuePosition)
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
            ).await?;
        }

        Ok(())
    }
}