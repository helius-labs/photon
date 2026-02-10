use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, Statement},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

async fn execute_sql(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
    log::info!("Executing SQL: {}", sql);
    manager
        .get_connection()
        .execute(Statement::from_string(
            manager.get_database_backend(),
            sql.to_string(),
        ))
        .await?;
    Ok(())
}

fn postgres_numeric_to_little_endian_hex_expr(value_expr: &str) -> String {
    format!(
        "lpad(to_hex((mod({value_expr}, 256))::int), 2, '0') || \
         lpad(to_hex((mod(trunc({value_expr} / 256), 256))::int), 2, '0') || \
         lpad(to_hex((mod(trunc({value_expr} / 65536), 256))::int), 2, '0') || \
         lpad(to_hex((mod(trunc({value_expr} / 16777216), 256))::int), 2, '0') || \
         lpad(to_hex((mod(trunc({value_expr} / 4294967296), 256))::int), 2, '0') || \
         lpad(to_hex((mod(trunc({value_expr} / 1099511627776), 256))::int), 2, '0') || \
         lpad(to_hex((mod(trunc({value_expr} / 281474976710656), 256))::int), 2, '0') || \
         lpad(to_hex((mod(trunc({value_expr} / 72057594037927936), 256))::int), 2, '0')",
    )
}

fn postgres_discriminator_backfill_sql() -> String {
    let discriminator_hex_expr = postgres_numeric_to_little_endian_hex_expr("discriminator");
    format!(
        r#"
        UPDATE accounts
        SET discriminator_new =
            CASE WHEN discriminator IS NOT NULL THEN
                decode(
                    {discriminator_hex_expr},
                    'hex'
                )
            ELSE NULL END;
        "#
    )
}

/// Migration to change discriminator column from REAL/Decimal to BLOB.
/// This fixes precision loss for large u64 discriminator values in SQLite.
///
/// SQLite REAL (IEEE 754 double) only has 53 bits of mantissa precision,
/// which causes precision loss for u64 values > 2^53 (like Anchor discriminators).
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        log::info!("Running m20260201_000001_add_discriminator_blob UP migration");
        match manager.get_database_backend() {
            sea_orm::DatabaseBackend::Sqlite => {
                log::info!("SQLite backend detected");
                // SQLite doesn't support ALTER COLUMN, so we recreate the table
                // with discriminator as BLOB instead of REAL.
                // Use a unique temp table name to avoid conflicts
                execute_sql(
                    manager,
                    r#"
                    CREATE TABLE accounts_discriminator_migration (
                        hash BLOB PRIMARY KEY NOT NULL,
                        data BLOB,
                        data_hash BLOB,
                        address BLOB,
                        onchain_pubkey BLOB,
                        owner BLOB NOT NULL,
                        tree BLOB NOT NULL,
                        leaf_index BIGINT NOT NULL,
                        seq BIGINT,
                        slot_created BIGINT NOT NULL,
                        spent BOOLEAN NOT NULL,
                        prev_spent BOOLEAN,
                        lamports REAL,
                        discriminator BLOB,
                        tree_type INTEGER,
                        nullified_in_tree BOOLEAN NOT NULL DEFAULT FALSE,
                        nullifier_queue_index BIGINT,
                        in_output_queue BOOLEAN NOT NULL DEFAULT TRUE,
                        queue BLOB,
                        nullifier BLOB,
                        tx_hash BLOB
                    );
                    "#,
                )
                .await?;

                log::info!("Created temp table, copying data...");
                // Copy data - note that existing REAL discriminator data will lose precision
                // when converted, but new data will be stored correctly as BLOB.
                // We skip discriminator during copy since it had precision loss anyway.
                execute_sql(
                    manager,
                    r#"
                    INSERT INTO accounts_discriminator_migration (
                        hash, data, data_hash, address, onchain_pubkey, owner, tree,
                        leaf_index, seq, slot_created, spent, prev_spent, lamports,
                        tree_type, nullified_in_tree, nullifier_queue_index,
                        in_output_queue, queue, nullifier, tx_hash
                    )
                    SELECT
                        hash, data, data_hash, address, onchain_pubkey, owner, tree,
                        leaf_index, seq, slot_created, spent, prev_spent, lamports,
                        tree_type, nullified_in_tree, nullifier_queue_index,
                        in_output_queue, queue, nullifier, tx_hash
                    FROM accounts;
                    "#,
                )
                .await?;

                log::info!("Data copied, dropping old indexes...");
                // Drop ALL indexes on the accounts table first, then the table
                // These indexes are created by various migrations:
                execute_sql(manager, "DROP INDEX IF EXISTS accounts_owner_idx;").await?;
                execute_sql(manager, "DROP INDEX IF EXISTS accounts_address_idx;").await?;
                execute_sql(manager, "DROP INDEX IF EXISTS accounts_onchain_pubkey_idx;").await?;
                execute_sql(manager, "DROP INDEX IF EXISTS accounts_address_spent_idx;").await?;
                execute_sql(manager, "DROP INDEX IF EXISTS accounts_owner_hash_idx;").await?;
                execute_sql(manager, "DROP INDEX IF EXISTS accounts_queue_idx;").await?;
                execute_sql(
                    manager,
                    "DROP INDEX IF EXISTS idx_accounts_nullifier_queue_optimized;",
                )
                .await?;

                log::info!("Indexes dropped, dropping old table...");
                execute_sql(manager, "DROP TABLE IF EXISTS accounts;").await?;

                log::info!(
                    "Old table dropped, creating new accounts table with BLOB discriminator..."
                );
                // Instead of RENAME (which has issues with connection pools),
                // create the new table directly and copy data from temp table
                execute_sql(
                    manager,
                    r#"
                    CREATE TABLE accounts (
                        hash BLOB PRIMARY KEY NOT NULL,
                        data BLOB,
                        data_hash BLOB,
                        address BLOB,
                        onchain_pubkey BLOB,
                        owner BLOB NOT NULL,
                        tree BLOB NOT NULL,
                        leaf_index BIGINT NOT NULL,
                        seq BIGINT,
                        slot_created BIGINT NOT NULL,
                        spent BOOLEAN NOT NULL,
                        prev_spent BOOLEAN,
                        lamports REAL,
                        discriminator BLOB,
                        tree_type INTEGER,
                        nullified_in_tree BOOLEAN NOT NULL DEFAULT FALSE,
                        nullifier_queue_index BIGINT,
                        in_output_queue BOOLEAN NOT NULL DEFAULT TRUE,
                        queue BLOB,
                        nullifier BLOB,
                        tx_hash BLOB
                    );
                    "#,
                )
                .await?;

                log::info!("Copying data from temp table to new accounts table...");
                execute_sql(
                    manager,
                    r#"
                    INSERT INTO accounts (
                        hash, data, data_hash, address, onchain_pubkey, owner, tree,
                        leaf_index, seq, slot_created, spent, prev_spent, lamports,
                        tree_type, nullified_in_tree, nullifier_queue_index,
                        in_output_queue, queue, nullifier, tx_hash
                    )
                    SELECT
                        hash, data, data_hash, address, onchain_pubkey, owner, tree,
                        leaf_index, seq, slot_created, spent, prev_spent, lamports,
                        tree_type, nullified_in_tree, nullifier_queue_index,
                        in_output_queue, queue, nullifier, tx_hash
                    FROM accounts_discriminator_migration;
                    "#,
                )
                .await?;

                log::info!("Dropping temp table...");
                execute_sql(
                    manager,
                    "DROP TABLE IF EXISTS accounts_discriminator_migration;",
                )
                .await?;

                log::info!("Recreating indexes...");
                // Recreate ALL indexes that were dropped
                execute_sql(
                    manager,
                    "CREATE INDEX IF NOT EXISTS accounts_owner_idx ON accounts (owner) WHERE NOT spent;",
                )
                .await?;
                execute_sql(
                    manager,
                    "CREATE INDEX IF NOT EXISTS accounts_address_idx ON accounts (address) WHERE NOT spent AND address IS NOT NULL;",
                )
                .await?;
                execute_sql(
                    manager,
                    "CREATE INDEX IF NOT EXISTS accounts_onchain_pubkey_idx ON accounts (onchain_pubkey) WHERE NOT spent AND onchain_pubkey IS NOT NULL;",
                )
                .await?;
                execute_sql(
                    manager,
                    "CREATE INDEX IF NOT EXISTS accounts_address_spent_idx ON accounts (address, seq);",
                )
                .await?;
                execute_sql(
                    manager,
                    "CREATE UNIQUE INDEX IF NOT EXISTS accounts_owner_hash_idx ON accounts (spent, owner, hash);",
                )
                .await?;
                execute_sql(
                    manager,
                    "CREATE INDEX IF NOT EXISTS accounts_queue_idx ON accounts (tree, in_output_queue, leaf_index) WHERE in_output_queue = 1;",
                )
                .await?;
                execute_sql(
                    manager,
                    "CREATE INDEX IF NOT EXISTS idx_accounts_nullifier_queue_optimized ON accounts (nullifier_queue_index, tree, spent, in_output_queue) WHERE nullifier_queue_index IS NOT NULL;",
                )
                .await?;
                log::info!("Migration complete!");
            }
            sea_orm::DatabaseBackend::Postgres => {
                // For Postgres, change column type from Decimal to BYTEA
                // First, add a new column
                execute_sql(
                    manager,
                    "ALTER TABLE accounts ADD COLUMN discriminator_new BYTEA;",
                )
                .await?;

                // Convert existing Decimal discriminator to 8-byte little-endian BYTEA.
                // Use numeric arithmetic instead of bigint casts, because existing values
                // may exceed i64::MAX.
                let discriminator_backfill_sql = postgres_discriminator_backfill_sql();
                execute_sql(manager, &discriminator_backfill_sql).await?;

                // Drop old column and rename new one
                execute_sql(manager, "ALTER TABLE accounts DROP COLUMN discriminator;").await?;
                execute_sql(
                    manager,
                    "ALTER TABLE accounts RENAME COLUMN discriminator_new TO discriminator;",
                )
                .await?;
            }
            _ => {}
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // This is a destructive migration - we can't restore the original REAL values
        // with full precision. For down migration, we just change the column back.
        match manager.get_database_backend() {
            sea_orm::DatabaseBackend::Sqlite => {
                // Recreate table with REAL discriminator
                execute_sql(
                    manager,
                    r#"
                    CREATE TABLE accounts_discriminator_migration (
                        hash BLOB PRIMARY KEY NOT NULL,
                        data BLOB,
                        data_hash BLOB,
                        address BLOB,
                        onchain_pubkey BLOB,
                        owner BLOB NOT NULL,
                        tree BLOB NOT NULL,
                        leaf_index BIGINT NOT NULL,
                        seq BIGINT,
                        slot_created BIGINT NOT NULL,
                        spent BOOLEAN NOT NULL,
                        prev_spent BOOLEAN,
                        lamports REAL,
                        discriminator REAL,
                        tree_type INTEGER,
                        nullified_in_tree BOOLEAN NOT NULL DEFAULT FALSE,
                        nullifier_queue_index BIGINT,
                        in_output_queue BOOLEAN NOT NULL DEFAULT TRUE,
                        queue BLOB,
                        nullifier BLOB,
                        tx_hash BLOB
                    );
                    "#,
                )
                .await?;

                execute_sql(
                    manager,
                    r#"
                    INSERT INTO accounts_discriminator_migration (
                        hash, data, data_hash, address, onchain_pubkey, owner, tree,
                        leaf_index, seq, slot_created, spent, prev_spent, lamports,
                        tree_type, nullified_in_tree, nullifier_queue_index,
                        in_output_queue, queue, nullifier, tx_hash
                    )
                    SELECT
                        hash, data, data_hash, address, onchain_pubkey, owner, tree,
                        leaf_index, seq, slot_created, spent, prev_spent, lamports,
                        tree_type, nullified_in_tree, nullifier_queue_index,
                        in_output_queue, queue, nullifier, tx_hash
                    FROM accounts;
                    "#,
                )
                .await?;

                execute_sql(manager, "DROP TABLE IF EXISTS accounts;").await?;
                execute_sql(
                    manager,
                    "ALTER TABLE accounts_discriminator_migration RENAME TO accounts;",
                )
                .await?;
            }
            sea_orm::DatabaseBackend::Postgres => {
                execute_sql(
                    manager,
                    "ALTER TABLE accounts ADD COLUMN discriminator_old DECIMAL(23, 0);",
                )
                .await?;
                execute_sql(manager, "ALTER TABLE accounts DROP COLUMN discriminator;").await?;
                execute_sql(
                    manager,
                    "ALTER TABLE accounts RENAME COLUMN discriminator_old TO discriminator;",
                )
                .await?;
            }
            _ => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm_migration::sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};
    use serial_test::serial;

    fn local_postgres_test_url() -> Option<String> {
        let url = std::env::var("TEST_DATABASE_URL").ok()?;
        if url.contains("127.0.0.1") || url.contains("localhost") {
            Some(url)
        } else {
            None
        }
    }

    #[test]
    fn postgres_backfill_sql_does_not_use_bigint_cast() {
        let sql = postgres_discriminator_backfill_sql();
        assert!(!sql.contains("::bigint"));
    }

    #[tokio::test]
    #[serial]
    async fn postgres_backfill_expression_supports_values_above_i64_max() {
        let Some(database_url) = local_postgres_test_url() else {
            return;
        };

        let db = Database::connect(&database_url)
            .await
            .expect("failed to connect to TEST_DATABASE_URL");

        if db.get_database_backend() != DatabaseBackend::Postgres {
            return;
        }

        let value = 18_224_491_089_580_469_651u64;
        let hex_expr = postgres_numeric_to_little_endian_hex_expr(&format!("{value}::numeric"));
        let sql = format!("SELECT decode({hex_expr}, 'hex') AS converted");
        let rows = db
            .query_all(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await
            .expect("failed to evaluate conversion expression");

        assert_eq!(rows.len(), 1);
        let converted: Vec<u8> = rows[0]
            .try_get("", "converted")
            .expect("failed to parse converted bytes");
        assert_eq!(converted, value.to_le_bytes().to_vec());
    }
}
