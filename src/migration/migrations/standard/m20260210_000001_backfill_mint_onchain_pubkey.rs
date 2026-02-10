use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
};

use crate::common::token_layout::{
    COMPRESSED_MINT_PDA_LEN, COMPRESSED_MINT_PDA_OFFSET_SQL, TOKEN_ACCOUNT_TYPE_OFFSET,
    TOKEN_ACCOUNT_TYPE_OFFSET_SQL,
};

#[derive(DeriveMigrationName)]
pub struct Migration;

/// Raw 32-byte pubkey for cTokenmWW8bLPjZEBAUgYy3zKxQZW6VKi7bqNFEVv3m
const LIGHT_TOKEN_PROGRAM_ID_HEX: &str =
    "0915a35723794e8fb65d075b6b72699c38dd02e5948b75b0e5a0418e80975b44";

fn backfill_mint_onchain_pubkey_sql(backend: DatabaseBackend) -> Result<String, DbErr> {
    match backend {
        DatabaseBackend::Postgres => Ok(format!(
            "UPDATE accounts \
             SET onchain_pubkey = substring(data from {COMPRESSED_MINT_PDA_OFFSET_SQL} for {COMPRESSED_MINT_PDA_LEN}) \
             WHERE onchain_pubkey IS NULL \
               AND spent = false \
               AND data IS NOT NULL \
               AND length(data) > {TOKEN_ACCOUNT_TYPE_OFFSET} \
               AND substring(data from {TOKEN_ACCOUNT_TYPE_OFFSET_SQL} for 1) = E'\\\\x01' \
               AND owner = E'\\\\x{LIGHT_TOKEN_PROGRAM_ID_HEX}'"
        )),
        DatabaseBackend::Sqlite => Ok(format!(
            "UPDATE accounts \
             SET onchain_pubkey = substr(data, {COMPRESSED_MINT_PDA_OFFSET_SQL}, {COMPRESSED_MINT_PDA_LEN}) \
             WHERE onchain_pubkey IS NULL \
               AND spent = 0 \
               AND data IS NOT NULL \
               AND length(data) > {TOKEN_ACCOUNT_TYPE_OFFSET} \
               AND substr(data, {TOKEN_ACCOUNT_TYPE_OFFSET_SQL}, 1) = X'01' \
               AND owner = X'{LIGHT_TOKEN_PROGRAM_ID_HEX}'"
        )),
        _ => Err(DbErr::Custom("Unsupported database backend".to_string())),
    }
}

fn clear_backfilled_mint_onchain_pubkey_sql(backend: DatabaseBackend) -> Result<String, DbErr> {
    match backend {
        DatabaseBackend::Postgres => Ok(format!(
            "UPDATE accounts \
             SET onchain_pubkey = NULL \
             WHERE onchain_pubkey IS NOT NULL \
               AND spent = false \
               AND data IS NOT NULL \
               AND length(data) > {TOKEN_ACCOUNT_TYPE_OFFSET} \
               AND substring(data from {TOKEN_ACCOUNT_TYPE_OFFSET_SQL} for 1) = E'\\\\x01' \
               AND owner = E'\\\\x{LIGHT_TOKEN_PROGRAM_ID_HEX}' \
               AND onchain_pubkey = substring(data from {COMPRESSED_MINT_PDA_OFFSET_SQL} for {COMPRESSED_MINT_PDA_LEN})"
        )),
        DatabaseBackend::Sqlite => Ok(format!(
            "UPDATE accounts \
             SET onchain_pubkey = NULL \
             WHERE onchain_pubkey IS NOT NULL \
               AND spent = 0 \
               AND data IS NOT NULL \
               AND length(data) > {TOKEN_ACCOUNT_TYPE_OFFSET} \
               AND substr(data, {TOKEN_ACCOUNT_TYPE_OFFSET_SQL}, 1) = X'01' \
               AND owner = X'{LIGHT_TOKEN_PROGRAM_ID_HEX}' \
               AND onchain_pubkey = substr(data, {COMPRESSED_MINT_PDA_OFFSET_SQL}, {COMPRESSED_MINT_PDA_LEN})"
        )),
        _ => Err(DbErr::Custom("Unsupported database backend".to_string())),
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();

        // Backfill onchain_pubkey for compressed mint accounts.
        // Compressed mints have:
        //   - owner = LIGHT_TOKEN_PROGRAM_ID
        //   - data length > 165 bytes
        //   - account_type byte at offset 165 (0-indexed) == 0x01 (Mint)
        // The mint PDA is at data bytes [84..116] (0-indexed), which is
        // substring starting at byte 85 (1-indexed) for 32 bytes.
        let sql = backfill_mint_onchain_pubkey_sql(backend)?;

        manager
            .get_connection()
            .execute(Statement::from_string(backend, sql))
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();

        // Clear only values this migration backfilled, preserving independently-updated rows.
        let sql = clear_backfilled_mint_onchain_pubkey_sql(backend)?;

        manager
            .get_connection()
            .execute(Statement::from_string(backend, sql))
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backfill_sql_uses_shared_offsets() {
        let pg_sql = backfill_mint_onchain_pubkey_sql(DatabaseBackend::Postgres).unwrap();
        assert!(pg_sql.contains(&format!(
            "substring(data from {COMPRESSED_MINT_PDA_OFFSET_SQL} for {COMPRESSED_MINT_PDA_LEN})"
        )));
        assert!(pg_sql.contains(&format!("length(data) > {TOKEN_ACCOUNT_TYPE_OFFSET}")));
        assert!(pg_sql.contains(&format!(
            "substring(data from {TOKEN_ACCOUNT_TYPE_OFFSET_SQL} for 1)"
        )));
    }

    #[test]
    fn test_down_sql_only_clears_backfilled_values() {
        let sqlite_sql = clear_backfilled_mint_onchain_pubkey_sql(DatabaseBackend::Sqlite).unwrap();
        assert!(sqlite_sql.contains("onchain_pubkey = substr(data,"));
        assert!(sqlite_sql.contains("SET onchain_pubkey = NULL"));
    }
}
