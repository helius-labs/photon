use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use solana_program::pubkey::Pubkey;
use std::str::FromStr;

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
        let solayer_accounts = vec![
            "ARDPkhymCbfdan375FCgPnBJQvUfHeb7nHVdBfwWSxrp",
            "2sYfW81EENCMe415CPhE2XzBA5iQf4TXRs31W1KP63YT",
        ];
        // Encode the accounts as hex strings
        let encoded_accounts = solayer_accounts
            .iter()
            .map(|account| {
                let pubkey = Pubkey::from_str(account).unwrap();
                format!("\\x{}", hex::encode(pubkey.to_bytes()))
            })
            .collect::<Vec<String>>()
            .join("', '");

        if manager.get_database_backend() == DatabaseBackend::Postgres {
            // Create index concurrently for Postgres
            execute_sql(
                manager,
                &format!(
                    "CREATE INDEX CONCURRENTLY IF NOT EXISTS solayer_account_index2 ON accounts (spent, owner, substring(data, 1, 32)) \
                    WHERE owner IN ('{}');",
                    encoded_accounts
                ),
            )
            .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        if manager.get_database_backend() == DatabaseBackend::Postgres {
            manager
                .drop_index(
                    Index::drop()
                        .name("solayer_account_index2")
                        .table(Accounts::Table)
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }
}
