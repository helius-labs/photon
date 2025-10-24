#[allow(clippy::module_inception)]
pub mod api;
pub mod error;
pub mod method;
pub mod rpc_server;

use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, Statement};

/// Helper function to set transaction isolation level for Postgres.
/// In test environments, this can be disabled by setting PHOTON_SKIP_ISOLATION_LEVEL=true
/// to avoid read-after-write visibility issues in tests.
pub async fn set_transaction_isolation_if_needed(
    tx: &DatabaseTransaction,
) -> Result<(), sea_orm::DbErr> {
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        // Skip isolation level setting in test environments
        if std::env::var("PHOTON_SKIP_ISOLATION_LEVEL").is_ok() {
            return Ok(());
        }

        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }
    Ok(())
}
