use std::sync::Arc;

use error::IngesterError;
use futures::stream::Stream;
use futures::stream::StreamExt;
use parser::parse_transaction;
use sea_orm::DatabaseConnection;
use solana_sdk::clock::Slot;
use solana_sdk::clock::UnixTimestamp;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_transaction_status::UiTransactionStatusMeta;
use solana_transaction_status::VersionedTransactionWithStatusMeta;
use std::pin::Pin;
pub mod error;
pub mod parser;
pub mod persist;

#[derive(Clone, Debug, PartialEq)]
pub struct VersionedTransactionWithUiStatusMeta {
    pub transaction: VersionedTransaction,
    pub meta: UiTransactionStatusMeta,
}

// We create this struct instead of using VersionedConfirmedTransactionWithStatusMeta because
// event though UiStatusMeta is pracitcally identical to StatusMeta, it has better deserialization support.
pub struct VersionedConfirmedTransactionWithUiStatusMeta {
    pub slot: Slot,
    pub tx_with_meta: VersionedTransactionWithUiStatusMeta,
    pub block_time: Option<UnixTimestamp>,
}

pub async fn index_transaction(
    db: &DatabaseConnection,
    txn: VersionedConfirmedTransactionWithUiStatusMeta,
) -> Result<(), IngesterError> {
    let event_bundle = parse_transaction(txn)?;
    persist::persist_bundle(db, event_bundle).await
}

// TODO: API here is work in progress. Subject to removal.
pub async fn index_transaction_stream(
    db: Arc<DatabaseConnection>,
    stream: Pin<Box<dyn Stream<Item = VersionedConfirmedTransactionWithUiStatusMeta> + Send>>,
    max_concurrency: usize,
) {
    // Use `for_each_concurrent` to control the level of concurrency.
    stream
        .for_each_concurrent(max_concurrency, |sig| {
            let db_clone = db.clone();
            async move {
                index_transaction(&db_clone, sig).await;
            }
        })
        .await;
}
