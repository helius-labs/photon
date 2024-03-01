use std::sync::Arc;

use error::IngesterError;
use futures::stream::Stream;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use parser::parse_transaction;
use sea_orm::DatabaseConnection;
use std::pin::Pin;
use transaction_info::TransactionInfo;
pub mod error;
pub mod parser;
pub mod persist;
pub mod transaction_info;

pub async fn index_transaction(
    db: &DatabaseConnection,
    txn: TransactionInfo,
) -> Result<(), IngesterError> {
    let event_bundles = parse_transaction(txn)?;
    let stream = futures::stream::iter(event_bundles);

    // We use a default parameter to avoid parameter input overload. High concurrency is likely
    // ineffective due to database locking since the events in a transaction likely affect the same tree.
    let max_concurrent_calls = 5;
    stream
        .map(|bundle| async { persist::persist_bundle(db, bundle).await })
        .buffer_unordered(max_concurrent_calls)
        .try_collect::<()>()
        .await
}

pub async fn index_transaction_stream(
    db: Arc<DatabaseConnection>,
    stream: Pin<Box<dyn Stream<Item = TransactionInfo>>>,
    max_concurrency: usize,
) {
    // Use `for_each_concurrent` to control the level of concurrency.
    stream
        .for_each_concurrent(max_concurrency, |sig| {
            let db_clone = db.clone();
            async move {
                if let Err(e) = index_transaction(&db_clone, sig).await {
                    log::error!("Failed to index transaction: {}", e);
                }
            }
        })
        .await;
}
