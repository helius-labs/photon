use std::thread::sleep;
use std::time::Duration;

use error::IngesterError;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use parser::parse_transaction;
use sea_orm::DatabaseConnection;
use sea_orm::DatabaseTransaction;
use sea_orm::TransactionTrait;
use typedefs::block_info::TransactionInfo;

use self::typedefs::block_info::BlockInfo;
pub mod error;
pub mod fetchers;
pub mod parser;
pub mod persist;
pub mod typedefs;

async fn index_transaction(
    txn: &DatabaseTransaction,
    transaction_info: &TransactionInfo,
    slot: u64,
) -> Result<(), IngesterError> {
    let event_bundles = parse_transaction(transaction_info, slot)?;
    let stream = futures::stream::iter(event_bundles);

    // We use a default parameter to avoid parameter input overload. High concurrency is likely
    // ineffective due to database locking since the events in a transaction likely affect the same tree.
    let max_concurrent_calls = 5;
    stream
        .map(|bundle| async { persist::persist_bundle(txn, bundle).await })
        .buffer_unordered(max_concurrent_calls)
        .try_collect::<()>()
        .await
}

pub async fn index_block(db: &DatabaseConnection, block: &BlockInfo) -> Result<(), IngesterError> {
    let txn = db.begin().await?;
    for transaction in &block.transactions {
        index_transaction(&txn, &transaction, block.slot).await?;
    }
    txn.commit().await?;
    Ok(())
}

pub async fn index_block_batch(db: &DatabaseConnection, block_batch: Vec<BlockInfo>) {
    for block in block_batch {
        index_block_with_infinite_retries(db, &block).await;
    }
}

async fn index_block_with_infinite_retries(db: &DatabaseConnection, block: &BlockInfo) {
    loop {
        match index_block(db, &block).await {
            Ok(()) => return (),
            Err(e) => {
                log::error!("Failed to index block: {}", e);
                sleep(Duration::from_secs(1));
            }
        }
    }
}
