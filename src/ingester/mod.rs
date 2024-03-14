use std::future::IntoFuture;
use std::thread::sleep;
use std::time::Duration;

use error::IngesterError;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use parser::parse_transaction;
use sea_orm::sea_query::OnConflict;
use sea_orm::ConnectionTrait;
use sea_orm::DatabaseConnection;
use sea_orm::DatabaseTransaction;
use sea_orm::DbErr;
use sea_orm::EntityTrait;
use sea_orm::QueryTrait;
use sea_orm::Set;
use sea_orm::TransactionTrait;
use sqlx::error::DatabaseError;
use typedefs::block_info::TransactionInfo;

use self::typedefs::block_info::BlockInfo;
use crate::dao::generated::blocks;
pub mod error;
pub mod fetchers;
pub mod parser;
pub mod persist;
pub mod typedefs;

pub async fn index_block(db: &DatabaseConnection, block: &BlockInfo) -> Result<(), IngesterError> {
    let txn = db.begin().await?;
    index_block_metadata(&db, block).await.unwrap();
    // for transaction in &block.transactions {
    //     index_transaction(&txn, &transaction, block.slot).await?;
    // }
    txn.commit().await.unwrap();

    Ok(())
}

async fn index_block_metadata(
    tx: &DatabaseConnection,
    block: &BlockInfo,
) -> Result<(), IngesterError> {
    let model = blocks::ActiveModel {
        slot: Set(block.slot as i64),
        parent_slot: Set(block.parent_slot as i64),
        block_time: Set(block.block_time as i64),
        blockhash: Set(block.blockhash.clone().into()),
        parent_blockhash: Set(block.parent_blockhash.clone().into()),
        block_height: Set(block.block_height as i64),
        ..Default::default()
    };

    // We first build the query and then execute it because SeaORM has a bug where it always throws
    // an error if we do not insert a record in an insert statement. However, in this case, it's
    // expected not to insert anything if the key already exists.
    let query = blocks::Entity::insert(model)
        .on_conflict(
            OnConflict::column(blocks::Column::Slot)
                .do_nothing()
                .to_owned(),
        )
        .build(tx.get_database_backend());
    tx.execute(query).await?;

    Ok(())
}

async fn index_transaction(
    txn: &DatabaseTransaction,
    transaction_info: &TransactionInfo,
    slot: u64,
) -> Result<(), IngesterError> {
    let event_bundles = parse_transaction(transaction_info, slot)?;

    for event_bundle in event_bundles {
        let result = persist::persist_bundle(txn, event_bundle).await.unwrap();
    }
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
