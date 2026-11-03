use cadence_macros::statsd_count;
use error::IngesterError;

use parser::parse_transaction;
use parser::TreeResolver;
use sea_orm::sea_query::OnConflict;
use sea_orm::ConnectionTrait;
use sea_orm::DatabaseConnection;
use sea_orm::DatabaseTransaction;

use sea_orm::EntityTrait;
use sea_orm::QueryTrait;
use sea_orm::Set;
use sea_orm::TransactionTrait;
use sea_orm::{ColumnTrait, QueryFilter, QueryOrder};
use solana_client::nonblocking::rpc_client::RpcClient;

use self::parser::state_update::StateUpdate;
use self::persist::persist_state_update;
use self::persist::MAX_SQL_INSERTS;
use self::typedefs::block_info::BlockInfo;
use self::typedefs::block_info::BlockMetadata;
use crate::dao::generated::blocks;
use crate::metric;
pub mod error;
pub mod fetchers;
pub mod indexer;
pub mod parser;
pub mod persist;
pub mod retry;
pub mod startup_cleanup;
pub mod supervisor;
pub mod typedefs;

async fn derive_block_state_update(
    conn: &DatabaseConnection,
    block: &BlockInfo,
    resolver: &mut TreeResolver<'_>,
) -> Result<StateUpdate, IngesterError> {
    let mut state_updates: Vec<StateUpdate> = Vec::new();
    for transaction in &block.transactions {
        state_updates
            .push(parse_transaction(conn, transaction, block.metadata.slot, resolver).await?);
    }
    let update = StateUpdate::merge_updates(state_updates);
    let trees: Vec<_> = update.tree_pubkeys().into_iter().collect();
    if !trees.is_empty() {
        let known = parser::tree_info::TreeInfo::get_tree_info_batch(conn, &trees).await?;
        for tree in trees {
            if !known.contains_key(&tree) {
                resolver
                    .discover_tree(conn, &tree, block.metadata.slot)
                    .await?;
            }
        }
    }
    Ok(update)
}

pub async fn index_block(
    db: &DatabaseConnection,
    block: &BlockInfo,
    rpc_client: &RpcClient,
) -> Result<(), IngesterError> {
    let mut resolver = TreeResolver::new(rpc_client);
    let state_update = derive_block_state_update(db, block, &mut resolver).await?;
    let txn = db.begin().await?;
    index_block_metadatas(&txn, vec![&block.metadata]).await?;
    persist_state_update(&txn, state_update).await?;
    txn.commit().await?;
    Ok(())
}

async fn index_block_metadatas(
    tx: &DatabaseTransaction,
    blocks: Vec<&BlockMetadata>,
) -> Result<u64, IngesterError> {
    let mut inserted = 0;
    for block_chunk in blocks.chunks(MAX_SQL_INSERTS) {
        let block_models: Vec<blocks::ActiveModel> = block_chunk
            .iter()
            .map(|block| {
                Ok::<blocks::ActiveModel, IngesterError>(blocks::ActiveModel {
                    slot: Set(checked_i64(block.slot)?),
                    parent_slot: Set(checked_i64(block.parent_slot)?),
                    block_time: Set(block.block_time),
                    blockhash: Set(block.blockhash.clone().into()),
                    parent_blockhash: Set(block.parent_blockhash.clone().into()),
                    block_height: Set(checked_i64(block.block_height)?),
                })
            })
            .collect::<Result<Vec<blocks::ActiveModel>, IngesterError>>()?;

        // We first build the query and then execute it because SeaORM has a bug where it always throws
        // expected not to insert anything if the key already exists.
        let query = blocks::Entity::insert_many(block_models)
            .on_conflict(
                OnConflict::column(blocks::Column::Slot)
                    .do_nothing()
                    .to_owned(),
            )
            .build(tx.get_database_backend());
        inserted += tx.execute(query).await?.rows_affected();
    }
    Ok(inserted)
}

fn checked_i64(value: u64) -> Result<i64, IngesterError> {
    i64::try_from(value).map_err(|_| {
        IngesterError::ParserError(format!("Block metadata value {} exceeds i64", value))
    })
}

pub async fn index_block_batch(
    db: &DatabaseConnection,
    block_batch: &Vec<BlockInfo>,
    rpc_client: &RpcClient,
) -> Result<(), IngesterError> {
    if block_batch.is_empty() {
        return Ok(());
    }
    for pair in block_batch.windows(2) {
        if pair[1].metadata.parent_slot != pair[0].metadata.slot
            || pair[1].metadata.parent_blockhash != pair[0].metadata.blockhash
        {
            return Err(IngesterError::ParserError(
                "Block batch contains a parent slot/hash mismatch".into(),
            ));
        }
    }
    // A commit can succeed even when its acknowledgement is lost. Skip only
    // blocks whose durable hash matches, so retrying cannot apply their effects
    // a second time or silently accept a different fork at the same slot.
    let slots = block_batch
        .iter()
        .map(|block| checked_i64(block.metadata.slot))
        .collect::<Result<Vec<_>, _>>()?;
    let existing = blocks::Entity::find()
        .filter(blocks::Column::Slot.is_in(slots))
        .all(db)
        .await?;
    let mut pending = Vec::new();
    for block in block_batch {
        if let Some(saved) = existing
            .iter()
            .find(|saved| saved.slot == block.metadata.slot as i64)
        {
            if saved.blockhash != block.metadata.blockhash.to_vec() {
                return Err(IngesterError::ParserError(format!("Committed block hash differs at slot {}; canonical-chain reconciliation required", block.metadata.slot)));
            }
        } else {
            pending.push(block);
        }
    }
    if pending.is_empty() {
        return Ok(());
    }
    let blocks_len = pending.len();
    let mut state_updates = Vec::new();
    let mut resolver = TreeResolver::new(rpc_client);
    for block in &pending {
        state_updates.push(derive_block_state_update(db, block, &mut resolver).await?);
    }
    let tx = db.begin().await?;
    if let Some(previous) = blocks::Entity::find()
        .order_by_desc(blocks::Column::Slot)
        .one(&tx)
        .await?
    {
        let first = &pending[0].metadata;
        if previous.slot >= checked_i64(first.slot)? {
            return Err(IngesterError::DatabaseError(
                "Checkpoint advanced while preparing the batch; reload and retry".into(),
            ));
        }
        if first.parent_slot != previous.slot as u64
            || first.parent_blockhash.to_vec() != previous.blockhash
        {
            return Err(IngesterError::ParserError(format!("Block {} does not extend committed checkpoint {}; canonical-chain reconciliation required", first.slot, previous.slot)));
        }
    }
    let block_metadatas: Vec<&BlockMetadata> = pending.iter().map(|b| &b.metadata).collect();
    // The unique slot inserts also claim this batch. If a prior commit becomes
    // visible while waiting for an insert conflict, rollback and re-derive only
    // the still-uncommitted blocks before applying any state changes.
    let inserted = index_block_metadatas(&tx, block_metadatas).await?;
    if inserted != pending.len() as u64 {
        return Err(IngesterError::DatabaseError(
            "Block batch was concurrently committed; reload and retry".into(),
        ));
    }
    persist::persist_state_update(&tx, StateUpdate::merge_updates(state_updates)).await?;
    tx.commit().await?;
    metric! {
        statsd_count!("blocks_indexed", blocks_len as i64);
    }
    Ok(())
}

pub async fn index_block_batch_with_infinite_retries(
    db: &DatabaseConnection,
    block_batch: Vec<BlockInfo>,
    rpc_client: &RpcClient,
) -> Result<(), IngesterError> {
    if block_batch.is_empty() {
        return Ok(());
    }
    let mut backoff = retry::Backoff::default();
    loop {
        match index_block_batch(db, &block_batch, rpc_client).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let start_block = block_batch[0].metadata.slot;
                let end_block = block_batch[block_batch.len() - 1].metadata.slot;
                log::error!(
                    "Failed to index block batch {}-{}. Got error {}",
                    start_block,
                    end_block,
                    e
                );
                metric! { statsd_count!("block_batch_index_failures", 1); }
                if !e.is_retryable() {
                    return Err(e);
                }
                let delay = backoff.next_delay();
                log::warn!(
                    "Retrying block batch after {:?}, attempt {}",
                    delay,
                    backoff.attempts()
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}
