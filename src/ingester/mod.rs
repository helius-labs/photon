use std::thread::sleep;
use std::time::Duration;

use cadence_macros::statsd_count;
use error::IngesterError;

use parser::parse_transaction;
use sea_orm::sea_query::OnConflict;
use sea_orm::DatabaseConnection;
use sea_orm::DatabaseTransaction;
use sea_orm::{ConnectionTrait, QueryTrait};

use self::parser::state_update::StateUpdate;
use self::persist::persist_state_update;
use self::persist::MAX_SQL_INSERTS;
use self::typedefs::block_info::BlockInfo;
use self::typedefs::block_info::BlockMetadata;
use crate::dao::generated::blocks;
use crate::ingester::gap::{RewindController, StateUpdateSequences};
use crate::metric;
use sea_orm::EntityTrait;
use sea_orm::Set;
use sea_orm::TransactionTrait;
pub mod error;
pub mod fetchers;
pub mod gap;
pub mod indexer;
pub mod parser;
pub mod persist;
pub mod typedefs;

fn derive_block_state_update(
    block: &BlockInfo,
    rewind_controller: Option<&RewindController>,
    tree_filter: Option<solana_pubkey::Pubkey>,
) -> Result<StateUpdate, IngesterError> {
    let mut state_updates: Vec<StateUpdate> = Vec::new();
    let mut sequences = StateUpdateSequences::default();

    // Parse each transaction and extract sequences with proper context
    for transaction in &block.transactions {
        let state_update = parse_transaction(transaction, block.metadata.slot, tree_filter)?;

        // Extract sequences with proper slot and signature context
        sequences.extract_state_update_sequences(
            &state_update,
            block.metadata.slot,
            &transaction.signature.to_string(),
        );

        state_updates.push(state_update);
    }

    // Check for gaps with proper context
    let gaps = sequences.detect_all_sequence_gaps();
    if !gaps.is_empty() {
        tracing::warn!(
            "Gaps detected in block {} sequences: {gaps:?}",
            block.metadata.slot
        );

        // Request rewind if controller is available
        if let Some(controller) = rewind_controller {
            if let Err(e) = controller.request_rewind_for_gaps(&gaps) {
                tracing::error!(
                    "Failed to request rewind for gaps in block {}: {}",
                    block.metadata.slot,
                    e
                );
                return Err(IngesterError::CustomError(
                    "Gap detection triggered rewind failure".to_string(),
                ));
            }
            // Return early after requesting rewind - don't continue processing
            return Err(IngesterError::CustomError(
                "Gap detection triggered rewind".to_string(),
            ));
        }
    }

    // Update sequence state with latest observed sequences
    sequences.update_sequence_state();

    Ok(StateUpdate::merge_updates(state_updates))
}

pub async fn index_block(db: &DatabaseConnection, block: &BlockInfo) -> Result<(), IngesterError> {
    let txn = db.begin().await?;
    index_block_metadatas(&txn, vec![&block.metadata]).await?;
    derive_block_state_update(block, None, None)?;
    persist_state_update(&txn, derive_block_state_update(block, None, None)?).await?;
    txn.commit().await?;
    Ok(())
}

async fn index_block_metadatas(
    tx: &DatabaseTransaction,
    blocks: Vec<&BlockMetadata>,
) -> Result<(), IngesterError> {
    for block_chunk in blocks.chunks(MAX_SQL_INSERTS) {
        let block_models: Vec<blocks::ActiveModel> = block_chunk
            .iter()
            .map(|block| {
                Ok::<blocks::ActiveModel, IngesterError>(blocks::ActiveModel {
                    slot: Set(block.slot as i64),
                    parent_slot: Set(block.parent_slot as i64),
                    block_time: Set(block.block_time),
                    blockhash: Set(block.blockhash.clone().into()),
                    parent_blockhash: Set(block.parent_blockhash.clone().into()),
                    block_height: Set(block.block_height as i64),
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
        tx.execute(query).await?;
    }
    Ok(())
}

fn block_contains_tree(block: &BlockInfo, tree_filter: &solana_pubkey::Pubkey) -> bool {
    for tx in &block.transactions {
        for instruction_group in &tx.instruction_groups {
            if instruction_group
                .outer_instruction
                .accounts
                .contains(tree_filter)
            {
                return true;
            }
        }
    }
    false
}

pub async fn index_block_batch(
    db: &DatabaseConnection,
    block_batch: &Vec<BlockInfo>,
    rewind_controller: Option<&RewindController>,
    tree_filter: Option<solana_pubkey::Pubkey>,
) -> Result<(), IngesterError> {
    // Pre-filter blocks if tree filter is specified
    let filtered_blocks: Vec<&BlockInfo> = if let Some(ref tree) = tree_filter {
        block_batch
            .iter()
            .filter(|block| block_contains_tree(block, tree))
            .collect()
    } else {
        block_batch.iter().collect()
    };

    if filtered_blocks.is_empty() {
        // Skip empty batches
        metric! {
            statsd_count!("blocks_skipped", block_batch.len() as i64);
        }
        return Ok(());
    }

    let blocks_len = filtered_blocks.len();
    let tx = db.begin().await?;
    let block_metadatas: Vec<&BlockMetadata> =
        filtered_blocks.iter().map(|b| &b.metadata).collect();
    index_block_metadatas(&tx, block_metadatas).await?;
    let mut state_updates = Vec::new();
    for block in filtered_blocks {
        state_updates.push(derive_block_state_update(
            block,
            rewind_controller,
            tree_filter,
        )?);
    }
    persist::persist_state_update(&tx, StateUpdate::merge_updates(state_updates)).await?;
    metric! {
        statsd_count!("blocks_indexed", blocks_len as i64);
        statsd_count!("blocks_skipped", (block_batch.len() - blocks_len) as i64);
    }
    log::info!(
        "Indexed {} blocks, skipped {} blocks",
        blocks_len,
        block_batch.len() - blocks_len
    );
    tx.commit().await?;
    Ok(())
}

pub async fn index_block_batch_with_infinite_retries(
    db: &DatabaseConnection,
    block_batch: Vec<BlockInfo>,
    rewind_controller: Option<&RewindController>,
    tree_filter: Option<solana_pubkey::Pubkey>,
) -> Result<(), IngesterError> {
    loop {
        match index_block_batch(db, &block_batch, rewind_controller, tree_filter).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                // Check if this is a gap-triggered rewind error
                if e.to_string().contains("Gap detection triggered rewind") {
                    // Don't retry, propagate the rewind error up
                    return Err(e);
                }

                let start_block = block_batch.first().unwrap().metadata.slot;
                let end_block = block_batch.last().unwrap().metadata.slot;
                log::error!(
                    "Failed to index block batch {}-{}. Got error {}",
                    start_block,
                    end_block,
                    e
                );
                sleep(Duration::from_secs(1));
            }
        }
    }
}
