use light_compressed_account::QueueType;
use log::debug;
use sea_orm::{ColumnTrait, ConnectionTrait, DbErr, EntityTrait, QueryFilter, Set};
use solana_pubkey::Pubkey;

use crate::dao::generated::{prelude::QueueHashChains, queue_hash_chains};

pub struct CachedHashChain {
    pub zkp_batch_index: i32,
    pub hash_chain: [u8; 32],
}

/// Store multiple hash chains in a single transaction
pub async fn store_hash_chains_batch<C>(
    db: &C,
    tree_pubkey: Pubkey,
    queue_type: QueueType,
    batch_start_index: u64,
    hash_chains: Vec<(usize, u64, [u8; 32])>, // (zkp_batch_index, start_offset, hash_chain)
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    if hash_chains.is_empty() {
        return Ok(());
    }

    let queue_type_int = queue_type as i32;
    let tree_bytes = tree_pubkey.to_bytes().to_vec();
    let chains_count = hash_chains.len();

    let models: Vec<queue_hash_chains::ActiveModel> = hash_chains
        .into_iter()
        .map(
            |(zkp_batch_idx, start_offset, hash_chain)| queue_hash_chains::ActiveModel {
                tree_pubkey: Set(tree_bytes.clone()),
                queue_type: Set(queue_type_int),
                batch_start_index: Set(batch_start_index as i64),
                zkp_batch_index: Set(zkp_batch_idx as i32),
                start_offset: Set(start_offset as i64),
                hash_chain: Set(hash_chain.to_vec()),
            },
        )
        .collect();

    queue_hash_chains::Entity::insert_many(models)
        .on_conflict(
            sea_orm::sea_query::OnConflict::columns([
                queue_hash_chains::Column::TreePubkey,
                queue_hash_chains::Column::QueueType,
                queue_hash_chains::Column::BatchStartIndex,
                queue_hash_chains::Column::ZkpBatchIndex,
            ])
            .do_nothing()
            .to_owned(),
        )
        .exec(db)
        .await?;

    debug!(
        "Processed {} hash chains for tree={}, queue_type={:?}, batch_start={}",
        chains_count, tree_pubkey, queue_type, batch_start_index
    );

    Ok(())
}

/// Retrieve cached hash chains for a specific tree and queue type
pub async fn get_cached_hash_chains<C>(
    db: &C,
    tree_pubkey: Pubkey,
    queue_type: QueueType,
    batch_start_index: u64,
) -> Result<Vec<CachedHashChain>, DbErr>
where
    C: ConnectionTrait,
{
    let queue_type_int = queue_type as i32;

    let results = QueueHashChains::find()
        .filter(queue_hash_chains::Column::TreePubkey.eq(tree_pubkey.to_bytes().to_vec()))
        .filter(queue_hash_chains::Column::QueueType.eq(queue_type_int))
        .filter(queue_hash_chains::Column::BatchStartIndex.eq(batch_start_index as i64))
        .all(db)
        .await?;

    let mut chains = Vec::with_capacity(results.len());
    for result in results {
        if result.hash_chain.len() == 32 {
            let mut hash_array = [0u8; 32];
            hash_array.copy_from_slice(&result.hash_chain);

            chains.push(CachedHashChain {
                zkp_batch_index: result.zkp_batch_index,
                hash_chain: hash_array,
            });
        }
    }

    chains.sort_by_key(|c| c.zkp_batch_index);
    Ok(chains)
}
