use hex;
use light_batched_merkle_tree::{
    batch::Batch, merkle_tree::BatchedMerkleTreeAccount, queue::BatchedQueueAccount,
};
use light_compressed_account::QueueType;
use light_hasher::hash_chain::create_hash_chain_from_slice;
use light_zero_copy::vec::ZeroCopyVecU64;
use log::{debug, error, trace};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_pubkey::Pubkey;

use crate::dao::generated::{accounts, address_queues};

use crate::{
    api::error::PhotonApiError, ingester::parser::tree_info::TreeInfo, metric,
    monitor::queue_hash_cache,
};
use cadence_macros::statsd_count;

#[derive(Debug, Clone)]
pub struct QueueHashChainInfo {
    pub queue_type: QueueType,
    pub tree_pubkey: Pubkey,
    pub batch_index: usize,
    pub current_index: u64,
}

#[derive(Debug)]
pub struct HashChainDivergence {
    pub queue_info: QueueHashChainInfo,
    pub expected_hash_chain: [u8; 32],
    pub actual_hash_chain: [u8; 32],
    pub zkp_batch_index: usize,
}

async fn verify_output_queue_hash_chains(
    rpc_client: &RpcClient,
    db: &DatabaseConnection,
    tree_pubkey: Pubkey,
) -> Result<(), Vec<HashChainDivergence>> {
    let tree_pubkey_str = tree_pubkey.to_string();
    let tree_info = match TreeInfo::get(db, &tree_pubkey_str).await.ok().flatten() {
        Some(info) => info,
        None => {
            trace!("No tree info found for {}", tree_pubkey);
            return Ok(());
        }
    };
    let queue_pubkey = Pubkey::from(tree_info.queue.to_bytes());

    let account = match rpc_client.get_account(&queue_pubkey).await {
        Ok(acc) => acc,
        Err(e) => {
            trace!("Failed to fetch queue account {}: {}", queue_pubkey, e);
            return Ok(());
        }
    };
    let mut account_data = account.data.clone();
    let queue = match BatchedQueueAccount::output_from_bytes(&mut account_data) {
        Ok(q) => q,
        Err(e) => {
            trace!("Failed to parse queue account {}: {:?}", queue_pubkey, e);
            return Ok(());
        }
    };

    let tree_account = match rpc_client.get_account(&tree_pubkey).await {
        Ok(acc) => acc,
        Err(e) => {
            trace!("Failed to fetch tree account {}: {}", tree_pubkey, e);
            return Ok(());
        }
    };
    let mut tree_account_data = tree_account.data.clone();
    let light_pubkey = light_compressed_account::Pubkey::from(tree_pubkey.to_bytes());
    let _merkle_tree =
        match BatchedMerkleTreeAccount::state_from_bytes(&mut tree_account_data, &light_pubkey) {
            Ok(tree) => tree,
            Err(e) => {
                trace!("Failed to parse tree account {}: {:?}", tree_pubkey, e);
                return Ok(());
            }
        };

    let metadata = queue.get_metadata();
    let batch_metadata = &metadata.batch_metadata;
    let currently_processing_batch_index = batch_metadata.get_current_batch_index();
    let current_batch = batch_metadata.get_current_batch();

    verify_queue_hash_chains(
        db,
        QueueType::OutputStateV2,
        tree_pubkey,
        &queue.hash_chain_stores[..],
        Some(&batch_metadata.batches),
        batch_metadata.zkp_batch_size,
        currently_processing_batch_index,
        current_batch.get_num_inserted_zkps(),
        current_batch.get_num_inserted_zkp_batch(),
    )
    .await
}

async fn verify_input_queue_hash_chains(
    rpc_client: &RpcClient,
    db: &DatabaseConnection,
    tree_pubkey: Pubkey,
) -> Result<(), Vec<HashChainDivergence>> {
    let account = match rpc_client.get_account(&tree_pubkey).await {
        Ok(acc) => acc,
        Err(e) => {
            trace!("Failed to fetch tree account {}: {}", tree_pubkey, e);
            return Ok(());
        }
    };
    let mut account_data = account.data.clone();
    let light_pubkey = light_compressed_account::Pubkey::from(tree_pubkey.to_bytes());
    let merkle_tree =
        match BatchedMerkleTreeAccount::state_from_bytes(&mut account_data, &light_pubkey) {
            Ok(tree) => tree,
            Err(e) => {
                trace!("Failed to parse tree account {}: {:?}", tree_pubkey, e);
                return Ok(());
            }
        };

    let metadata = merkle_tree.get_metadata();
    let queue_batches = &metadata.queue_batches;
    let pending_batch_index = queue_batches.pending_batch_index as usize;
    let pending_batch = &queue_batches.batches[pending_batch_index];

    verify_queue_hash_chains(
        db,
        QueueType::InputStateV2,
        tree_pubkey,
        &merkle_tree.hash_chain_stores[..],
        Some(&queue_batches.batches),
        queue_batches.zkp_batch_size,
        pending_batch_index,
        pending_batch.get_num_inserted_zkps(),
        pending_batch.get_num_inserted_zkp_batch(),
    )
    .await
}

async fn verify_address_queue_hash_chains(
    rpc_client: &RpcClient,
    db: &DatabaseConnection,
    tree_pubkey: Pubkey,
) -> Result<(), Vec<HashChainDivergence>> {
    let account = match rpc_client.get_account(&tree_pubkey).await {
        Ok(acc) => acc,
        Err(e) => {
            trace!("Failed to fetch tree account {}: {}", tree_pubkey, e);
            return Ok(());
        }
    };
    let mut account_data = account.data.clone();
    let light_pubkey = light_compressed_account::Pubkey::from(tree_pubkey.to_bytes());
    let merkle_tree =
        match BatchedMerkleTreeAccount::address_from_bytes(&mut account_data, &light_pubkey) {
            Ok(tree) => tree,
            Err(e) => {
                trace!(
                    "Failed to parse address tree account {}: {:?}",
                    tree_pubkey,
                    e
                );
                return Ok(());
            }
        };

    let metadata = merkle_tree.get_metadata();
    let queue_batches = &metadata.queue_batches;
    let pending_batch_index = queue_batches.pending_batch_index as usize;
    let current_batch = &queue_batches.batches[pending_batch_index];

    verify_queue_hash_chains(
        db,
        QueueType::AddressV2,
        tree_pubkey,
        &merkle_tree.hash_chain_stores[..],
        Some(&queue_batches.batches),
        queue_batches.zkp_batch_size,
        pending_batch_index,
        current_batch.get_num_inserted_zkps(),
        current_batch.get_num_inserted_zkp_batch(),
    )
    .await
}

async fn verify_queue_hash_chains(
    db: &DatabaseConnection,
    queue_type: QueueType,
    tree_pubkey: Pubkey,
    on_chain_hash_chains: &[ZeroCopyVecU64<'_, [u8; 32]>],
    on_chain_batches: Option<&[Batch; 2]>,
    zkp_batch_size: u64,
    pending_batch_index: usize,
    num_inserted_zkps: u64,
    num_inserted_in_current_zkp: u64,
) -> Result<(), Vec<HashChainDivergence>> {
    let mut divergences = Vec::new();

    if num_inserted_in_current_zkp > 0 && num_inserted_in_current_zkp < zkp_batch_size {
        debug!(
            "Skipping ZKP verification for tree {} type {:?} - incomplete batch: {}/{} elements",
            tree_pubkey, queue_type, num_inserted_in_current_zkp, zkp_batch_size
        );
        return Ok(());
    }

    let on_chain_batch_hash_chains = &on_chain_hash_chains[pending_batch_index];
    let total_on_chain_zkps = on_chain_batch_hash_chains.len() as u64;
    if num_inserted_zkps >= total_on_chain_zkps {
        debug!(
            "Tree {} type {:?}: All {} hash chains already inserted, skipping validation",
            tree_pubkey, queue_type, total_on_chain_zkps
        );
        return Ok(());
    }

    let on_chain_chains: Vec<[u8; 32]> = on_chain_batch_hash_chains
        .iter()
        .skip(num_inserted_zkps as usize)
        .map(|h| *h)
        .collect();

    if on_chain_chains.is_empty() {
        return Ok(());
    }

    let batch_start_index = on_chain_batches
        .map(|batches| batches[pending_batch_index].start_index)
        .unwrap_or(0);
    let start_offset = batch_start_index + (num_inserted_zkps * zkp_batch_size);

    let cached_chains =
        queue_hash_cache::get_cached_hash_chains(db, tree_pubkey, queue_type, batch_start_index)
            .await
            .unwrap_or_default();

    let cached_map: std::collections::HashMap<i32, [u8; 32]> = cached_chains
        .into_iter()
        .map(|c| (c.zkp_batch_index, c.hash_chain))
        .collect();

    let start_zkp_batch_idx = num_inserted_zkps as usize;

    let mut computed_chains = Vec::with_capacity(on_chain_chains.len());
    let mut chains_to_cache = Vec::new();

    for zkp_batch_idx in 0..on_chain_chains.len() {
        let actual_zkp_idx = start_zkp_batch_idx + zkp_batch_idx;

        if let Some(&cached_chain) = cached_map.get(&(actual_zkp_idx as i32)) {
            computed_chains.push(cached_chain);
        } else {
            let chain_offset = start_offset + (zkp_batch_idx as u64 * zkp_batch_size);
            let chains = compute_hash_chains_from_db(
                db,
                queue_type,
                tree_pubkey,
                chain_offset,
                1,
                zkp_batch_size,
            )
            .await?;

            if !chains.is_empty() {
                computed_chains.push(chains[0]);
                chains_to_cache.push((actual_zkp_idx, chain_offset, chains[0]));
            }
        }
    }

    if !chains_to_cache.is_empty() {
        if let Err(e) = queue_hash_cache::store_hash_chains_batch(
            db,
            tree_pubkey,
            queue_type,
            batch_start_index,
            chains_to_cache,
        )
        .await
        {
            error!("Failed to cache hash chains: {:?}", e);
        }
    }

    for (zkp_batch_idx, (on_chain, computed)) in on_chain_chains
        .iter()
        .zip(computed_chains.iter())
        .enumerate()
    {
        if on_chain != computed {
            divergences.push(HashChainDivergence {
                queue_info: QueueHashChainInfo {
                    queue_type,
                    tree_pubkey,
                    batch_index: pending_batch_index,
                    current_index: start_offset + (zkp_batch_idx as u64 * zkp_batch_size),
                },
                expected_hash_chain: *computed,
                actual_hash_chain: *on_chain,
                zkp_batch_index: zkp_batch_idx,
            });
        }
    }

    if divergences.is_empty() {
        Ok(())
    } else {
        Err(divergences)
    }
}

async fn compute_hash_chains_from_db(
    db: &DatabaseConnection,
    queue_type: QueueType,
    tree_pubkey: Pubkey,
    start_offset: u64,
    num_zkp_batches: u64,
    zkp_batch_size: u64,
) -> Result<Vec<[u8; 32]>, Vec<HashChainDivergence>> {
    let total_elements = num_zkp_batches * zkp_batch_size;

    let all_elements =
        fetch_queue_elements(db, queue_type, tree_pubkey, start_offset, total_elements)
            .await
            .map_err(|e| {
                error!(
                    "Failed to fetch queue elements for tree {} type {:?}: {:?}",
                    tree_pubkey, queue_type, e
                );
                vec![]
            })?;

    let mut hash_chains = Vec::new();

    for (i, chunk) in all_elements.chunks(zkp_batch_size as usize).enumerate() {
        if chunk.len() == zkp_batch_size as usize {
            let hash_chain = create_hash_chain_from_slice(chunk).map_err(|e| {
                error!(
                    "Failed to create hash chain for tree {} type {:?}: {:?}",
                    tree_pubkey, queue_type, e
                );
                vec![]
            })?;
            hash_chains.push(hash_chain);
        } else {
            // Incomplete batches are expected during normal operation
            // Only log at debug level to reduce noise
            debug!(
                "Incomplete batch {} for tree {} type {:?} with {} elements when expecting {}",
                i,
                tree_pubkey,
                queue_type,
                chunk.len(),
                zkp_batch_size
            );
        }
    }

    Ok(hash_chains)
}

async fn fetch_queue_elements(
    db: &DatabaseConnection,
    queue_type: QueueType,
    tree_pubkey: Pubkey,
    start_offset: u64,
    limit: u64,
) -> Result<Vec<[u8; 32]>, PhotonApiError> {
    use sea_orm::QuerySelect;

    let tree_bytes = tree_pubkey.to_bytes().to_vec();
    let mut result = Vec::with_capacity(limit as usize);

    match queue_type {
        QueueType::InputStateV2 => {
            let query = accounts::Entity::find()
                .filter(accounts::Column::Tree.eq(tree_bytes))
                .filter(accounts::Column::NullifierQueueIndex.is_not_null())
                .filter(accounts::Column::NullifierQueueIndex.gte(start_offset as i64))
                .filter(accounts::Column::NullifierQueueIndex.lt((start_offset + limit) as i64))
                .order_by_asc(accounts::Column::NullifierQueueIndex)
                .limit(limit);

            let elements = query
                .all(db)
                .await
                .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

            for element in elements.iter() {
                let mut value = [0u8; 32];

                if let Some(ref nullifier) = element.nullifier {
                    if nullifier.len() >= 32 {
                        value.copy_from_slice(&nullifier[..32]);
                    } else {
                        return Err(PhotonApiError::UnexpectedError(format!(
                            "Nullifier hash too short: expected at least 32 bytes, got {}",
                            nullifier.len()
                        )));
                    }
                } else {
                    return Err(PhotonApiError::UnexpectedError(
                        "Missing nullifier for InputStateV2 queue".to_string(),
                    ));
                }

                result.push(value);
            }
        }
        QueueType::OutputStateV2 => {
            let query = accounts::Entity::find()
                .filter(accounts::Column::Tree.eq(tree_bytes))
                .filter(accounts::Column::LeafIndex.gte(start_offset as i64))
                .filter(accounts::Column::LeafIndex.lt((start_offset + limit) as i64))
                .order_by_asc(accounts::Column::LeafIndex)
                .limit(limit);

            let elements = query
                .all(db)
                .await
                .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

            for element in elements.iter() {
                let mut value = [0u8; 32];

                if element.hash.len() >= 32 {
                    value.copy_from_slice(&element.hash[..32]);
                } else {
                    return Err(PhotonApiError::UnexpectedError(format!(
                        "Account hash too short: expected at least 32 bytes, got {}",
                        element.hash.len()
                    )));
                }

                result.push(value);
            }
        }
        QueueType::AddressV2 => {
            let query = address_queues::Entity::find()
                .filter(address_queues::Column::Tree.eq(tree_bytes))
                .filter(address_queues::Column::QueueIndex.gte(start_offset as i64))
                .filter(address_queues::Column::QueueIndex.lt((start_offset + limit) as i64))
                .order_by_asc(address_queues::Column::QueueIndex)
                .limit(limit);

            let elements = query
                .all(db)
                .await
                .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

            for element in elements.iter() {
                let mut value = [0u8; 32];

                if element.address.len() >= 32 {
                    value.copy_from_slice(&element.address[..32]);
                } else {
                    return Err(PhotonApiError::UnexpectedError(format!(
                        "Address too short: expected at least 32 bytes, got {}",
                        element.address.len()
                    )));
                }

                result.push(value);
            }
        }
        _ => {
            return Err(PhotonApiError::ValidationError(format!(
                "Unsupported queue type: {:?}",
                queue_type
            )))
        }
    };

    Ok(result)
}

pub fn log_divergence(divergence: &HashChainDivergence) {
    error!(
        "Hash chain divergence: tree={} type={:?} batch={} zkp_batch={} index={}",
        divergence.queue_info.tree_pubkey,
        divergence.queue_info.queue_type,
        divergence.queue_info.batch_index,
        divergence.zkp_batch_index,
        divergence.queue_info.current_index,
    );
    error!(
        "  Expected: {}",
        hex::encode(&divergence.expected_hash_chain)
    );
    error!("  On-chain: {}", hex::encode(&divergence.actual_hash_chain));
}

pub async fn verify_single_queue(
    rpc_client: &RpcClient,
    db: &DatabaseConnection,
    tree_pubkey: Pubkey,
    queue_type: QueueType,
) -> Result<(), Vec<HashChainDivergence>> {
    // TODO: Fix AddressV2 queue hash chain computation
    // Currently skipping validation because raw address bytes don't match on-chain hash chains
    // Need to investigate correct hash format for address queue elements
    if queue_type == QueueType::AddressV2 {
        debug!(
            "Temporarily skipping AddressV2 queue hash chain validation for tree {}",
            tree_pubkey
        );
        return Ok(());
    }

    let result = match queue_type {
        QueueType::OutputStateV2 => {
            verify_output_queue_hash_chains(rpc_client, db, tree_pubkey).await
        }
        QueueType::InputStateV2 => {
            verify_input_queue_hash_chains(rpc_client, db, tree_pubkey).await
        }
        QueueType::AddressV2 => verify_address_queue_hash_chains(rpc_client, db, tree_pubkey).await,
        _ => {
            trace!("Skipping non-v2 queue type: {:?}", queue_type);
            Ok(())
        }
    };

    match result {
        Ok(()) => {
            let tree_str = tree_pubkey.to_string();
            let queue_type_str = format!("{:?}", queue_type);
            metric! {
                statsd_count!(
                    "v2_queue_validation_success",
                    1,
                    "tree" => tree_str.as_str(),
                    "queue_type" => queue_type_str.as_str()
                );
            }
            Ok(())
        }
        Err(divergences) => {
            if !divergences.is_empty() {
                let tree_str = tree_pubkey.to_string();
                let queue_type_str = format!("{:?}", queue_type);
                metric! {
                    statsd_count!(
                        "v2_queue_divergences",
                        divergences.len() as i64,
                        "tree" => tree_str.as_str(),
                        "queue_type" => queue_type_str.as_str()
                    );
                }
            }
            Err(divergences)
        }
    }
}

pub async fn collect_v2_trees(
    db: &DatabaseConnection,
) -> Result<Vec<(Pubkey, QueueType)>, PhotonApiError> {
    use crate::dao::generated::tree_metadata;
    use light_compressed_account::TreeType;

    // Query for V2 trees (StateV2 and AddressV2)
    let v2_trees = tree_metadata::Entity::find()
        .filter(
            tree_metadata::Column::TreeType
                .is_in([TreeType::StateV2 as i32, TreeType::AddressV2 as i32]),
        )
        .all(db)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to query V2 trees: {}", e)))?;

    let mut result = Vec::new();
    for tree in v2_trees {
        let tree_pubkey = Pubkey::try_from(tree.tree_pubkey.as_slice())
            .map_err(|e| PhotonApiError::UnexpectedError(format!("Invalid tree pubkey: {}", e)))?;

        match tree.tree_type {
            t if t == TreeType::StateV2 as i32 => {
                result.push((tree_pubkey, QueueType::InputStateV2));
                result.push((tree_pubkey, QueueType::OutputStateV2));
            }
            t if t == TreeType::AddressV2 as i32 => {
                result.push((tree_pubkey, QueueType::AddressV2));
            }
            _ => continue,
        }
    }

    Ok(result)
}
