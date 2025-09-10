use light_batched_merkle_tree::{
    batch::Batch, merkle_tree::BatchedMerkleTreeAccount, queue::BatchedQueueAccount,
};
use light_compressed_account::QueueType;
use light_hasher::hash_chain::create_hash_chain_from_slice;
use light_zero_copy::vec::ZeroCopyVecU64;
use log::{debug, error, trace};
use sea_orm::DatabaseConnection;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

use crate::{
    api::error::PhotonApiError,
    ingester::parser::tree_info::{TreeInfo, QUEUE_TREE_MAPPING},
    metric,
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

pub async fn verify_v2_queues(
    rpc_client: &RpcClient,
    db: &DatabaseConnection,
    tree_pubkeys: Vec<(Pubkey, QueueType)>,
) -> Result<(), Vec<HashChainDivergence>> {
    let mut divergences = Vec::new();

    for (tree_pubkey, queue_type) in tree_pubkeys {
        let result = match queue_type {
            QueueType::OutputStateV2 => {
                verify_output_queue_hash_chains(rpc_client, db, tree_pubkey).await
            }
            QueueType::InputStateV2 => {
                verify_input_queue_hash_chains(rpc_client, db, tree_pubkey).await
            }
            QueueType::AddressV2 => {
                verify_address_queue_hash_chains(rpc_client, db, tree_pubkey).await
            }
            _ => {
                trace!("Skipping non-v2 queue type: {:?}", queue_type);
                continue;
            }
        };

        if let Err(mut divs) = result {
            divergences.append(&mut divs);
        }
    }

    if !divergences.is_empty() {
        for divergence in &divergences {
            log_divergence(divergence);
        }
        metric! {
            statsd_count!("v2_queue_divergences", divergences.len() as i64);
        }
        Err(divergences)
    } else {
        debug!("All v2 queue hash chains verified successfully");
        metric! {
            statsd_count!("v2_queue_validation_success", 1);
        }
        Ok(())
    }
}

async fn verify_output_queue_hash_chains(
    rpc_client: &RpcClient,
    db: &DatabaseConnection,
    tree_pubkey: Pubkey,
) -> Result<(), Vec<HashChainDivergence>> {
    let tree_pubkey_str = tree_pubkey.to_string();
    let tree_info = TreeInfo::get(&tree_pubkey_str).ok_or_else(|| vec![])?;
    let queue_pubkey = Pubkey::from(tree_info.queue.to_bytes());

    let account = rpc_client
        .get_account(&queue_pubkey)
        .await
        .map_err(|_| vec![])?;
    let mut account_data = account.data.clone();
    let queue = BatchedQueueAccount::output_from_bytes(&mut account_data).map_err(|_| vec![])?;

    let tree_account = rpc_client
        .get_account(&tree_pubkey)
        .await
        .map_err(|_| vec![])?;
    let mut tree_account_data = tree_account.data.clone();
    let light_pubkey = light_compressed_account::Pubkey::from(tree_pubkey.to_bytes());
    let _merkle_tree =
        BatchedMerkleTreeAccount::state_from_bytes(&mut tree_account_data, &light_pubkey)
            .map_err(|_| vec![])?;

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
    let account = rpc_client
        .get_account(&tree_pubkey)
        .await
        .map_err(|_| vec![])?;
    let mut account_data = account.data.clone();
    let light_pubkey = light_compressed_account::Pubkey::from(tree_pubkey.to_bytes());
    let merkle_tree = BatchedMerkleTreeAccount::state_from_bytes(&mut account_data, &light_pubkey)
        .map_err(|_| vec![])?;

    let metadata = merkle_tree.get_metadata();
    let queue_batches = &metadata.queue_batches;
    // For InputStateV2, we should use pending_batch_index from queue_batches
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
    let account = rpc_client
        .get_account(&tree_pubkey)
        .await
        .map_err(|_| vec![])?;
    let mut account_data = account.data.clone();
    let light_pubkey = light_compressed_account::Pubkey::from(tree_pubkey.to_bytes());
    let merkle_tree =
        BatchedMerkleTreeAccount::address_from_bytes(&mut account_data, &light_pubkey)
            .map_err(|_| vec![])?;

    let metadata = merkle_tree.get_metadata();
    let queue_batches = &metadata.queue_batches;
    // Use pending_batch_index, NOT currently_processing_batch_index!
    // The pending_batch_index is where unprocessed items are waiting
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
    let on_chain_batch_hash_chains = &on_chain_hash_chains[pending_batch_index];
    
    // Get the total number of hash chains currently on-chain
    let total_on_chain_zkps = on_chain_batch_hash_chains.len() as u64;
    
    if num_inserted_zkps >= total_on_chain_zkps {
        // All on-chain hash chains have been inserted, nothing new to validate
        debug!(
            "{:?}: All {} hash chains already inserted, skipping validation",
            queue_type, total_on_chain_zkps
        );
        return Ok(());
    }
    
    // Validate ALL pending hash chains
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

    // check if we have cached hash chains for this batch
    let cached_chains =
        queue_hash_cache::get_cached_hash_chains(db, tree_pubkey, queue_type, batch_start_index)
            .await
            .unwrap_or_default();

    // determine which zkp batches we need to compute
    let start_zkp_batch_idx = num_inserted_zkps as usize;
    let end_zkp_batch_idx = start_zkp_batch_idx + on_chain_chains.len();

    let cached_map: std::collections::HashMap<i32, [u8; 32]> = cached_chains
        .into_iter()
        .map(|c| (c.zkp_batch_index, c.hash_chain))
        .collect();

    let mut chains_to_compute = Vec::new();
    let mut zkp_indices = Vec::new();

    for zkp_batch_idx in start_zkp_batch_idx..end_zkp_batch_idx {
        zkp_indices.push(zkp_batch_idx);
        if !cached_map.contains_key(&(zkp_batch_idx as i32)) {
            chains_to_compute.push(zkp_batch_idx);
        }
    }

    // Compute missing hash chains if needed
    let mut computed_map = cached_map;

    if !chains_to_compute.is_empty() {
        let new_chains = compute_missing_hash_chains(
            db,
            queue_type,
            tree_pubkey,
            batch_start_index,
            zkp_batch_size,
            &chains_to_compute,
            num_inserted_in_current_zkp,
        )
        .await?;

        // Store the newly computed chains for future use
        let chains_to_store: Vec<(usize, u64, [u8; 32])> = chains_to_compute
            .iter()
            .zip(new_chains.iter())
            .map(|(zkp_idx, hash)| {
                let offset = batch_start_index + (*zkp_idx as u64 * zkp_batch_size);
                (*zkp_idx, offset, *hash)
            })
            .collect();

        if let Err(e) = queue_hash_cache::store_hash_chains_batch(
            db,
            tree_pubkey,
            queue_type,
            batch_start_index,
            chains_to_store,
        )
        .await
        {
            error!("Failed to store hash chains: {:?}", e);
        }

        // Add newly computed chains to our map
        for (zkp_idx, hash) in chains_to_compute.iter().zip(new_chains.iter()) {
            computed_map.insert(*zkp_idx as i32, *hash);
        }
    }

    // Build the final computed_chains vector in the correct order
    let computed_chains: Vec<[u8; 32]> = zkp_indices
        .iter()
        .map(|idx| {
            computed_map
                .get(&(*idx as i32))
                .copied()
                .expect("Hash chain should exist either from cache or computation")
        })
        .collect();

    if computed_chains.len() != on_chain_chains.len() {
        error!(
            "Computed chains count mismatch: computed={}, on_chain={}",
            computed_chains.len(),
            on_chain_chains.len()
        );
    }

    
    // Check for divergences
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

async fn compute_missing_hash_chains(
    db: &DatabaseConnection,
    queue_type: QueueType,
    tree_pubkey: Pubkey,
    batch_start_index: u64,
    zkp_batch_size: u64,
    zkp_batch_indices: &[usize],
    num_inserted_in_current_zkp: u64,
) -> Result<Vec<[u8; 32]>, Vec<HashChainDivergence>> {
    let mut hash_chains = Vec::new();
    
    // Calculate total elements needed
    let total_elements_needed = zkp_batch_indices.len() * zkp_batch_size as usize;
    
    debug!(
        "{:?}: Fetching {} total elements for {} zkp batches",
        queue_type, total_elements_needed, zkp_batch_indices.len()
    );
    
    // Fetch all elements at once based on queue type
    // For state queues, we need to offset by the first zkp_batch_idx we're processing
    let all_elements = match queue_type {
        QueueType::AddressV2 => {
            fetch_all_address_queue_elements(db, tree_pubkey, total_elements_needed).await
        }
        QueueType::InputStateV2 => {
            // Calculate the proper start offset: batch_start + (first_zkp_batch_idx * zkp_batch_size)
            let start_offset = batch_start_index + (zkp_batch_indices[0] as u64 * zkp_batch_size);
            fetch_all_input_queue_elements(db, tree_pubkey, start_offset, total_elements_needed).await
        }
        QueueType::OutputStateV2 => {
            // Calculate the proper start offset: batch_start + (first_zkp_batch_idx * zkp_batch_size)
            let start_offset = batch_start_index + (zkp_batch_indices[0] as u64 * zkp_batch_size);
            fetch_all_output_queue_elements(db, tree_pubkey, start_offset, total_elements_needed).await
        }
        _ => {
            return Err(vec![HashChainDivergence {
                queue_info: QueueHashChainInfo {
                    queue_type,
                    tree_pubkey,
                    batch_index: 0,
                    current_index: 0,
                },
                expected_hash_chain: [0; 32],
                actual_hash_chain: [0; 32],
                zkp_batch_index: 0,
            }]);
        }
    }.map_err(|e| {
        error!("Failed to fetch queue elements: {:?}", e);
        vec![]
    })?;
    
    if all_elements.is_empty() {
        error!("No elements found for queue_type={:?}, tree={}", queue_type, tree_pubkey);
        return Err(vec![]);
    }
    
    debug!(
        "Fetched {} total elements, splitting into {} zkp batches",
        all_elements.len(),
        zkp_batch_indices.len()
    );
    
    // Split into zkp batches and compute hash chains
    for (idx, &zkp_batch_idx) in zkp_batch_indices.iter().enumerate() {
        let batch_start = idx * zkp_batch_size as usize;
        let batch_end = if zkp_batch_idx == zkp_batch_indices.last().copied().unwrap_or(0) 
            && num_inserted_in_current_zkp > 0 {
            // Last batch might be incomplete
            batch_start + num_inserted_in_current_zkp as usize
        } else {
            batch_start + zkp_batch_size as usize
        };
        
        let batch_end = batch_end.min(all_elements.len());
        
        if batch_start >= all_elements.len() {
            debug!("No more elements for zkp_batch {}", zkp_batch_idx);
            break;
        }
        
        let batch_elements = &all_elements[batch_start..batch_end];
        
        debug!(
            "zkp_batch {}: using elements [{}, {}) - {} elements",
            zkp_batch_idx, batch_start, batch_end, batch_elements.len()
        );
        
        let hash_chain = create_hash_chain_from_slice(batch_elements).map_err(|e| {
            error!("Failed to create hash chain: {:?}", e);
            vec![]
        })?;
        
        debug!(
            "zkp_batch {} computed hash chain: {}",
            zkp_batch_idx,
            hex::encode(&hash_chain)
        );
        
        hash_chains.push(hash_chain);
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
    use crate::dao::generated::{accounts, address_queues};
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

    let tree_bytes = tree_pubkey.to_bytes().to_vec();
    let mut result = Vec::new();

    match queue_type {
        QueueType::AddressV2 => {
            // For AddressV2, match the forester's logic exactly:
            // processed_items_offset = zkp_batch_idx * zkp_batch_size
            // This is used as start_queue_index: WHERE queue_index >= start_queue_index
            
            // Calculate which zkp batch we're validating
            // start_offset is the tree position (e.g., 15001, 15251, 15501)
            // limit is the zkp_batch_size (250 for addresses)
            let batch_start = if start_offset > 15000 { 15001 } else { 1 };
            let zkp_batch_idx = (start_offset - batch_start) / limit;
            
            // Following forester: processed_items_offset = chunk_idx * zkp_batch_size  
            let start_queue_index = zkp_batch_idx * limit;
            
            debug!(
                "Fetching AddressV2: tree={}, start_offset={}, zkp_batch_idx={}, start_queue_index={}, limit={}",
                tree_pubkey, start_offset, zkp_batch_idx, start_queue_index, limit
            );
            
            // Fetch addresses using queue_index >= start_queue_index
            // This exactly matches get_batch_address_update_info
            let elements = address_queues::Entity::find()
                .filter(address_queues::Column::Tree.eq(tree_bytes.clone()))
                .filter(address_queues::Column::QueueIndex.gte(start_queue_index as i64))
                .order_by_asc(address_queues::Column::QueueIndex)
                .limit(limit)
                .all(db)
                .await
                .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;
            
            if elements.is_empty() {
                debug!("No pending addresses with queue_index >= {} for tree {}", start_queue_index, tree_pubkey);
                return Ok(vec![]);
            }
            
            debug!(
                "Found {} addresses for zkp_batch {} (queue_index range: {}-{})",
                elements.len(),
                zkp_batch_idx,
                elements.first().map(|e| e.queue_index).unwrap_or(0),
                elements.last().map(|e| e.queue_index).unwrap_or(0)
            );
            
            for element in elements.iter() {
                if element.address.len() >= 32 {
                    let mut value = [0u8; 32];
                    value.copy_from_slice(&element.address[..32]);
                    result.push(value);
                } else {
                    return Err(PhotonApiError::UnexpectedError(format!(
                        "Address too short: expected at least 32 bytes, got {}",
                        element.address.len()
                    )));
                }
            }
        }
        QueueType::InputStateV2 => {
            let elements = accounts::Entity::find()
                .filter(accounts::Column::Tree.eq(tree_bytes))
                .filter(accounts::Column::NullifierQueueIndex.is_not_null())
                .filter(accounts::Column::NullifierQueueIndex.gte(start_offset as i64))
                .filter(accounts::Column::NullifierQueueIndex.lt((start_offset + limit) as i64))
                .order_by_asc(accounts::Column::NullifierQueueIndex)
                .limit(limit)
                .all(db)
                .await
                .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;
            
            for element in elements.iter() {
                if let Some(ref nullifier) = element.nullifier {
                    if nullifier.len() >= 32 {
                        let mut value = [0u8; 32];
                        value.copy_from_slice(&nullifier[..32]);
                        result.push(value);
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
            }
        }
        QueueType::OutputStateV2 => {
            let elements = accounts::Entity::find()
                .filter(accounts::Column::Tree.eq(tree_bytes))
                .filter(accounts::Column::LeafIndex.gte(start_offset as i64))
                .filter(accounts::Column::LeafIndex.lt((start_offset + limit) as i64))
                .order_by_asc(accounts::Column::LeafIndex)
                .limit(limit)
                .all(db)
                .await
                .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;
            
            for element in elements.iter() {
                if element.hash.len() >= 32 {
                    let mut value = [0u8; 32];
                    value.copy_from_slice(&element.hash[..32]);
                    result.push(value);
                } else {
                    return Err(PhotonApiError::UnexpectedError(format!(
                        "Account hash too short: expected at least 32 bytes, got {}",
                        element.hash.len()
                    )));
                }
            }
        }
        _ => {
            return Err(PhotonApiError::ValidationError(format!(
                "Unsupported queue type: {:?}",
                queue_type
            )))
        }
    }

    Ok(result)
}

// Fetch all pending addresses from the queue at once for AddressV2
async fn fetch_all_address_queue_elements(
    db: &DatabaseConnection,
    tree_pubkey: Pubkey,
    limit: usize,
) -> Result<Vec<[u8; 32]>, PhotonApiError> {
    use crate::dao::generated::address_queues;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
    
    let tree_bytes = tree_pubkey.to_bytes().to_vec();
    let mut result = Vec::new();
    
    debug!(
        "Fetching all AddressV2 elements: tree={}, limit={}",
        tree_pubkey, limit
    );
    
    // Fetch all pending addresses from the queue in order
    let elements = address_queues::Entity::find()
        .filter(address_queues::Column::Tree.eq(tree_bytes))
        .order_by_asc(address_queues::Column::QueueIndex)
        .limit(limit as u64)
        .all(db)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;
    
    debug!(
        "Found {} pending addresses (queue_index range: {}-{})",
        elements.len(),
        elements.first().map(|e| e.queue_index).unwrap_or(0),
        elements.last().map(|e| e.queue_index).unwrap_or(0)
    );
    
    
    
    for element in elements.iter() {
        if element.address.len() >= 32 {
            let mut value = [0u8; 32];
            value.copy_from_slice(&element.address[..32]);
            result.push(value);
        } else {
            return Err(PhotonApiError::UnexpectedError(format!(
                "Address too short: expected at least 32 bytes, got {}",
                element.address.len()
            )));
        }
    }
    
    Ok(result)
}

// Fetch all input state elements from accounts table at once
async fn fetch_all_input_queue_elements(
    db: &DatabaseConnection,
    tree_pubkey: Pubkey,
    start_offset: u64,
    limit: usize,
) -> Result<Vec<[u8; 32]>, PhotonApiError> {
    use crate::dao::generated::accounts;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
    
    let tree_bytes = tree_pubkey.to_bytes().to_vec();
    let mut result = Vec::new();
    
    debug!(
        "Fetching all InputStateV2 elements: tree={}, start_offset={}, limit={}",
        tree_pubkey, start_offset, limit
    );
    
    let elements = accounts::Entity::find()
        .filter(accounts::Column::Tree.eq(tree_bytes))
        .filter(accounts::Column::NullifierQueueIndex.is_not_null())
        .filter(accounts::Column::NullifierQueueIndex.gte(start_offset as i64))
        .filter(accounts::Column::NullifierQueueIndex.lt((start_offset + limit as u64) as i64))
        .order_by_asc(accounts::Column::NullifierQueueIndex)
        .all(db)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;
    
    debug!(
        "Found {} input states (nullifier_queue_index range: {}-{})",
        elements.len(),
        elements.first().and_then(|e| e.nullifier_queue_index).unwrap_or(0),
        elements.last().and_then(|e| e.nullifier_queue_index).unwrap_or(0)
    );
    
    for element in elements.iter() {
        if let Some(ref nullifier) = element.nullifier {
            if nullifier.len() >= 32 {
                let mut value = [0u8; 32];
                value.copy_from_slice(&nullifier[..32]);
                result.push(value);
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
    }
    
    Ok(result)
}

// Fetch all output state elements from accounts table at once
async fn fetch_all_output_queue_elements(
    db: &DatabaseConnection,
    tree_pubkey: Pubkey,
    start_offset: u64,
    limit: usize,
) -> Result<Vec<[u8; 32]>, PhotonApiError> {
    use crate::dao::generated::accounts;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
    
    let tree_bytes = tree_pubkey.to_bytes().to_vec();
    let mut result = Vec::new();
    
    debug!(
        "Fetching all OutputStateV2 elements: tree={}, start_offset={}, limit={}",
        tree_pubkey, start_offset, limit
    );
    
    let elements = accounts::Entity::find()
        .filter(accounts::Column::Tree.eq(tree_bytes))
        .filter(accounts::Column::LeafIndex.gte(start_offset as i64))
        .filter(accounts::Column::LeafIndex.lt((start_offset + limit as u64) as i64))
        .order_by_asc(accounts::Column::LeafIndex)
        .all(db)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;
    
    debug!(
        "Found {} output states (leaf_index range: {}-{})",
        elements.len(),
        elements.first().map(|e| e.leaf_index).unwrap_or(0),
        elements.last().map(|e| e.leaf_index).unwrap_or(0)
    );
    
    for element in elements.iter() {
        if element.hash.len() >= 32 {
            let mut value = [0u8; 32];
            value.copy_from_slice(&element.hash[..32]);
            result.push(value);
        } else {
            return Err(PhotonApiError::UnexpectedError(format!(
                "Account hash too short: expected at least 32 bytes, got {}",
                element.hash.len()
            )));
        }
    }
    
    Ok(result)
}

fn log_divergence(divergence: &HashChainDivergence) {
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

pub async fn collect_v2_trees() -> Vec<(Pubkey, QueueType)> {
    let mut trees = Vec::new();

    for (_, tree_info) in QUEUE_TREE_MAPPING.iter() {
        match tree_info.tree_type {
            light_compressed_account::TreeType::StateV2 => {
                let sdk_pubkey = Pubkey::from(tree_info.tree.to_bytes());
                trees.push((sdk_pubkey, QueueType::InputStateV2));
                trees.push((sdk_pubkey, QueueType::OutputStateV2));
            }
            light_compressed_account::TreeType::AddressV2 => {
                let sdk_pubkey = Pubkey::from(tree_info.tree.to_bytes());
                trees.push((sdk_pubkey, QueueType::AddressV2));
            }
            _ => {}
        }
    }

    trees
}
