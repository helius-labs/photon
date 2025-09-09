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
    let currently_processing_batch_index = queue_batches.get_current_batch_index();
    let current_batch = queue_batches.get_current_batch();

    verify_queue_hash_chains(
        db,
        QueueType::AddressV2,
        tree_pubkey,
        &merkle_tree.hash_chain_stores[..],
        Some(&queue_batches.batches),
        queue_batches.zkp_batch_size,
        currently_processing_batch_index,
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

    // Compute hash chains from database
    let computed_chains = compute_hash_chains_from_db(
        db,
        queue_type,
        tree_pubkey,
        start_offset,
        on_chain_chains.len() as u64,
        zkp_batch_size,
        num_inserted_in_current_zkp,
    )
    .await?;

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

async fn compute_hash_chains_from_db(
    db: &DatabaseConnection,
    queue_type: QueueType,
    tree_pubkey: Pubkey,
    start_offset: u64,
    num_zkp_batches: u64,
    zkp_batch_size: u64,
    num_inserted_in_current_zkp: u64,
) -> Result<Vec<[u8; 32]>, Vec<HashChainDivergence>> {
    let has_incomplete_batch = num_inserted_in_current_zkp > 0;
    let total_complete_batches = if has_incomplete_batch {
        num_zkp_batches - 1
    } else {
        num_zkp_batches
    };

    let total_elements = if has_incomplete_batch {
        (total_complete_batches * zkp_batch_size) + num_inserted_in_current_zkp
    } else {
        num_zkp_batches * zkp_batch_size
    };

    let all_elements =
        fetch_queue_elements(db, queue_type, tree_pubkey, start_offset, total_elements)
            .await
            .map_err(|e| {
                error!("Failed to fetch queue elements: {:?}", e);
                vec![]
            })?;

    let mut hash_chains = Vec::new();
    for chunk in all_elements.chunks(zkp_batch_size as usize) {
        let hash_chain = create_hash_chain_from_slice(chunk).map_err(|_| vec![])?;
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
    use crate::dao::generated::accounts;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

    let tree_bytes = tree_pubkey.to_bytes().to_vec();

    let query = match queue_type {
        QueueType::InputStateV2 => accounts::Entity::find()
            .filter(accounts::Column::Tree.eq(tree_bytes))
            .filter(accounts::Column::NullifierQueueIndex.is_not_null())
            .filter(accounts::Column::NullifierQueueIndex.gte(start_offset as i64))
            .filter(accounts::Column::NullifierQueueIndex.lt((start_offset + limit) as i64))
            .order_by_asc(accounts::Column::NullifierQueueIndex)
            .limit(limit),
        QueueType::OutputStateV2 => accounts::Entity::find()
            .filter(accounts::Column::Tree.eq(tree_bytes))
            .filter(accounts::Column::LeafIndex.gte(start_offset as i64))
            .filter(accounts::Column::LeafIndex.lt((start_offset + limit) as i64))
            .order_by_asc(accounts::Column::LeafIndex)
            .limit(limit),
        QueueType::AddressV2 => accounts::Entity::find()
            .filter(accounts::Column::Tree.eq(tree_bytes))
            .filter(accounts::Column::LeafIndex.gte(start_offset as i64))
            .filter(accounts::Column::LeafIndex.lt((start_offset + limit) as i64))
            .order_by_asc(accounts::Column::LeafIndex)
            .limit(limit),
        _ => {
            return Err(PhotonApiError::ValidationError(format!(
                "Unsupported queue type: {:?}",
                queue_type
            )))
        }
    };

    let elements = query
        .all(db)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

    // Convert to hash values based on queue type
    let mut result = Vec::with_capacity(elements.len());
    for element in elements.iter() {
        let mut value = [0u8; 32];

        match queue_type {
            QueueType::InputStateV2 => {
                // Input queues use nullifiers for hash chains
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
            }
            _ => {
                // Output and Address queues use account hashes
                if element.hash.len() >= 32 {
                    value.copy_from_slice(&element.hash[..32]);
                } else {
                    return Err(PhotonApiError::UnexpectedError(format!(
                        "Account hash too short: expected at least 32 bytes, got {}",
                        element.hash.len()
                    )));
                }
            }
        }

        result.push(value);
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
