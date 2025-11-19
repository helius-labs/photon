use light_batched_merkle_tree::constants::DEFAULT_ADDRESS_ZKP_BATCH_SIZE;
use light_compressed_account::QueueType;
use light_hasher::{Hasher, Poseidon};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, FromQueryResult,
    QueryFilter, QueryOrder, QuerySelect, Statement, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::api::method::get_multiple_new_address_proofs::{
    get_multiple_new_address_proofs_helper, AddressWithTree, MAX_ADDRESSES,
};
use crate::common::format_bytes;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::get_multiple_compressed_leaf_proofs_by_indices;
use crate::{ingester::persist::persisted_state_tree::get_subtrees, monitor::queue_hash_cache};
use solana_sdk::pubkey::Pubkey;

const MAX_QUEUE_ELEMENTS: u16 = 30_000;
const TREE_HEIGHT: u8 = 32;

/// Encode tree node position as a single u64
/// Format: [level: u8][position: 56 bits]
/// Level 0 = leaves, Level 31 = root
#[inline]
fn encode_node_index(level: u8, position: u64) -> u64 {
    debug_assert!(level < TREE_HEIGHT);
    ((level as u64) << 56) | position
}

enum QueueDataV2 {
    Output(OutputQueueDataV2),
    Input(InputQueueDataV2),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsV2Request {
    pub tree: Hash,

    pub output_queue_start_index: Option<u64>,
    pub output_queue_limit: Option<u16>,

    pub input_queue_start_index: Option<u64>,
    pub input_queue_limit: Option<u16>,

    pub address_queue_start_index: Option<u64>,
    pub address_queue_limit: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub address_queue_zkp_batch_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsV2Response {
    pub context: Context,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_queue: Option<OutputQueueDataV2>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_queue: Option<InputQueueDataV2>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub address_queue: Option<AddressQueueDataV2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OutputQueueDataV2 {
    pub leaf_indices: Vec<u64>,
    pub account_hashes: Vec<Hash>,
    pub leaves: Vec<Hash>,

    /// Deduplicated tree nodes
    /// node_index encoding: (level << 56) | position
    pub nodes: Vec<u64>,
    pub node_hashes: Vec<Hash>,

    pub initial_root: Hash,
    pub first_queue_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InputQueueDataV2 {
    pub leaf_indices: Vec<u64>,
    pub account_hashes: Vec<Hash>,
    pub leaves: Vec<Hash>,
    pub tx_hashes: Vec<Hash>,

    /// Deduplicated tree nodes
    pub nodes: Vec<u64>,
    pub node_hashes: Vec<Hash>,

    pub initial_root: Hash,
    pub first_queue_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AddressQueueDataV2 {
    pub addresses: Vec<SerializablePubkey>,
    pub queue_indices: Vec<u64>,
    pub nodes: Vec<u64>,
    pub node_hashes: Vec<Hash>,
    pub low_element_indices: Vec<u64>,
    pub low_element_values: Vec<Hash>,
    pub low_element_next_indices: Vec<u64>,
    pub low_element_next_values: Vec<Hash>,
    pub low_element_proofs: Vec<Vec<Hash>>,
    pub leaves_hash_chains: Vec<Hash>,
    pub initial_root: Hash,
    pub start_index: u64,
    pub subtrees: Vec<Hash>,
}

#[derive(FromQueryResult, Debug)]
struct QueueElement {
    leaf_index: i64,
    hash: Vec<u8>,
    tx_hash: Option<Vec<u8>>,
    nullifier_queue_index: Option<i64>,
}

pub async fn get_queue_elements_v2(
    conn: &DatabaseConnection,
    request: GetQueueElementsV2Request,
) -> Result<GetQueueElementsV2Response, PhotonApiError> {
    let has_output_request = request.output_queue_limit.is_some();
    let has_input_request = request.input_queue_limit.is_some();
    let has_address_request = request.address_queue_limit.is_some();

    if !has_output_request && !has_input_request && !has_address_request {
        return Err(PhotonApiError::ValidationError(
            "At least one queue must be requested".to_string(),
        ));
    }

    let context = Context::extract(conn).await?;

    let tx = conn.begin().await?;
    crate::api::set_transaction_isolation_if_needed(&tx).await?;

    let output_queue = if let Some(limit) = request.output_queue_limit {
        match fetch_queue_v2(
            &tx,
            &request.tree,
            QueueType::OutputStateV2,
            request.output_queue_start_index,
            limit,
        )
        .await?
        {
            QueueDataV2::Output(data) => Some(data),
            QueueDataV2::Input(_) => unreachable!("OutputStateV2 should return Output"),
        }
    } else {
        None
    };

    let input_queue = if let Some(limit) = request.input_queue_limit {
        match fetch_queue_v2(
            &tx,
            &request.tree,
            QueueType::InputStateV2,
            request.input_queue_start_index,
            limit,
        )
        .await?
        {
            QueueDataV2::Input(data) => Some(data),
            QueueDataV2::Output(_) => unreachable!("InputStateV2 should return Input"),
        }
    } else {
        None
    };

    let address_zkp_batch_size = request
        .address_queue_zkp_batch_size
        .unwrap_or(DEFAULT_ADDRESS_ZKP_BATCH_SIZE as u16);
    let address_queue = if let Some(limit) = request.address_queue_limit {
        Some(
            fetch_address_queue_v2(
                &tx,
                &request.tree,
                request.address_queue_start_index,
                limit,
                address_zkp_batch_size,
            )
            .await?,
        )
    } else {
        None
    };

    tx.commit().await?;

    Ok(GetQueueElementsV2Response {
        context,
        output_queue,
        input_queue,
        address_queue,
    })
}

async fn fetch_queue_v2(
    tx: &sea_orm::DatabaseTransaction,
    tree: &Hash,
    queue_type: QueueType,
    start_index: Option<u64>,
    limit: u16,
) -> Result<QueueDataV2, PhotonApiError> {
    if limit > MAX_QUEUE_ELEMENTS {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many queue elements requested {}. Maximum allowed: {}",
            limit, MAX_QUEUE_ELEMENTS
        )));
    }

    let mut query_condition = Condition::all().add(accounts::Column::Tree.eq(tree.to_vec()));

    let query = match queue_type {
        QueueType::InputStateV2 => {
            query_condition = query_condition
                .add(accounts::Column::NullifierQueueIndex.is_not_null())
                .add(accounts::Column::NullifiedInTree.eq(false));
            if let Some(start_queue_index) = start_index {
                query_condition = query_condition
                    .add(accounts::Column::NullifierQueueIndex.gte(start_queue_index as i64));
            }
            accounts::Entity::find()
                .filter(query_condition)
                .order_by_asc(accounts::Column::NullifierQueueIndex)
        }
        QueueType::OutputStateV2 => {
            query_condition = query_condition.add(accounts::Column::InOutputQueue.eq(true));
            if let Some(start_queue_index) = start_index {
                query_condition =
                    query_condition.add(accounts::Column::LeafIndex.gte(start_queue_index as i64));
            }
            accounts::Entity::find()
                .filter(query_condition)
                .order_by_asc(accounts::Column::LeafIndex)
        }
        _ => {
            return Err(PhotonApiError::ValidationError(format!(
                "Invalid queue type: {:?}",
                queue_type
            )))
        }
    };

    let queue_elements: Vec<QueueElement> = query
        .limit(limit as u64)
        .into_model::<QueueElement>()
        .all(tx)
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("DB error fetching queue elements: {}", e))
        })?;

    if queue_elements.is_empty() {
        return Ok(match queue_type {
            QueueType::OutputStateV2 => QueueDataV2::Output(OutputQueueDataV2::default()),
            QueueType::InputStateV2 => QueueDataV2::Input(InputQueueDataV2::default()),
            _ => unreachable!("Only OutputStateV2 and InputStateV2 are supported"),
        });
    }

    let indices: Vec<u64> = queue_elements.iter().map(|e| e.leaf_index as u64).collect();
    let first_queue_index = match queue_type {
        QueueType::InputStateV2 => {
            queue_elements[0]
                .nullifier_queue_index
                .ok_or(PhotonApiError::ValidationError(
                    "Nullifier queue index is missing".to_string(),
                ))? as u64
        }
        QueueType::OutputStateV2 => queue_elements[0].leaf_index as u64,
        _ => unreachable!("Only OutputStateV2 and InputStateV2 are supported"),
    };

    let generated_proofs = get_multiple_compressed_leaf_proofs_by_indices(
        tx,
        SerializablePubkey::from(tree.0),
        indices.clone(),
    )
    .await?;

    if generated_proofs.len() != indices.len() {
        return Err(PhotonApiError::ValidationError(format!(
            "Expected {} proofs for {} queue elements, but got {} proofs",
            indices.len(),
            queue_elements.len(),
            generated_proofs.len()
        )));
    }

    let (nodes, node_hashes) = deduplicate_nodes(&generated_proofs);

    let initial_root = generated_proofs[0].root.clone();

    let leaf_indices = indices;
    let account_hashes: Vec<Hash> = queue_elements
        .iter()
        .map(|e| Hash::new(e.hash.as_slice()).unwrap())
        .collect();
    let leaves: Vec<Hash> = generated_proofs.iter().map(|p| p.hash.clone()).collect();

    Ok(match queue_type {
        QueueType::OutputStateV2 => QueueDataV2::Output(OutputQueueDataV2 {
            leaf_indices,
            account_hashes,
            leaves,
            nodes,
            node_hashes,
            initial_root,
            first_queue_index,
        }),
        QueueType::InputStateV2 => {
            let tx_hashes: Vec<Hash> = queue_elements
                .iter()
                .map(|e| {
                    e.tx_hash
                        .as_ref()
                        .map(|tx| Hash::new(tx.as_slice()).unwrap())
                        .unwrap_or_default()
                })
                .collect();

            QueueDataV2::Input(InputQueueDataV2 {
                leaf_indices,
                account_hashes,
                leaves,
                tx_hashes,
                nodes,
                node_hashes,
                initial_root,
                first_queue_index,
            })
        }
        _ => unreachable!("Only OutputStateV2 and InputStateV2 are supported"),
    })
}

async fn fetch_address_queue_v2(
    tx: &sea_orm::DatabaseTransaction,
    tree: &Hash,
    start_queue_index: Option<u64>,
    limit: u16,
    zkp_batch_size: u16,
) -> Result<AddressQueueDataV2, PhotonApiError> {
    if limit as usize > MAX_ADDRESSES {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many addresses requested {}. Maximum allowed: {}",
            limit, MAX_ADDRESSES
        )));
    }

    let merkle_tree_bytes = tree.to_vec();
    let serializable_tree =
        SerializablePubkey::try_from(merkle_tree_bytes.clone()).map_err(|_| {
            PhotonApiError::UnexpectedError("Failed to parse merkle tree pubkey".to_string())
        })?;

    let tree_info = TreeInfo::get(tx, &serializable_tree.to_string())
        .await?
        .ok_or_else(|| PhotonApiError::UnexpectedError("Failed to get tree info".to_string()))?;

    let max_index_stmt = Statement::from_string(
        tx.get_database_backend(),
        format!(
            "SELECT COALESCE(MAX(leaf_index + 1), 1) as max_index FROM indexed_trees WHERE tree = {}",
            format_bytes(merkle_tree_bytes.clone(), tx.get_database_backend())
        ),
    );
    let max_index_result = tx.query_one(max_index_stmt).await?;
    let batch_start_index = match max_index_result {
        Some(row) => row.try_get::<i64>("", "max_index")? as usize,
        None => 1,
    };

    let offset_condition = match start_queue_index {
        Some(start) => format!("AND queue_index >= {}", start),
        None => String::new(),
    };

    let address_queue_stmt = Statement::from_string(
        tx.get_database_backend(),
        format!(
            "SELECT tree, address, queue_index FROM address_queues
             WHERE tree = {}
             {}
             ORDER BY queue_index ASC
             LIMIT {}",
            format_bytes(merkle_tree_bytes.clone(), tx.get_database_backend()),
            offset_condition,
            limit
        ),
    );

    let queue_results = tx.query_all(address_queue_stmt).await.map_err(|e| {
        PhotonApiError::UnexpectedError(format!("DB error fetching address queue: {}", e))
    })?;

    let subtrees = get_subtrees(tx, merkle_tree_bytes.clone(), tree_info.height as usize)
        .await?
        .into_iter()
        .map(Hash::from)
        .collect();

    if queue_results.is_empty() {
        return Ok(AddressQueueDataV2 {
            start_index: batch_start_index as u64,
            subtrees,
            low_element_proofs: Vec::new(),
            ..Default::default()
        });
    }

    let mut addresses = Vec::with_capacity(queue_results.len());
    let mut queue_indices = Vec::with_capacity(queue_results.len());
    let mut addresses_with_trees = Vec::with_capacity(queue_results.len());

    for row in &queue_results {
        let address: Vec<u8> = row.try_get("", "address")?;
        let queue_index: i64 = row.try_get("", "queue_index")?;
        let address_pubkey = SerializablePubkey::try_from(address.clone()).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to parse address: {}", e))
        })?;

        addresses.push(address_pubkey);
        queue_indices.push(queue_index as u64);
        addresses_with_trees.push(AddressWithTree {
            address: address_pubkey,
            tree: serializable_tree,
        });
    }

    let non_inclusion_proofs =
        get_multiple_new_address_proofs_helper(tx, addresses_with_trees, MAX_ADDRESSES, false)
            .await?;

    if non_inclusion_proofs.len() != queue_results.len() {
        return Err(PhotonApiError::ValidationError(format!(
            "Expected {} proofs for {} queue elements, but got {} proofs",
            queue_results.len(),
            queue_results.len(),
            non_inclusion_proofs.len()
        )));
    }

    let mut nodes_map: HashMap<u64, Hash> = HashMap::new();
    let mut low_element_indices = Vec::with_capacity(non_inclusion_proofs.len());
    let mut low_element_values = Vec::with_capacity(non_inclusion_proofs.len());
    let mut low_element_next_indices = Vec::with_capacity(non_inclusion_proofs.len());
    let mut low_element_next_values = Vec::with_capacity(non_inclusion_proofs.len());
    let mut low_element_proofs = Vec::with_capacity(non_inclusion_proofs.len());

    for proof in &non_inclusion_proofs {
        let low_value = Hash::new(&proof.lowerRangeAddress.to_bytes_vec()).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Invalid low element value: {}", e))
        })?;
        let next_value = Hash::new(&proof.higherRangeAddress.to_bytes_vec()).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Invalid next element value: {}", e))
        })?;

        low_element_indices.push(proof.lowElementLeafIndex as u64);
        low_element_values.push(low_value.clone());
        low_element_next_indices.push(proof.nextIndex as u64);
        low_element_next_values.push(next_value.clone());
        low_element_proofs.push(proof.proof.clone());

        let mut pos = proof.lowElementLeafIndex as u64;
        for (level, node_hash) in proof.proof.iter().enumerate() {
            let sibling_pos = if pos % 2 == 0 { pos + 1 } else { pos - 1 };
            let node_idx = encode_node_index(level as u8, sibling_pos);
            nodes_map.insert(node_idx, node_hash.clone());
            pos /= 2;
        }

        let leaf_idx = encode_node_index(0, proof.lowElementLeafIndex as u64);
        let hashed_leaf = compute_indexed_leaf_hash(&low_value, &next_value)?;
        nodes_map.insert(leaf_idx, hashed_leaf);
    }

    let mut sorted_nodes: Vec<(u64, Hash)> = nodes_map.into_iter().collect();
    sorted_nodes.sort_by_key(|(idx, _)| *idx);
    let (nodes, node_hashes): (Vec<u64>, Vec<Hash>) = sorted_nodes.into_iter().unzip();

    let initial_root = non_inclusion_proofs
        .first()
        .map(|proof| proof.root.clone())
        .unwrap_or_default();

    // Fetch cached hash chains for this batch
    let mut leaves_hash_chains = Vec::new();
    let tree_pubkey_bytes: [u8; 32] = serializable_tree
        .to_bytes_vec()
        .as_slice()
        .try_into()
        .map_err(|_| PhotonApiError::UnexpectedError("Invalid tree pubkey bytes".to_string()))?;
    let tree_pubkey = Pubkey::new_from_array(tree_pubkey_bytes);
    let cached = queue_hash_cache::get_cached_hash_chains(
        tx,
        tree_pubkey,
        QueueType::AddressV2,
        batch_start_index as u64,
    )
    .await
    .map_err(|e| PhotonApiError::UnexpectedError(format!("Cache error: {}", e)))?;

    if !cached.is_empty() {
        let mut sorted = cached;
        sorted.sort_by_key(|c| c.zkp_batch_index);
        for entry in sorted {
            leaves_hash_chains.push(Hash::from(entry.hash_chain));
        }
    } else if !addresses.is_empty() {
        if zkp_batch_size == 0 {
            return Err(PhotonApiError::ValidationError(
                "Address queue ZKP batch size must be greater than zero".to_string(),
            ));
        }

        let batch_size = zkp_batch_size as usize;
        let batch_count = addresses.len() / batch_size;

        if batch_count > 0 {
            let mut chains_to_cache = Vec::new();

            for batch_idx in 0..batch_count {
                let start = batch_idx * batch_size;
                let end = start + batch_size;
                let slice = &addresses[start..end];

                let mut decoded = Vec::with_capacity(batch_size);
                for pk in slice {
                    let bytes = pk.to_bytes_vec();
                    let arr: [u8; 32] = bytes
                        .as_slice()
                        .try_into()
                        .map_err(|_| {
                            PhotonApiError::UnexpectedError(
                                "Invalid address pubkey length for hash chain".to_string(),
                            )
                        })?;
                    decoded.push(arr);
                }

                let hash_chain = create_hash_chain_from_slice(&decoded).map_err(|e| {
                    PhotonApiError::UnexpectedError(format!("Hash chain error: {}", e))
                })?;

                leaves_hash_chains.push(Hash::from(hash_chain));
                let chain_offset =
                    (batch_start_index as u64) + (batch_idx as u64 * zkp_batch_size as u64);
                chains_to_cache.push((batch_idx, chain_offset, hash_chain));
            }

            if !chains_to_cache.is_empty() {
                let _ = queue_hash_cache::store_hash_chains_batch(
                    tx,
                    tree_pubkey,
                    QueueType::AddressV2,
                    batch_start_index as u64,
                    chains_to_cache,
                )
                .await;
            }
        }
    }

    Ok(AddressQueueDataV2 {
        addresses,
        queue_indices,
        nodes,
        node_hashes,
        low_element_indices,
        low_element_values,
        low_element_next_indices,
        low_element_next_values,
        low_element_proofs,
        leaves_hash_chains,
        initial_root,
        start_index: batch_start_index as u64,
        subtrees,
    })
}

/// Deduplicate nodes across all merkle proofs
/// Returns parallel arrays: (node_indices, node_hashes)
fn deduplicate_nodes(
    proofs: &[crate::ingester::persist::MerkleProofWithContext],
) -> (Vec<u64>, Vec<Hash>) {
    let mut nodes_map: HashMap<u64, Hash> = HashMap::new();

    for proof_ctx in proofs {
        let mut pos = proof_ctx.leaf_index as u64;

        for (level, node_hash) in proof_ctx.proof.iter().enumerate() {
            let sibling_pos = if pos % 2 == 0 { pos + 1 } else { pos - 1 };
            let node_idx = encode_node_index(level as u8, sibling_pos);
            nodes_map.insert(node_idx, node_hash.clone());
            pos = pos / 2;
        }

        let leaf_idx = encode_node_index(0, proof_ctx.leaf_index as u64);
        nodes_map.insert(leaf_idx, proof_ctx.hash.clone());
    }

    let mut sorted_nodes: Vec<(u64, Hash)> = nodes_map.into_iter().collect();
    sorted_nodes.sort_by_key(|(idx, _)| *idx);

    let (nodes, node_hashes): (Vec<u64>, Vec<Hash>) = sorted_nodes.into_iter().unzip();
    (nodes, node_hashes)
}

fn compute_indexed_leaf_hash(low_value: &Hash, next_value: &Hash) -> Result<Hash, PhotonApiError> {
    let hashed = Poseidon::hashv(&[&low_value.0, &next_value.0]).map_err(|e| {
        PhotonApiError::UnexpectedError(format!("Failed to hash indexed leaf: {}", e))
    })?;
    Ok(Hash::from(hashed))
}
use light_compressed_account::hash_chain::create_hash_chain_from_slice;
