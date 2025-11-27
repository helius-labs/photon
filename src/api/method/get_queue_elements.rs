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
use light_batched_merkle_tree::constants::{
    DEFAULT_ADDRESS_ZKP_BATCH_SIZE, DEFAULT_ZKP_BATCH_SIZE,
};
use light_compressed_account::hash_chain::create_hash_chain_from_slice;
use light_compressed_account::QueueType;
use light_hasher::{Hasher, Poseidon};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, FromQueryResult,
    QueryFilter, QueryOrder, QuerySelect, Statement, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use utoipa::ToSchema;

const MAX_QUEUE_ELEMENTS: u16 = 30_000;

/// Encode tree node position as a single u64
/// Format: [level: u8][position: 56 bits]
/// Level 0 = leaves, Level (tree_height-1) = root
#[inline]
fn encode_node_index(level: u8, position: u64, tree_height: u8) -> u64 {
    debug_assert!(
        level < tree_height,
        "level {} >= tree_height {}",
        level,
        tree_height
    );
    ((level as u64) << 56) | position
}

struct StateQueueProofData {
    proofs: Vec<crate::ingester::persist::MerkleProofWithContext>,
    tree_height: u8,
}

enum QueueData {
    Output(OutputQueueData, Option<StateQueueProofData>),
    Input(InputQueueData, Option<StateQueueProofData>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsRequest {
    pub tree: Hash,

    pub output_queue_start_index: Option<u64>,
    pub output_queue_limit: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_queue_zkp_batch_size: Option<u16>,

    pub input_queue_start_index: Option<u64>,
    pub input_queue_limit: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_queue_zkp_batch_size: Option<u16>,

    pub address_queue_start_index: Option<u64>,
    pub address_queue_limit: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub address_queue_zkp_batch_size: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsResponse {
    pub context: Context,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_queue: Option<StateQueueData>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub address_queue: Option<AddressQueueData>,
}

/// State queue data with shared tree nodes for output and input queues
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StateQueueData {
    /// Shared deduplicated tree nodes for state queues (output + input)
    /// node_index encoding: (level << 56) | position
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub nodes: Vec<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub node_hashes: Vec<Hash>,
    /// Initial root for the state tree (shared by output and input queues)
    pub initial_root: Hash,
    /// Sequence number of the root
    pub root_seq: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_queue: Option<OutputQueueData>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_queue: Option<InputQueueData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OutputQueueData {
    pub leaf_indices: Vec<u64>,
    pub account_hashes: Vec<Hash>,
    pub leaves: Vec<Hash>,
    pub first_queue_index: u64,
    pub next_index: u64,
    pub leaves_hash_chains: Vec<Hash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InputQueueData {
    pub leaf_indices: Vec<u64>,
    pub account_hashes: Vec<Hash>,
    pub leaves: Vec<Hash>,
    pub tx_hashes: Vec<Hash>,
    pub nullifiers: Vec<Hash>,
    pub first_queue_index: u64,
    pub leaves_hash_chains: Vec<Hash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AddressQueueData {
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
    pub root_seq: u64,
}

#[derive(FromQueryResult, Debug)]
struct QueueElement {
    leaf_index: i64,
    hash: Vec<u8>,
    tx_hash: Option<Vec<u8>>,
    nullifier_queue_index: Option<i64>,
    nullifier: Option<Vec<u8>>,
}

pub async fn get_queue_elements(
    conn: &DatabaseConnection,
    request: GetQueueElementsRequest,
) -> Result<GetQueueElementsResponse, PhotonApiError> {
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

    // Fetch output and input queues with their proof data
    let (output_queue, output_proof_data) = if let Some(limit) = request.output_queue_limit {
        let zkp_hint = request.output_queue_zkp_batch_size;
        match fetch_queue(
            &tx,
            &request.tree,
            QueueType::OutputStateV2,
            request.output_queue_start_index,
            limit,
            zkp_hint,
        )
        .await?
        {
            QueueData::Output(data, proof_data) => (Some(data), proof_data),
            QueueData::Input(_, _) => unreachable!("OutputState should return Output"),
        }
    } else {
        (None, None)
    };

    let (input_queue, input_proof_data) = if let Some(limit) = request.input_queue_limit {
        let zkp_hint = request.input_queue_zkp_batch_size;
        match fetch_queue(
            &tx,
            &request.tree,
            QueueType::InputStateV2,
            request.input_queue_start_index,
            limit,
            zkp_hint,
        )
        .await?
        {
            QueueData::Input(data, proof_data) => (Some(data), proof_data),
            QueueData::Output(_, _) => unreachable!("InputState should return Input"),
        }
    } else {
        (None, None)
    };

    let state_queue = if has_output_request || has_input_request {
        let (nodes, node_hashes, initial_root, root_seq) =
            merge_state_queue_proofs(&output_proof_data, &input_proof_data)?;

        Some(StateQueueData {
            nodes,
            node_hashes,
            initial_root,
            root_seq,
            output_queue,
            input_queue,
        })
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

    Ok(GetQueueElementsResponse {
        context,
        state_queue,
        address_queue,
    })
}

fn merge_state_queue_proofs(
    output_proof_data: &Option<StateQueueProofData>,
    input_proof_data: &Option<StateQueueProofData>,
) -> Result<(Vec<u64>, Vec<Hash>, Hash, u64), PhotonApiError> {
    let mut all_proofs: Vec<&crate::ingester::persist::MerkleProofWithContext> = Vec::new();
    let mut tree_height: Option<u8> = None;
    let mut initial_root: Option<Hash> = None;
    let mut root_seq: Option<u64> = None;

    // Collect proofs from output queue
    if let Some(ref proof_data) = output_proof_data {
        tree_height = Some(proof_data.tree_height);
        for proof in &proof_data.proofs {
            if initial_root.is_none() {
                initial_root = Some(proof.root.clone());
                root_seq = Some(proof.root_seq);
            }
            all_proofs.push(proof);
        }
    }

    // Collect proofs from input queue
    if let Some(ref proof_data) = input_proof_data {
        if tree_height.is_none() {
            tree_height = Some(proof_data.tree_height);
        }
        for proof in &proof_data.proofs {
            if initial_root.is_none() {
                initial_root = Some(proof.root.clone());
                root_seq = Some(proof.root_seq);
            }
            all_proofs.push(proof);
        }
    }

    if all_proofs.is_empty() || tree_height.is_none() {
        return Ok((Vec::new(), Vec::new(), Hash::default(), 0));
    }

    let height = tree_height.unwrap();
    let (nodes, node_hashes) = deduplicate_nodes_from_refs(&all_proofs, height);

    Ok((
        nodes,
        node_hashes,
        initial_root.unwrap_or_default(),
        root_seq.unwrap_or_default(),
    ))
}

async fn fetch_queue(
    tx: &sea_orm::DatabaseTransaction,
    tree: &Hash,
    queue_type: QueueType,
    start_index: Option<u64>,
    limit: u16,
    zkp_batch_size_hint: Option<u16>,
) -> Result<QueueData, PhotonApiError> {
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
                .add(accounts::Column::NullifiedInTree.eq(false))
                .add(accounts::Column::Spent.eq(true));
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

    let mut queue_elements: Vec<QueueElement> = query
        .limit(limit as u64)
        .into_model::<QueueElement>()
        .all(tx)
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("DB error fetching queue elements: {}", e))
        })?;

    if queue_elements.is_empty() {
        return Ok(match queue_type {
            QueueType::OutputStateV2 => QueueData::Output(OutputQueueData::default(), None),
            QueueType::InputStateV2 => QueueData::Input(InputQueueData::default(), None),
            _ => unreachable!("Only OutputState and InputState are supported"),
        });
    }

    let mut indices: Vec<u64> = queue_elements.iter().map(|e| e.leaf_index as u64).collect();
    let first_queue_index = match queue_type {
        QueueType::InputStateV2 => {
            queue_elements[0]
                .nullifier_queue_index
                .ok_or(PhotonApiError::ValidationError(
                    "Nullifier queue index is missing".to_string(),
                ))? as u64
        }
        QueueType::OutputStateV2 => queue_elements[0].leaf_index as u64,
        _ => unreachable!("Only OutputState and InputState are supported"),
    };
    if let Some(start) = start_index {
        if first_queue_index > start {
            return Err(PhotonApiError::ValidationError(format!(
                "Requested start_index {} but first_queue_index {} is later (possible pruning)",
                start, first_queue_index
            )));
        }
    }

    let serializable_tree = SerializablePubkey::from(tree.0);

    let tree_info = TreeInfo::get(tx, &serializable_tree.to_string())
        .await?
        .ok_or_else(|| PhotonApiError::UnexpectedError("Failed to get tree info".to_string()))?;

    // For output queue, next_index is where the elements will be appended.
    // This is the minimum leaf_index of the queued elements (first_queue_index).
    // We cannot use tree_metadata.next_index because it's only updated by the monitor,
    // not by the ingester when processing batch events.
    let next_index = if queue_type == QueueType::OutputStateV2 {
        first_queue_index
    } else {
        0
    };

    let zkp_batch_size = zkp_batch_size_hint
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_ZKP_BATCH_SIZE as u16) as usize;
    if zkp_batch_size > 0 {
        let full_batches = indices.len() / zkp_batch_size;
        let allowed = full_batches * zkp_batch_size;
        if allowed == 0 {
            return Ok(match queue_type {
                QueueType::OutputStateV2 => QueueData::Output(OutputQueueData::default(), None),
                QueueType::InputStateV2 => QueueData::Input(InputQueueData::default(), None),
                _ => unreachable!("Only OutputState and InputState are supported"),
            });
        }
        if indices.len() > allowed {
            indices.truncate(allowed);
            queue_elements.truncate(allowed);
        }
    }

    let generated_proofs =
        get_multiple_compressed_leaf_proofs_by_indices(tx, serializable_tree, indices.clone())
            .await?;

    if generated_proofs.len() != indices.len() {
        return Err(PhotonApiError::ValidationError(format!(
            "Expected {} proofs for {} queue elements, but got {} proofs",
            indices.len(),
            queue_elements.len(),
            generated_proofs.len()
        )));
    }

    // Return proofs for merging at response level
    let proof_data = Some(StateQueueProofData {
        proofs: generated_proofs.clone(),
        tree_height: tree_info.height as u8,
    });

    let leaf_indices = indices.clone();
    let account_hashes: Vec<Hash> = queue_elements
        .iter()
        .map(|e| Hash::new(e.hash.as_slice()).unwrap())
        .collect();
    let leaves: Vec<Hash> = generated_proofs.iter().map(|p| p.hash.clone()).collect();

    let tree_pubkey_bytes: [u8; 32] = serializable_tree
        .to_bytes_vec()
        .as_slice()
        .try_into()
        .map_err(|_| PhotonApiError::UnexpectedError("Invalid tree pubkey bytes".to_string()))?;
    let tree_pubkey = Pubkey::new_from_array(tree_pubkey_bytes);

    let batch_start_index = first_queue_index;
    let cached =
        queue_hash_cache::get_cached_hash_chains(tx, tree_pubkey, queue_type, batch_start_index)
            .await
            .map_err(|e| PhotonApiError::UnexpectedError(format!("Cache error: {}", e)))?;

    let expected_batch_count = indices.len() / zkp_batch_size;
    let leaves_hash_chains = if !cached.is_empty() && cached.len() >= expected_batch_count {
        let mut sorted = cached;
        sorted.sort_by_key(|c| c.zkp_batch_index);
        sorted
            .into_iter()
            .take(expected_batch_count)
            .map(|entry| Hash::from(entry.hash_chain))
            .collect()
    } else {
        // Fall back to computing locally if cache is empty (e.g., monitor hasn't run yet)
        log::warn!(
            "No cached hash chains for {:?} queue (batch_start_index={}, cached={}, expected={})",
            queue_type,
            batch_start_index,
            cached.len(),
            expected_batch_count
        );
        compute_state_queue_hash_chains(&queue_elements, queue_type, zkp_batch_size)?
    };

    Ok(match queue_type {
        QueueType::OutputStateV2 => QueueData::Output(
            OutputQueueData {
                leaf_indices,
                account_hashes,
                leaves,
                first_queue_index,
                next_index,
                leaves_hash_chains,
            },
            proof_data,
        ),
        QueueType::InputStateV2 => {
            let tx_hashes: Result<Vec<Hash>, PhotonApiError> = queue_elements
                .iter()
                .enumerate()
                .map(|(idx, e)| {
                    e.tx_hash
                        .as_ref()
                        .ok_or_else(|| {
                            PhotonApiError::UnexpectedError(format!(
                                "Missing tx_hash for spent queue element at index {} (leaf_index={})",
                                idx, e.leaf_index
                            ))
                        })
                        .and_then(|tx| {
                            Hash::new(tx.as_slice()).map_err(|e| {
                                PhotonApiError::UnexpectedError(format!("Invalid tx_hash: {}", e))
                            })
                        })
                })
                .collect();

            let nullifiers: Result<Vec<Hash>, PhotonApiError> = queue_elements
                .iter()
                .enumerate()
                .map(|(idx, e)| {
                    e.nullifier
                        .as_ref()
                        .ok_or_else(|| {
                            PhotonApiError::UnexpectedError(format!(
                                "Missing nullifier for spent queue element at index {} (leaf_index={})",
                                idx, e.leaf_index
                            ))
                        })
                        .and_then(|n| {
                            Hash::new(n.as_slice()).map_err(|e| {
                                PhotonApiError::UnexpectedError(format!("Invalid nullifier: {}", e))
                            })
                        })
                })
                .collect();

            QueueData::Input(
                InputQueueData {
                    leaf_indices,
                    account_hashes,
                    leaves,
                    tx_hashes: tx_hashes?,
                    nullifiers: nullifiers?,
                    first_queue_index,
                    leaves_hash_chains,
                },
                proof_data,
            )
        }
        _ => unreachable!("Only OutputState and InputState are supported"),
    })
}

fn compute_state_queue_hash_chains(
    queue_elements: &[QueueElement],
    queue_type: QueueType,
    zkp_batch_size: usize,
) -> Result<Vec<Hash>, PhotonApiError> {
    use light_compressed_account::hash_chain::create_hash_chain_from_slice;

    if zkp_batch_size == 0 || queue_elements.is_empty() {
        return Ok(Vec::new());
    }

    let batch_count = queue_elements.len() / zkp_batch_size;
    if batch_count == 0 {
        return Ok(Vec::new());
    }

    let mut hash_chains = Vec::with_capacity(batch_count);

    for batch_idx in 0..batch_count {
        let start = batch_idx * zkp_batch_size;
        let end = start + zkp_batch_size;
        let batch_elements = &queue_elements[start..end];

        let mut values: Vec<[u8; 32]> = Vec::with_capacity(zkp_batch_size);

        for element in batch_elements {
            let value: [u8; 32] = match queue_type {
                QueueType::OutputStateV2 => element.hash.as_slice().try_into().map_err(|_| {
                    PhotonApiError::UnexpectedError(format!(
                        "Invalid hash length: expected 32 bytes, got {}",
                        element.hash.len()
                    ))
                })?,
                QueueType::InputStateV2 => element
                    .nullifier
                    .as_ref()
                    .ok_or_else(|| {
                        PhotonApiError::UnexpectedError(
                            "Missing nullifier for InputState queue element".to_string(),
                        )
                    })?
                    .as_slice()
                    .try_into()
                    .map_err(|_| {
                        PhotonApiError::UnexpectedError(
                            "Invalid nullifier length: expected 32 bytes".to_string(),
                        )
                    })?,
                _ => {
                    return Err(PhotonApiError::ValidationError(format!(
                        "Unsupported queue type for hash chain computation: {:?}",
                        queue_type
                    )))
                }
            };
            values.push(value);
        }

        let hash_chain = create_hash_chain_from_slice(&values).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Hash chain computation error: {}", e))
        })?;

        hash_chains.push(Hash::from(hash_chain));
    }

    log::debug!(
        "Computed {} hash chains for {:?} queue with {} elements (zkp_batch_size={})",
        hash_chains.len(),
        queue_type,
        queue_elements.len(),
        zkp_batch_size
    );

    Ok(hash_chains)
}

async fn fetch_address_queue_v2(
    tx: &sea_orm::DatabaseTransaction,
    tree: &Hash,
    start_queue_index: Option<u64>,
    limit: u16,
    zkp_batch_size: u16,
) -> Result<AddressQueueData, PhotonApiError> {
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
        return Ok(AddressQueueData {
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
            let node_idx = encode_node_index(level as u8, sibling_pos, tree_info.height as u8);
            nodes_map.insert(node_idx, node_hash.clone());
            pos /= 2;
        }

        let leaf_idx =
            encode_node_index(0, proof.lowElementLeafIndex as u64, tree_info.height as u8);
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
    let root_seq = non_inclusion_proofs
        .first()
        .map(|proof| proof.rootSeq)
        .unwrap_or_default();

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

    let expected_batch_count = if !addresses.is_empty() && zkp_batch_size > 0 {
        addresses.len() / zkp_batch_size as usize
    } else {
        0
    };

    log::debug!(
        "Address queue hash chain cache: batch_start_index={}, cached_count={}, expected_count={}, addresses={}, zkp_batch_size={}",
        batch_start_index,
        cached.len(),
        expected_batch_count,
        addresses.len(),
        zkp_batch_size
    );

    if !cached.is_empty() && cached.len() >= expected_batch_count {
        log::debug!(
            "Using {} cached hash chains for batch_start_index={}",
            cached.len(),
            batch_start_index
        );
        let mut sorted = cached;
        sorted.sort_by_key(|c| c.zkp_batch_index);
        for entry in sorted {
            leaves_hash_chains.push(Hash::from(entry.hash_chain));
        }
    } else if !addresses.is_empty() {
        if cached.is_empty() {
            log::debug!(
                "No cached hash chains found, creating {} new chains for batch_start_index={}",
                expected_batch_count,
                batch_start_index
            );
        }
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
                    let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
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

    Ok(AddressQueueData {
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
        root_seq,
    })
}

/// Deduplicate nodes across all merkle proofs (takes references to proofs)
/// Returns parallel arrays: (node_indices, node_hashes)
fn deduplicate_nodes_from_refs(
    proofs: &[&crate::ingester::persist::MerkleProofWithContext],
    tree_height: u8,
) -> (Vec<u64>, Vec<Hash>) {
    let mut nodes_map: HashMap<u64, Hash> = HashMap::new();

    for proof_ctx in proofs {
        let mut pos = proof_ctx.leaf_index as u64;
        let mut current_hash = proof_ctx.hash.clone();

        // Store the leaf itself
        let leaf_idx = encode_node_index(0, pos, tree_height);
        nodes_map.insert(leaf_idx, current_hash.clone());

        // Walk up the proof path, storing BOTH the sibling AND the current node at each level
        for (level, sibling_hash) in proof_ctx.proof.iter().enumerate() {
            let sibling_pos = if pos.is_multiple_of(2) { pos + 1 } else { pos - 1 };

            // Store the sibling (from proof)
            let sibling_idx = encode_node_index(level as u8, sibling_pos, tree_height);
            nodes_map.insert(sibling_idx, sibling_hash.clone());

            // Compute and store the parent node on the path
            // This allows MerkleTree::update_upper_layers() to read both children
            let parent_hash = if pos.is_multiple_of(2) {
                // Current is left, sibling is right
                Poseidon::hashv(&[&current_hash.0, &sibling_hash.0])
            } else {
                // Sibling is left, current is right
                Poseidon::hashv(&[&sibling_hash.0, &current_hash.0])
            };

            match parent_hash {
                Ok(hash) => {
                    current_hash = Hash::from(hash);
                    // Store the parent at the next level
                    let parent_pos = pos / 2;
                    let parent_idx = encode_node_index((level + 1) as u8, parent_pos, tree_height);
                    nodes_map.insert(parent_idx, current_hash.clone());
                }
                Err(_) => {
                    // If hash fails, we can't compute parent, stop here
                    break;
                }
            }

            pos /= 2;
        }
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
