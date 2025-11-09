use light_compressed_account::QueueType;
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, QuerySelect, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::ingester::persist::get_multiple_compressed_leaf_proofs_by_indices;

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsV2Response {
    pub context: Context,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_queue: Option<OutputQueueDataV2>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_queue: Option<InputQueueDataV2>,
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

    if !has_output_request && !has_input_request {
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

    tx.commit().await?;

    Ok(GetQueueElementsV2Response {
        context,
        output_queue,
        input_queue,
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
