use light_compressed_account::QueueType;
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, QuerySelect, TransactionTrait,
};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::ingester::persist::get_multiple_compressed_leaf_proofs_by_indices;

const MAX_QUEUE_ELEMENTS: u16 = 4000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsRequest {
    pub tree: Hash,

    pub output_queue_start_index: Option<u64>,
    pub output_queue_limit: Option<u16>,

    pub input_queue_start_index: Option<u64>,
    pub input_queue_limit: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsResponse {
    pub context: Context,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_queue_elements: Option<Vec<GetQueueElementsResponseValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_queue_index: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_queue_elements: Option<Vec<GetQueueElementsResponseValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_queue_index: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsResponseValue {
    pub proof: Vec<Hash>,
    pub root: Hash,
    pub leaf_index: u64,
    pub leaf: Hash,
    pub tree: Hash,
    pub root_seq: u64,
    pub tx_hash: Option<Hash>,
    pub account_hash: Hash,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullifier: Option<Hash>,
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

    if !has_output_request && !has_input_request {
        return Err(PhotonApiError::ValidationError(
            "At least one queue must be requested".to_string(),
        ));
    }

    let context = Context::extract(conn).await?;

    let tx = conn.begin().await?;
    crate::api::set_transaction_isolation_if_needed(&tx).await?;

    let (output_queue_elements, output_first_queue_index) =
        if let Some(limit) = request.output_queue_limit {
            let (elements, first_idx) = fetch_queue(
                &tx,
                &request.tree,
                QueueType::OutputStateV2,
                request.output_queue_start_index,
                limit,
            )
            .await?;
            (Some(elements), Some(first_idx))
        } else {
            (None, None)
        };

    let (input_queue_elements, input_first_queue_index) =
        if let Some(limit) = request.input_queue_limit {
            let (elements, first_idx) = fetch_queue(
                &tx,
                &request.tree,
                QueueType::InputStateV2,
                request.input_queue_start_index,
                limit,
            )
            .await?;
            (Some(elements), Some(first_idx))
        } else {
            (None, None)
        };

    tx.commit().await?;

    Ok(GetQueueElementsResponse {
        context,
        output_queue_elements,
        output_queue_index: output_first_queue_index,
        input_queue_elements,
        input_queue_index: input_first_queue_index,
    })
}

async fn fetch_queue(
    tx: &sea_orm::DatabaseTransaction,
    tree: &Hash,
    queue_type: QueueType,
    start_index: Option<u64>,
    limit: u16,
) -> Result<(Vec<GetQueueElementsResponseValue>, u64), PhotonApiError> {
    if limit > MAX_QUEUE_ELEMENTS {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many queue elements requested {}. Maximum allowed: {}",
            limit, MAX_QUEUE_ELEMENTS
        )));
    }

    let mut query_condition = Condition::all().add(accounts::Column::Tree.eq(tree.to_vec()));

    match queue_type {
        QueueType::InputStateV2 => {
            query_condition = query_condition
                .add(accounts::Column::NullifierQueueIndex.is_not_null())
                .add(accounts::Column::NullifiedInTree.eq(false));
            if let Some(start_queue_index) = start_index {
                query_condition = query_condition
                    .add(accounts::Column::NullifierQueueIndex.gte(start_queue_index as i64));
            }
        }
        QueueType::OutputStateV2 => {
            query_condition = query_condition.add(accounts::Column::InOutputQueue.eq(true));
            if let Some(start_queue_index) = start_index {
                query_condition =
                    query_condition.add(accounts::Column::LeafIndex.gte(start_queue_index as i64));
            }
        }
        _ => {
            return Err(PhotonApiError::ValidationError(format!(
                "Invalid queue type: {:?}",
                queue_type
            )))
        }
    }

    let query = match queue_type {
        QueueType::InputStateV2 => accounts::Entity::find()
            .filter(query_condition)
            .order_by_asc(accounts::Column::NullifierQueueIndex),
        QueueType::OutputStateV2 => accounts::Entity::find()
            .filter(query_condition)
            .order_by_asc(accounts::Column::LeafIndex),
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
        return Ok((vec![], 0));
    }

    let indices: Vec<u64> = queue_elements.iter().map(|e| e.leaf_index as u64).collect();
    let first_value_queue_index = match queue_type {
        QueueType::InputStateV2 => {
            queue_elements[0]
                .nullifier_queue_index
                .ok_or(PhotonApiError::ValidationError(
                    "Nullifier queue index is missing".to_string(),
                ))? as u64
        }
        QueueType::OutputStateV2 => queue_elements[0].leaf_index as u64,
        _ => {
            return Err(PhotonApiError::ValidationError(format!(
                "Invalid queue type: {:?}",
                queue_type
            )))
        }
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

    let result: Vec<GetQueueElementsResponseValue> = generated_proofs
        .into_iter()
        .zip(queue_elements.iter())
        .map(|(proof, queue_element)| {
            let tx_hash = queue_element
                .tx_hash
                .as_ref()
                .map(|tx_hash| Hash::new(tx_hash.as_slice()).unwrap());
            let account_hash = Hash::new(queue_element.hash.as_slice()).unwrap();
            let nullifier = queue_element
                .nullifier
                .as_ref()
                .map(|nullifier| Hash::new(nullifier.as_slice()).unwrap());
            Ok(GetQueueElementsResponseValue {
                proof: proof.proof,
                root: proof.root,
                leaf_index: proof.leaf_index as u64,
                leaf: proof.hash,
                tree: Hash::from(proof.merkle_tree.0.to_bytes()),
                root_seq: proof.root_seq,
                tx_hash,
                account_hash,
                nullifier,
            })
        })
        .collect::<Result<_, PhotonApiError>>()?;

    Ok((result, first_value_queue_index))
}
