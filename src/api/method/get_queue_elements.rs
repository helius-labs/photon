use light_compressed_account::QueueType;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait,
    FromQueryResult, QueryFilter, QueryOrder, QuerySelect, Statement, TransactionTrait,
};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::ingester::persist::get_multiple_compressed_leaf_proofs_by_indices;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsRequest {
    pub tree: Hash,
    pub start_offset: Option<u64>,
    pub num_elements: u16,
    pub queue_type: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsResponse {
    pub context: Context,
    pub value: Vec<GetQueueElementsResponseValue>,
    pub first_value_queue_index: u64,
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
}

#[derive(FromQueryResult, Debug)]
struct QueueElement {
    leaf_index: i64,
    hash: Vec<u8>,
    tx_hash: Option<Vec<u8>>,
    nullifier_queue_index: Option<i64>,
}

pub async fn get_queue_elements(
    conn: &DatabaseConnection,
    request: GetQueueElementsRequest,
) -> Result<GetQueueElementsResponse, PhotonApiError> {
    let queue_type = QueueType::from(request.queue_type as u64);
    let num_elements = request.num_elements;
    let context = Context::extract(conn).await?;
    let tx = conn.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }

    let mut query_condition =
        Condition::all().add(accounts::Column::Tree.eq(request.tree.to_vec()));

    match queue_type {
        QueueType::InputStateV2 => {
            query_condition =
                query_condition.add(accounts::Column::NullifierQueueIndex.is_not_null());
            if let Some(start_offset) = request.start_offset {
                query_condition = query_condition
                    .add(accounts::Column::NullifierQueueIndex.gte(start_offset as i64));
            }
        }
        QueueType::OutputStateV2 => {
            query_condition = query_condition.add(accounts::Column::InOutputQueue.eq(true));
            if let Some(start_offset) = request.start_offset {
                query_condition =
                    query_condition.add(accounts::Column::LeafIndex.gte(start_offset as i64));
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
        .limit(num_elements as u64)
        .into_model::<QueueElement>()
        .all(&tx)
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("DB error fetching queue elements: {}", e))
        })?;
    let indices: Vec<u64> = queue_elements.iter().map(|e| e.leaf_index as u64).collect();
    let (proofs, first_value_queue_index) = if !indices.is_empty() {
        let first_value_queue_index = match queue_type {
            QueueType::InputStateV2 => Ok(queue_elements[0].nullifier_queue_index.ok_or(
                PhotonApiError::ValidationError("Nullifier queue index is missing".to_string()),
            )? as u64),
            QueueType::OutputStateV2 => Ok(queue_elements[0].leaf_index as u64),
            _ => Err(PhotonApiError::ValidationError(format!(
                "Invalid queue type: {:?}",
                queue_type
            ))),
        }?;
        (
            get_multiple_compressed_leaf_proofs_by_indices(
                &tx,
                SerializablePubkey::from(request.tree.0),
                indices,
            )
            .await?,
            first_value_queue_index,
        )
    } else {
        (vec![], 0)
    };

    tx.commit().await?;

    let result: Vec<GetQueueElementsResponseValue> = proofs
        .into_iter()
        .zip(queue_elements.iter())
        .map(|(proof, queue_element)| {
            let tx_hash = queue_element
                .tx_hash
                .as_ref()
                .map(|tx_hash| Hash::new(tx_hash.as_slice()).unwrap());
            let account_hash = Hash::new(queue_element.hash.as_slice()).unwrap();
            Ok(GetQueueElementsResponseValue {
                proof: proof.proof,
                root: proof.root,
                leaf_index: proof.leaf_index as u64,
                leaf: proof.hash,
                tree: Hash::from(proof.merkle_tree.0.to_bytes()),
                root_seq: proof.root_seq,
                tx_hash,
                account_hash,
            })
        })
        .collect::<Result<_, PhotonApiError>>()?;

    Ok(GetQueueElementsResponse {
        context,
        value: result,
        first_value_queue_index,
    })
}
