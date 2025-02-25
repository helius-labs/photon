use light_merkle_tree_metadata::queue::QueueType;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait,
    FromQueryResult, QueryFilter, QueryOrder, QuerySelect, QueryTrait, Statement, TransactionTrait,
};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::api::method::utils::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::ingester::persist::get_multiple_compressed_leaf_proofs_by_indices;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsRequest {
    pub merkle_tree: Hash,
    pub start_offset: Option<u64>,
    pub num_elements: u16,
    pub queue_type: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsResponse {
    pub context: Context,
    pub value: Vec<MerkleProofWithContextV2>,
    pub first_value_queue_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct MerkleProofWithContextV2 {
    pub proof: Vec<Hash>,
    pub root: Hash,
    pub leaf_index: u64,
    pub leaf: Hash,
    pub merkle_tree: Hash,
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
        Condition::all().add(accounts::Column::Tree.eq(request.merkle_tree.to_vec()));

    match queue_type {
        QueueType::BatchedInput => {
            query_condition =
                query_condition.add(accounts::Column::NullifierQueueIndex.is_not_null());
            if let Some(start_offset) = request.start_offset {
                query_condition = query_condition
                    .add(accounts::Column::NullifierQueueIndex.gte(start_offset as i64));
            }
        }
        QueueType::BatchedOutput => {
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
        QueueType::BatchedInput => accounts::Entity::find()
            .filter(query_condition)
            .order_by_asc(accounts::Column::NullifierQueueIndex),
        QueueType::BatchedOutput => accounts::Entity::find()
            .filter(query_condition)
            .order_by_asc(accounts::Column::LeafIndex),
        _ => {
            return Err(PhotonApiError::ValidationError(format!(
                "Invalid queue type: {:?}",
                queue_type
            )))
        }
    };

    let sql = query.build(conn.get_database_backend()).sql;
    let values = query.build(conn.get_database_backend()).values;
    println!("sql: {:?}", sql);
    println!("values: {:?}", values);

    let queue_elements: Vec<_> = query.clone().all(&tx).await?;
    println!("0 queue_elements: {:?}", queue_elements);

    let queue_elements: Vec<QueueElement> = query
        .limit(num_elements as u64)
        .into_model::<QueueElement>()
        .all(&tx)
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("DB error fetching queue elements: {}", e))
        })?;

    println!("queue_elements: {:?}", queue_elements);

    let indices: Vec<u64> = queue_elements.iter().map(|e| e.leaf_index as u64).collect();

    println!("indices: {:?}", indices);

    let (proofs, first_value_queue_index) = if !indices.is_empty() {
        let first_value_queue_index = match queue_type {
            QueueType::BatchedInput => Ok(queue_elements[0].nullifier_queue_index.ok_or(
                PhotonApiError::ValidationError("Nullifier queue index is missing".to_string()),
            )? as u64),
            QueueType::BatchedOutput => Ok(queue_elements[0].leaf_index as u64),
            _ => Err(PhotonApiError::ValidationError(format!(
                "Invalid queue type: {:?}",
                queue_type
            ))),
        }?;
        (
            get_multiple_compressed_leaf_proofs_by_indices(
                &tx,
                SerializablePubkey::from(request.merkle_tree.0),
                indices,
            )
            .await?,
            first_value_queue_index,
        )
    } else {
        (vec![], 0)
    };

    tx.commit().await?;

    let result: Vec<MerkleProofWithContextV2> = proofs
        .into_iter()
        .zip(queue_elements.iter())
        .map(|(proof, queue_element)| {
            let tx_hash = queue_element
                .tx_hash
                .as_ref()
                .map(|tx_hash| Hash::new(tx_hash.as_slice()).unwrap());
            let account_hash = Hash::new(queue_element.hash.as_slice()).unwrap();
            Ok(MerkleProofWithContextV2 {
                proof: proof.proof,
                root: proof.root,
                leaf_index: proof.leafIndex as u64,
                leaf: proof.hash,
                merkle_tree: Hash::from(proof.merkleTree.0.to_bytes()),
                root_seq: proof.rootSeq,
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
