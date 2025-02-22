use light_merkle_tree_metadata::queue::QueueType;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, FromQueryResult, Statement,
    TransactionTrait,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::api::method::utils::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::persist::{
    bytes_to_sql_format, get_multiple_compressed_leaf_proofs_by_indices,
};

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

#[derive(FromQueryResult)]
struct QueueElement {
    leaf_index: i64,
    hash: Vec<u8>,
    tx_hash: Option<Vec<u8>>,
}

pub async fn get_queue_elements(
    conn: &DatabaseConnection,
    request: GetQueueElementsRequest,
) -> Result<GetQueueElementsResponse, PhotonApiError> {
    let merkle_tree_pubkey_str =
        bytes_to_sql_format(conn.get_database_backend(), request.merkle_tree.to_vec());
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

    let leaf_indices_filter = if let Some(start_offset) = request.start_offset {
        format!("AND leaf_index >= {}", start_offset)
    } else {
        "".to_string()
    };
    let queue_type_filter = match queue_type {
        QueueType::BatchedInput => Ok(
            "AND nullifier_queue_index IS NOT NULL ORDER BY nullifier_queue_index ASC".to_string(),
        ),
        QueueType::BatchedOutput => {
            Ok("AND in_output_queue = TRUE ORDER BY leaf_index ASC".to_string())
        }
        _ => Err(PhotonApiError::ValidationError(format!(
            "Invalid queue type: {:?}",
            queue_type
        ))),
    }?;

    let raw_sql = format!(
        "
        SELECT leaf_index, hash, tx_hash
        FROM accounts
        WHERE tree = {merkle_tree_pubkey_str}
        {leaf_indices_filter}
        {queue_type_filter}
        LIMIT {num_elements}
        ",
    );
    let stmt = Statement::from_string(tx.get_database_backend(), raw_sql);
    let queue_elements = QueueElement::find_by_statement(stmt)
        .all(&tx)
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("DB error fetching queue elements: {}", e))
        })?;

    let indices: Vec<u64> = queue_elements.iter().map(|e| e.leaf_index as u64).collect();

    let proofs = if !indices.is_empty() {
        get_multiple_compressed_leaf_proofs_by_indices(
            &tx,
            SerializablePubkey::from(request.merkle_tree.0),
            indices,
        )
        .await?
    } else {
        vec![]
    };

    tx.commit().await?;

    let result: Vec<MerkleProofWithContextV2> = proofs
        .into_iter()
        .zip(queue_elements.iter())
        .map(|(proof, queue_element)| {
            assert_eq!(
                proof.leafIndex as u64, queue_element.leaf_index as u64,
                "Leaf index mismatch debug."
            );
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
    })
}
