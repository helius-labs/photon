use log::info;
use crate::ingester::persist::persisted_state_tree::{get_multiple_compressed_leaf_proofs_by_indices, MerkleProofWithContext};

use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use super::{
    super::error::PhotonApiError,
    utils::{Context, PAGE_LIMIT},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetProofsByIndicesRequest {
    pub merkle_tree: Hash,
    pub indices: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetProofsByIndicesResponse {
    pub context: Context,
    pub value: Vec<MerkleProofWithContext>,
}

pub async fn get_proofs_by_indices(
    conn: &DatabaseConnection,
    request: GetProofsByIndicesRequest,
) -> Result<GetProofsByIndicesResponse, PhotonApiError> {
    info!(
        "Getting proofs for tree {} for indices {:?}",
        request.merkle_tree, request.indices
    );
    if request.indices.len() > PAGE_LIMIT as usize {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many hashes requested {}. Maximum allowed: {}",
            request.indices.len(),
            PAGE_LIMIT
        )));
    }

    let context = Context::extract(conn).await?;
    let tx = conn.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
            .await?;
    }

    let proofs = get_multiple_compressed_leaf_proofs_by_indices(
        &tx,
        SerializablePubkey::from(request.merkle_tree.0),
        request.indices
    ).await?;
    for proof in &proofs {
        info!(
            "Proof for index {}: {:?}",
            proof.leafIndex, proof.proof.iter().map(|x| x.to_vec()).collect::<Vec<_>>()
        );

        // TODO: remove
        // match validate_proof(proof) {
        //     Ok(_) => info!("Proof for index {} is valid", proof.leafIndex),
        //     Err(e) => {
        //         return Err(PhotonApiError::UnexpectedError(format!(
        //             "Proof for index {} is invalid: {}",
        //             proof.leafIndex, e
        //         )))
        //     }
        // }
    }

    tx.commit().await?;
    Ok(GetProofsByIndicesResponse {
        value: proofs,
        context,
    })
}
