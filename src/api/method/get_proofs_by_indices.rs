use log::info;
use crate::ingester::persist::persisted_state_tree::{
    get_multiple_compressed_leaf_proofs, MerkleProofWithContext,
};

use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use crate::common::typedefs::hash::Hash;
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

    let proofs = get_multiple_compressed_leaf_proofs(&tx, None, Some(request.indices)).await?;
    info!("Proofs: {:?}", proofs);

    tx.commit().await?;
    Ok(GetProofsByIndicesResponse {
        value: proofs,
        context,
    })
}
