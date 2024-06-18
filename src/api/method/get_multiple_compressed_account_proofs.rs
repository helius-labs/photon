use crate::ingester::persist::persisted_state_tree::{
    get_multiple_compressed_leaf_proofs, MerkleProofWithContext,
};

use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    utils::{Context, PAGE_LIMIT},
};
use crate::common::typedefs::hash::Hash;

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleCompressedAccountProofsResponse {
    pub context: Context,
    pub value: Vec<MerkleProofWithContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HashList(pub Vec<Hash>);

pub async fn get_multiple_compressed_account_proofs(
    conn: &DatabaseConnection,
    request: HashList,
) -> Result<GetMultipleCompressedAccountProofsResponse, PhotonApiError> {
    let request = request.0;
    if request.len() > PAGE_LIMIT as usize {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many hashes requested {}. Maximum allowed: {}",
            request.len(),
            PAGE_LIMIT
        )));
    }
    let context = Context::extract(conn).await?;
    let proofs = get_multiple_compressed_leaf_proofs(conn, request).await?;
    Ok(GetMultipleCompressedAccountProofsResponse {
        value: proofs,
        context,
    })
}
