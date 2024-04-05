use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    get_multiple_compressed_account_proofs::{
        get_multiple_compressed_account_proofs_helper, MerkleProofWithContext,
    },
    utils::{Context, ResponseWithContext},
};
use crate::dao::typedefs::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HashRequest(Hash);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// We do not use generics to simplify documentation generation.
pub struct GetCompressedAccountProofResponse {
    pub context: Context,
    pub value: MerkleProofWithContext,
}

pub async fn get_compressed_account_proof(
    conn: &DatabaseConnection,
    request: HashRequest,
) -> Result<GetCompressedAccountProofResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let hash = request.0;

    get_multiple_compressed_account_proofs_helper(conn, vec![hash])
        .await?
        .into_iter()
        .next()
        .map(|account| GetCompressedAccountProofResponse {
            value: account,
            context,
        })
        .ok_or(PhotonApiError::RecordNotFound(
            "Account not found".to_string(),
        ))
}
