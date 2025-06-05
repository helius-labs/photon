mod v2;
pub use v2::{
    get_multiple_compressed_account_proofs_v2, GetMultipleCompressedAccountProofsResponseV2,
};

use super::get_compressed_account_proof::GetCompressedAccountProofResponseValue;
use super::{super::error::PhotonApiError, utils::PAGE_LIMIT};
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::ingester::persist::get_multiple_compressed_leaf_proofs;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleCompressedAccountProofsResponse {
    pub context: Context,
    pub value: Vec<GetCompressedAccountProofResponseValue>,
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
    let tx = conn.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }
    let proofs = get_multiple_compressed_leaf_proofs(&tx, request).await?;
    tx.commit().await?;
    Ok(GetMultipleCompressedAccountProofsResponse {
        value: proofs.into_iter().map(Into::into).collect(),
        context,
    })
}
