use crate::ingester::persist::get_multiple_compressed_leaf_proofs;
use crate::ingester::persist::persisted_state_tree::MerkleProofWithContext;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    utils::{Context, HashRequest},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountProofResponse {
    pub context: Context,
    pub value: MerkleProofWithContext,
}

pub async fn get_compressed_account_proof(
    conn: &DatabaseConnection,
    request: HashRequest,
) -> Result<GetCompressedAccountProofResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let hash = request.hash;
    let tx = conn.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }
    let res = get_multiple_compressed_leaf_proofs(&tx, vec![hash])
        .await?
        .into_iter()
        .next()
        .map(|account| GetCompressedAccountProofResponse {
            value: account,
            context,
        })
        .ok_or(PhotonApiError::RecordNotFound(
            "Account not found".to_string(),
        ));
    tx.commit().await?;
    res
}
