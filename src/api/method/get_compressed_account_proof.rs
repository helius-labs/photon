use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ingester::persist::persisted_state_tree::{
    get_multiple_compressed_leaf_proofs, MerkleProofWithContext,
};

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
    let res = get_multiple_compressed_leaf_proofs(&tx, Some(vec![hash]), None)
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
