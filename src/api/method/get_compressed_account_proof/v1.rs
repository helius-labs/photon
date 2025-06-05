use crate::api::error::PhotonApiError;
use crate::api::method::utils::HashRequest;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::persist::{get_multiple_compressed_leaf_proofs, MerkleProofWithContext};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountProofResponse {
    pub context: Context,
    pub value: GetCompressedAccountProofResponseValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct GetCompressedAccountProofResponseValue {
    pub proof: Vec<Hash>,
    pub root: Hash,
    pub leaf_index: u32,
    pub hash: Hash,
    pub merkle_tree: SerializablePubkey,
    pub root_seq: u64,
}

impl From<MerkleProofWithContext> for GetCompressedAccountProofResponseValue {
    fn from(proof: MerkleProofWithContext) -> Self {
        GetCompressedAccountProofResponseValue {
            proof: proof.proof,
            root: proof.root,
            leaf_index: proof.leaf_index,
            hash: proof.hash,
            merkle_tree: proof.merkle_tree,
            root_seq: proof.root_seq,
        }
    }
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
            value: account.into(),
            context,
        })
        .ok_or(PhotonApiError::RecordNotFound(
            "Account not found".to_string(),
        ));
    tx.commit().await?;
    res
}
