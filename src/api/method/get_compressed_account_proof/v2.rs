use crate::api::error::PhotonApiError;
use crate::api::method::get_validity_proof::TreeContextInfo;
use crate::api::method::utils::HashRequest;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::{accounts, state_trees};
use crate::ingester::persist::{
    get_multiple_compressed_leaf_proofs, get_multiple_compressed_leaf_proofs_by_indices,
    MerkleProofWithContext,
};
use jsonrpsee_core::Serialize;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    Statement, TransactionTrait,
};
use serde::Deserialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountProofResponseV2 {
    pub context: Context,
    pub value: GetCompressedAccountProofResponseValueV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountProofResponseValueV2 {
    pub proof: Vec<Hash>,
    pub root: Hash,
    pub leaf_index: u32,
    pub hash: Hash,
    pub root_seq: u64,
    pub prove_by_index: bool,
    pub tree_context: TreeContextInfo,
}

impl From<MerkleProofWithContext> for GetCompressedAccountProofResponseValueV2 {
    fn from(proof: MerkleProofWithContext) -> Self {
        GetCompressedAccountProofResponseValueV2 {
            proof: proof.proof,
            root: proof.root,
            leaf_index: proof.leaf_index,
            hash: proof.hash,
            root_seq: proof.root_seq,
            prove_by_index: false,
            // Default values to be overridden as needed
            tree_context: TreeContextInfo {
                tree_type: 0,
                tree: proof.merkle_tree,
                queue: Default::default(),
                cpi_context: None,
            },
        }
    }
}

pub async fn get_compressed_account_proof_v2(
    conn: &DatabaseConnection,
    request: HashRequest,
) -> Result<GetCompressedAccountProofResponseV2, PhotonApiError> {
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

    let account = accounts::Entity::find()
        .filter(accounts::Column::Hash.eq(hash.to_vec()))
        .one(&tx)
        .await?;

    if account.is_none() {
        return Err(PhotonApiError::RecordNotFound(
            "Account not found".to_string(),
        ));
    }

    let leaf_node = state_trees::Entity::find()
        .filter(
            state_trees::Column::Hash
                .eq(hash.to_vec())
                .and(state_trees::Column::Level.eq(0)),
        )
        .one(&tx)
        .await?;

    // Determine how to generate the proof based on available data
    let mut result: GetCompressedAccountProofResponseValueV2 = if leaf_node.is_some() {
        let mut response: GetCompressedAccountProofResponseValueV2 =
            get_multiple_compressed_leaf_proofs(&tx, vec![hash])
                .await?
                .into_iter()
                .next()
                .ok_or(PhotonApiError::RecordNotFound(
                    "Account not found by hash".to_string(),
                ))?
                .into();
        response.prove_by_index = false;
        response
    } else if let Some(account) = account.clone() {
        // Use index-based proof if we found the account in a queue but not in state_trees
        let leaf_index = account.leaf_index as u64;
        let merkle_tree = SerializablePubkey::try_from(account.tree.clone())?;
        let mut response: GetCompressedAccountProofResponseValueV2 =
            get_multiple_compressed_leaf_proofs_by_indices(&tx, merkle_tree, vec![leaf_index])
                .await?
                .into_iter()
                .next()
                .ok_or(PhotonApiError::RecordNotFound(
                    "Account not found by index".to_string(),
                ))?
                .into();
        response.prove_by_index = true;
        response
    } else {
        return Err(PhotonApiError::RecordNotFound(
            "Account not found".to_string(),
        ));
    };

    // Enrich with account data if available
    if let Some(account) = account {
        result.tree_context.tree_type = account.tree_type as u16;
        result.tree_context.queue = SerializablePubkey::try_from(account.queue)?;
    }

    let response = GetCompressedAccountProofResponseV2 {
        value: result,
        context,
    };

    tx.commit().await?;
    Ok(response)
}
