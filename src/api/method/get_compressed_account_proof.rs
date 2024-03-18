use crate::dao::generated::{state_trees, utxos};
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};
use serde::{Deserialize, Serialize};

use super::{
    super::error::PhotonApiError,
    utils::{AccountIdentifier, CompressedAccountRequest, Context, ResponseWithContext},
};
use crate::dao::typedefs::hash::Hash;

use super::utils::{build_full_proof, get_proof_path, ProofResponse};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountProof {
    pub hash: Hash,
    pub root: Hash,
    pub proof: Vec<Hash>,
}

pub type GetCompressedAccountProofResponse = ResponseWithContext<AccountProof>;

// TODO: Optimize the DB queries to reduce latency.
// We make three calls when account_id but we only need to make two.
pub async fn get_compressed_account_proof(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<GetCompressedAccountProofResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let id = request.get_id()?;
    // Extract the leaf hash from the user or look it up via the provided account_id.

    let leaf_hash = match id {
        AccountIdentifier::Address(address) => Hash::try_from(
            utxos::Entity::find()
                .filter(utxos::Column::Account.eq::<Vec<u8>>(address.clone().into()))
                .one(conn)
                .await?
                .ok_or(PhotonApiError::RecordNotFound(format!(
                    "Account {} not found",
                    address
                )))?
                .hash,
        )?,
        AccountIdentifier::Hash(hash) => hash,
    };

    let leaf_node = state_trees::Entity::find()
        .filter(
            state_trees::Column::Hash
                .eq(leaf_hash.to_vec())
                .and(state_trees::Column::Level.eq(0)),
        )
        .one(conn)
        .await?
        .ok_or(PhotonApiError::RecordNotFound(format!(
            "Leaf node not found for hash {}",
            leaf_hash
        )))?;

    let tree = leaf_node.tree;
    let required_node_indices = get_proof_path(leaf_node.node_idx);

    // Proofs are served from leaf to root.
    // Therefore we sort by level (ascending).
    let proof_nodes = state_trees::Entity::find()
        .filter(
            state_trees::Column::Tree
                .eq(tree)
                .and(state_trees::Column::NodeIdx.is_in(required_node_indices.clone())),
        )
        .order_by_asc(state_trees::Column::Level)
        .all(conn)
        .await?;

    let ProofResponse { root, proof } = build_full_proof(proof_nodes, required_node_indices)?;
    let account_proof = AccountProof {
        hash: Hash::try_from(leaf_node.hash)?.into(),
        root,
        proof,
    };
    Ok(GetCompressedAccountProofResponse {
        value: account_proof,
        context,
    })
}
