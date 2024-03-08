use dao::generated::{state_trees, utxos};
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};
use serde::{Deserialize, Serialize};

use crate::error::PhotonApiError;
use dao::typedefs::hash::Hash;

use dao::typedefs::serializable_pubkey::SerializablePubkey;

use super::utils::{build_full_proof, get_proof_path, ProofResponse};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountProofRequest {
    pub hash: Option<Hash>,
    pub account_id: Option<SerializablePubkey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountProofResponse {
    pub hash: Hash,
    pub root: Hash,
    pub proof: Vec<Hash>,
}

// TODO: Optimize the DB queries to reduce latency.
// We make three calls when account_id but we only need to make two.
pub async fn get_compressed_account_proof(
    conn: &DatabaseConnection,
    request: GetCompressedAccountProofRequest,
) -> Result<Option<GetCompressedAccountProofResponse>, PhotonApiError> {
    let GetCompressedAccountProofRequest { hash, account_id } = request;

    // Extract the leaf hash from the user or look it up via the provided account_id.
    let leaf_hash: Vec<u8>;
    if let Some(h) = hash.clone() {
        leaf_hash = h.into();
    } else if let Some(a) = account_id {
        let acc_vec: Vec<u8> = a.into();
        let res = utxos::Entity::find()
            .filter(utxos::Column::Account.eq(acc_vec))
            .one(conn)
            .await?;
        if let Some(utxo) = res {
            leaf_hash = utxo.hash;
        } else {
            return Ok(None);
        }
    } else {
        return Err(PhotonApiError::ValidationError(
            "Must provide either `hash` or `account_id`".to_string(),
        ));
    }

    let leaf_node = state_trees::Entity::find()
        .filter(
            state_trees::Column::Hash
                .eq(leaf_hash)
                .and(state_trees::Column::Level.eq(0)),
        )
        .one(conn)
        .await?;
    if leaf_node.is_none() {
        return Ok(None);
    }
    let leaf_node = leaf_node.unwrap();
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
    let res = GetCompressedAccountProofResponse {
        hash: Hash::try_from(leaf_node.hash)?.into(),
        root,
        proof,
    };
    Ok(Some(res))
}
