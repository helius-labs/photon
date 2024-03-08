use dao::generated::state_trees;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};
use serde::{Deserialize, Serialize};

use crate::error::PhotonApiError;
use dao::typedefs::hash::Hash;

use super::utils::get_proof_path;
use super::utils::{build_full_proof, ProofResponse};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetUtxoProofRequest {
    pub hash: Hash,
}

// TODO: Optimize the DB queries to reduce latency.
// We make three calls when account_id but we only need to make two.
pub async fn get_utxo_proof(
    conn: &DatabaseConnection,
    request: GetUtxoProofRequest,
) -> Result<ProofResponse, PhotonApiError> {
    let leaf_node = state_trees::Entity::find()
        .filter(
            state_trees::Column::Hash
                .eq::<Vec<u8>>(request.hash.clone().into())
                .and(state_trees::Column::Level.eq(0)),
        )
        .one(conn)
        .await?
        .ok_or(PhotonApiError::RecordNotFound(format!(
            "UTXO {} not found",
            request.hash
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

    build_full_proof(proof_nodes, required_node_indices).map_err(Into::into)
}
