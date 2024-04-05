use std::collections::HashMap;

use crate::dao::{generated::state_trees, typedefs::serializable_pubkey::SerializablePubkey};
use itertools::Itertools;
use sea_orm::{
    sea_query::Expr, ColumnTrait, Condition, DatabaseConnection, EntityTrait, QueryFilter,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    utils::{Context, ResponseWithContext, PAGE_LIMIT},
};
use crate::dao::typedefs::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MerkleProofWithContext {
    pub proof: Vec<Hash>,
    pub leaf_index: u32,
    pub hash: Hash,
    pub merkle_tree: SerializablePubkey,
}

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
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
    let proofs = get_multiple_compressed_account_proofs_helper(conn, request).await?;
    Ok(GetMultipleCompressedAccountProofsResponse {
        value: proofs,
        context,
    })
}

pub async fn get_multiple_compressed_account_proofs_helper(
    conn: &DatabaseConnection,
    hashes: Vec<Hash>,
) -> Result<Vec<MerkleProofWithContext>, PhotonApiError> {
    let leaf_nodes = state_trees::Entity::find()
        .filter(
            state_trees::Column::Hash
                .is_in(hashes.iter().map(|x| x.to_vec()).collect::<Vec<Vec<u8>>>())
                .and(state_trees::Column::Level.eq(0)),
        )
        .all(conn)
        .await?;

    if leaf_nodes.len() != hashes.len() {
        return Err(PhotonApiError::RecordNotFound(
            "Leaf nodes not found for some hashes".to_string(),
        ));
    }
    let leaf_hashes_to_model = leaf_nodes
        .iter()
        .map(|leaf_node| (leaf_node.hash.clone(), leaf_node.clone()))
        .collect::<HashMap<Vec<u8>, state_trees::Model>>();

    let leaf_hashes_to_required_nodes = leaf_nodes
        .iter()
        .map(|leaf_node| {
            let required_node_indices = get_proof_path(leaf_node.node_idx);
            (
                leaf_node.hash.clone(),
                (leaf_node.tree.clone(), required_node_indices),
            )
        })
        .collect::<HashMap<Vec<u8>, (Vec<u8>, Vec<i64>)>>();

    let all_required_node_indices = leaf_hashes_to_required_nodes
        .values()
        .flat_map(|(tree, indices)| indices.iter().map(move |&idx| (tree.clone(), idx)))
        .dedup()
        .collect::<Vec<(Vec<u8>, i64)>>();

    let mut condition = Condition::any();
    for (tree, node) in all_required_node_indices.clone() {
        let node_condition = Condition::all()
            .add(Expr::col(state_trees::Column::Tree).eq(tree))
            .add(Expr::col(state_trees::Column::NodeIdx).eq(node));

        // Add this condition to the overall condition with an OR
        condition = condition.add(node_condition);
    }

    let proof_nodes = state_trees::Entity::find()
        .filter(condition)
        .all(conn)
        .await?;

    let node_to_proof = proof_nodes
        .iter()
        .map(|node| ((node.tree.clone(), node.node_idx), node.clone()))
        .collect::<HashMap<(Vec<u8>, i64), state_trees::Model>>();

    hashes
        .iter()
        .map(|hash| {
            let (tree, required_node_indices) = leaf_hashes_to_required_nodes
                .get(&hash.to_vec())
                .ok_or(PhotonApiError::RecordNotFound(format!(
                "Leaf node not found for hash {}",
                hash
            )))?;

            let proofs = required_node_indices
                .iter()
                .map(|idx| {
                    node_to_proof
                        .get(&(tree.clone(), *idx))
                        .map(|node| {
                            Hash::try_from(node.hash.clone()).map_err(|_| {
                                PhotonApiError::UnexpectedError(
                                    "Failed to convert hash to bytes".to_string(),
                                )
                            })
                        })
                        .unwrap_or(Ok(Hash::from([0; 32])))
                })
                .collect::<Result<Vec<Hash>, PhotonApiError>>()?;

            let leaf_model =
                leaf_hashes_to_model
                    .get(&hash.to_vec())
                    .ok_or(PhotonApiError::RecordNotFound(format!(
                        "Leaf node not found for hash {}",
                        hash
                    )))?;

            Ok(MerkleProofWithContext {
                proof: proofs,
                leaf_index: leaf_model.leaf_idx.ok_or(PhotonApiError::RecordNotFound(
                    "Leaf index not found".to_string(),
                ))? as u32,
                hash: hash.clone(),
                merkle_tree: leaf_model.tree.clone().try_into()?,
            })
        })
        .collect()
}

pub fn get_proof_path(index: i64) -> Vec<i64> {
    let mut indexes = vec![];
    let mut idx = index;
    while idx > 1 {
        if idx % 2 == 0 {
            indexes.push(idx + 1)
        } else {
            indexes.push(idx - 1)
        }
        idx >>= 1
    }
    indexes.push(1);
    indexes
}
