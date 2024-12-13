use sea_orm::{QueryFilter, QueryOrder};
use jsonrpsee_core::Serialize;
use log::{debug, info};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait};
use serde::Deserialize;
use utoipa::ToSchema;
use crate::api::error::PhotonApiError;
use crate::api::method::utils::Context;
use crate::common::typedefs::hash::Hash;
use crate::dao::generated::{indexed_trees, state_trees};
use crate::ingester::persist::compute_parent_hash;
use crate::ingester::persist::persisted_indexed_merkle_tree::compute_range_node_hash;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetSubtreesRequest {
    pub merkle_tree: Hash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetSubtreesResponse {
    pub context: Context,
    pub value: Vec<Hash>,
}

pub const BATCH_ADDRESS_TREE_HEIGHT: u32 = 40;
pub const BATCH_STATE_TREE_HEIGHT: u32 = 32;
async fn get_subtrees_from_indexed_tree(
    conn: &DatabaseConnection,
    merkle_tree: Vec<u8>,
) -> Result<Vec<Hash>, PhotonApiError> {
    debug!("Getting subtrees from indexed tree...");

    // Get all entries ordered by leaf_index
    let entries = indexed_trees::Entity::find()
        .filter(indexed_trees::Column::Tree.eq(merkle_tree.clone()))
        .order_by_asc(indexed_trees::Column::LeafIndex)
        .all(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

    let mut subtrees = vec![[0u8; 32]; BATCH_ADDRESS_TREE_HEIGHT as usize];

    // If we have entries, calculate the subtrees
    if !entries.is_empty() {
        // Build initial layer from leaf hashes
        let mut current_layer: Vec<Vec<u8>> = entries.iter()
            .map(|e| compute_range_node_hash(e)
                .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to compute range node hash: {}", e)))
                .map(|h| h.to_vec()))
            .collect::<Result<Vec<_>, _>>()?;

        let mut level = 0;
        while !current_layer.is_empty() && level < BATCH_ADDRESS_TREE_HEIGHT as usize {
            // Store the rightmost left node at this level
            if current_layer.len() % 2 == 0 && current_layer.len() >= 2 {
                // For even number of nodes, take second-to-last
                subtrees[level].copy_from_slice(&current_layer[current_layer.len() - 2]);
            } else if current_layer.len() % 2 == 1 {
                // For odd number of nodes, take the last one
                subtrees[level].copy_from_slice(&current_layer[current_layer.len() - 1]);
            }

            // Calculate next layer
            let mut next_layer = Vec::new();
            for chunk in current_layer.chunks(2) {
                if chunk.len() == 2 {
                    let parent = compute_parent_hash(chunk[0].clone(), chunk[1].clone())
                        .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to compute parent hash: {}", e)))?;
                    next_layer.push(parent);
                } else {
                    next_layer.push(chunk[0].clone());
                }
            }
            current_layer = next_layer;
            level += 1;
        }
    }

    Ok(subtrees.into_iter().map(Hash::from).collect())
}

async fn get_subtrees_from_state_tree(
    conn: &DatabaseConnection,
    merkle_tree: Vec<u8>,
) -> Result<Vec<Hash>, PhotonApiError> {
    debug!("Getting subtrees from state tree...");

    let mut subtrees = vec![[0u8; 32]; BATCH_STATE_TREE_HEIGHT as usize];

    let nodes = state_trees::Entity::find()
        .filter(state_trees::Column::Tree.eq(merkle_tree))
        .all(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

    if !nodes.is_empty() {
        // Group nodes by level
        let mut layers: Vec<Vec<Vec<u8>>> = vec![Vec::new(); BATCH_STATE_TREE_HEIGHT as usize];
        for node in nodes {
            let level = node.level as usize;
            if level < BATCH_STATE_TREE_HEIGHT as usize {
                layers[level].push(node.hash);
            }
        }

        // For each non-empty layer, find rightmost left node
        for (level, layer) in layers.iter().enumerate() {
            if !layer.is_empty() {
                if layer.len() % 2 == 0 && layer.len() >= 2 {
                    // For even number of nodes, take second-to-last
                    subtrees[level].copy_from_slice(&layer[layer.len() - 2]);
                } else if layer.len() % 2 == 1 {
                    // For odd number of nodes, take the last one
                    subtrees[level].copy_from_slice(&layer[layer.len() - 1]);
                }
            }
        }
    }

    Ok(subtrees.into_iter().map(Hash::from).collect())
}

pub async fn get_subtrees(
    conn: &DatabaseConnection,
    request: GetSubtreesRequest,
) -> Result<GetSubtreesResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let merkle_tree = request.merkle_tree.to_vec();

    info!("Getting subtrees for merkle tree {:?}", request.merkle_tree);

    // Check if it's an indexed tree
    let indexed_exists = indexed_trees::Entity::find()
        .filter(indexed_trees::Column::Tree.eq(merkle_tree.clone()))
        .one(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error checking indexed tree: {}", e)))?
        .is_some();

    // Choose the appropriate implementation based on tree type
    let subtrees = if indexed_exists {
        get_subtrees_from_indexed_tree(conn, merkle_tree).await?
    } else {
        // Check state tree
        let state_exists = state_trees::Entity::find()
            .filter(state_trees::Column::Tree.eq(merkle_tree.clone()))
            .one(conn)
            .await
            .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error checking state tree: {}", e)))?
            .is_some();

        if state_exists {
            get_subtrees_from_state_tree(conn, merkle_tree).await?
        } else {
            return Err(PhotonApiError::UnexpectedError("Merkle tree not found".into()));
        }
    };

    Ok(GetSubtreesResponse {
        context,
        value: subtrees,
    })
}