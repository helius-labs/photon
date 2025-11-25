use crate::api::error::PhotonApiError;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::state_trees;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::leaf_node::{leaf_index_to_node_index, LeafNode};
use crate::ingester::persist::persisted_batch_event::sequence::get_current_tree_sequence;
use crate::ingester::persist::persisted_state_tree::{get_proof_nodes, get_proof_path, ZERO_BYTES};
use crate::ingester::persist::MerkleProofWithContext;
use sea_orm::QueryFilter;
use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait};
use std::collections::HashMap;

pub async fn get_multiple_compressed_leaf_proofs_by_indices(
    txn: &DatabaseTransaction,
    merkle_tree_pubkey: SerializablePubkey,
    indices: Vec<u64>,
) -> Result<Vec<MerkleProofWithContext>, PhotonApiError> {
    // TODO: add assertion that: abs(max(index from db) - max(indices)) <= BATCH_SIZE * 2
    if indices.is_empty() {
        return Ok(Vec::new());
    }

    // Convert SerializablePubkey to [u8; 32] for the helper function
    let tree_bytes = merkle_tree_pubkey.0.to_bytes();
    let root_seq = get_current_tree_sequence(txn, &tree_bytes)
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to get tree sequence: {}", e))
        })? as u32;
    let root_seq = if root_seq == 0 { None } else { Some(root_seq) };

    log::debug!(
        "Fetching proofs for {} indices on tree {}, current root_seq: {:?}",
        indices.len(),
        merkle_tree_pubkey,
        root_seq
    );

    let existing_leaves = state_trees::Entity::find()
        .filter(
            state_trees::Column::LeafIdx
                .is_in(indices.iter().map(|&x| x as i64).collect::<Vec<i64>>())
                .and(state_trees::Column::Level.eq(0))
                .and(state_trees::Column::Tree.eq(merkle_tree_pubkey.to_bytes_vec())),
        )
        .all(txn)
        .await?;

    let mut index_to_leaf = existing_leaves
        .into_iter()
        .map(|x| (x.leaf_idx.unwrap_or_default() as u64, x))
        .collect::<HashMap<_, _>>();

    let tree_height = TreeInfo::height(txn, &merkle_tree_pubkey.to_string())
        .await?
        .ok_or(PhotonApiError::RecordNotFound(format!(
            "Tree info not found for tree: {}",
            merkle_tree_pubkey
        )))?
        + 1;

    // Create leaf nodes for all requested indices
    let mut leaf_nodes = Vec::new();

    for idx in indices {
        if let Some(existing) = index_to_leaf.remove(&idx) {
            // Use existing leaf
            leaf_nodes.push((
                LeafNode {
                    tree: merkle_tree_pubkey,
                    leaf_index: idx as u32,
                    hash: Hash::try_from(existing.hash)?,
                    seq: root_seq,
                },
                existing.node_idx,
            ));
        } else {
            let zero_leaf = LeafNode {
                tree: merkle_tree_pubkey,
                leaf_index: idx as u32,
                hash: Hash::from(ZERO_BYTES[0]),
                seq: root_seq,
            };
            let node_idx = leaf_index_to_node_index(zero_leaf.leaf_index, tree_height);
            leaf_nodes.push((zero_leaf.clone(), node_idx));
        }
    }

    get_multiple_compressed_leaf_proofs_from_full_leaf_info(txn, leaf_nodes, tree_height).await
}

pub async fn get_multiple_compressed_leaf_proofs(
    txn: &DatabaseTransaction,
    hashes: Vec<Hash>,
) -> Result<Vec<MerkleProofWithContext>, PhotonApiError> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }

    let leaf_records = state_trees::Entity::find()
        .filter(
            state_trees::Column::Hash
                .is_in(hashes.iter().map(|x| x.to_vec()).collect::<Vec<Vec<u8>>>())
                .and(state_trees::Column::Level.eq(0)),
        )
        .all(txn)
        .await?;

    // Get unique trees from the leaf records
    let unique_trees: Vec<Vec<u8>> = leaf_records
        .iter()
        .map(|x| x.tree.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // Fetch root seq for each tree using the helper function
    let mut tree_to_root_seq = HashMap::new();
    for tree in unique_trees {
        // Convert Vec<u8> to [u8; 32] for the helper
        let tree_array: [u8; 32] = tree.clone().try_into().map_err(|_| {
            PhotonApiError::UnexpectedError("Invalid tree bytes length".to_string())
        })?;

        let root_seq = get_current_tree_sequence(txn, &tree_array)
            .await
            .map_err(|e| {
                PhotonApiError::UnexpectedError(format!("Failed to get tree sequence: {}", e))
            })?;

        let root_seq = if root_seq == 0 {
            None
        } else {
            Some(root_seq as u32)
        };

        if let Ok(tree_pubkey) = SerializablePubkey::try_from(tree.clone()) {
            log::debug!("Tree {} root seq: {:?}", tree_pubkey, root_seq);
        }

        tree_to_root_seq.insert(tree, root_seq);
    }

    let leaf_nodes_with_node_index = leaf_records
        .into_iter()
        .map(|x| {
            let tree_bytes = x.tree.clone();
            let root_seq = tree_to_root_seq.get(&tree_bytes).and_then(|&s| s);
            Ok((
                LeafNode {
                    tree: SerializablePubkey::try_from(tree_bytes)?,
                    leaf_index: x.leaf_idx.ok_or(PhotonApiError::RecordNotFound(
                        "Leaf index not found".to_string(),
                    ))? as u32,
                    hash: Hash::try_from(x.hash.clone())?,
                    seq: root_seq,
                },
                x.node_idx,
            ))
        })
        .collect::<Result<Vec<(LeafNode, i64)>, PhotonApiError>>()?;

    if leaf_nodes_with_node_index.len() != hashes.len() {
        return Err(PhotonApiError::RecordNotFound(format!(
            "Leaf nodes not found for hashes. Got {} hashes. Expected {}.",
            leaf_nodes_with_node_index.len(),
            hashes.len()
        )));
    }

    let hash_to_leaf_node_with_node_index = leaf_nodes_with_node_index
        .iter()
        .map(|(leaf_node, node_index)| (leaf_node.hash.clone(), (leaf_node.clone(), *node_index)))
        .collect::<HashMap<Hash, (LeafNode, i64)>>();

    let leaf_nodes_with_node_index = hashes
        .iter()
        .map(|hash| {
            hash_to_leaf_node_with_node_index
                .get(hash)
                .ok_or(PhotonApiError::RecordNotFound(format!(
                    "Leaf node not found for hash: {}",
                    hash
                )))
                .cloned()
        })
        .collect::<Result<Vec<(LeafNode, i64)>, PhotonApiError>>()?;

    let tree_height = if !leaf_nodes_with_node_index.is_empty() {
        let first_tree = &leaf_nodes_with_node_index[0].0.tree;
        TreeInfo::height(txn, &first_tree.to_string())
            .await?
            .ok_or(PhotonApiError::RecordNotFound(format!(
                "Tree info not found for tree: {}",
                first_tree
            )))?
            + 1 // Add 1 for indexed trees
    } else {
        return Ok(Vec::new());
    };

    get_multiple_compressed_leaf_proofs_from_full_leaf_info(
        txn,
        leaf_nodes_with_node_index,
        tree_height,
    )
    .await
}

pub async fn get_multiple_compressed_leaf_proofs_from_full_leaf_info(
    txn: &DatabaseTransaction,
    leaf_nodes_with_node_index: Vec<(LeafNode, i64)>,
    tree_height: u32,
) -> Result<Vec<MerkleProofWithContext>, PhotonApiError> {
    let include_leafs = false;
    let leaf_locations_to_required_nodes = leaf_nodes_with_node_index
        .iter()
        .map(|(leaf_node, node_index)| {
            let required_node_indices = get_proof_path(*node_index, include_leafs);
            (
                (leaf_node.tree.to_bytes_vec(), *node_index),
                required_node_indices,
            )
        })
        .collect::<HashMap<(Vec<u8>, i64), Vec<i64>>>();

    let node_to_model = get_proof_nodes(
        txn,
        leaf_nodes_with_node_index
            .iter()
            .map(|(node, node_index)| (node.tree.to_bytes_vec(), *node_index))
            .collect::<Vec<(Vec<u8>, i64)>>(),
        include_leafs,
        true,
        tree_height,
    )
    .await?;

    let proofs: Result<Vec<MerkleProofWithContext>, PhotonApiError> = leaf_nodes_with_node_index
        .iter()
        .map(|(leaf_node, node_index)| {
            let required_node_indices = leaf_locations_to_required_nodes
                .get(&(leaf_node.tree.to_bytes_vec(), *node_index))
                .ok_or(PhotonApiError::RecordNotFound(format!(
                    "Leaf node not found for tree and index: {} {}",
                    leaf_node.tree, node_index
                )))?;

            let mut proof = required_node_indices
                .iter()
                .enumerate()
                .map(|(level, idx)| {
                    node_to_model
                        .get(&(leaf_node.tree.to_bytes_vec(), *idx))
                        .map(|node| {
                            Hash::try_from(node.hash.clone()).map_err(|_| {
                                PhotonApiError::UnexpectedError(
                                    "Failed to convert hash to bytes".to_string(),
                                )
                            })
                        })
                        .unwrap_or(Ok(Hash::from(ZERO_BYTES[level])))
                })
                .collect::<Result<Vec<Hash>, PhotonApiError>>()?;

            let root_seq = match node_to_model.get(&(leaf_node.tree.to_bytes_vec(), 1)) {
                Some(root) => root.seq,
                None => None,
            };

            let root = proof.pop().ok_or(PhotonApiError::UnexpectedError(
                "Root node not found in proof".to_string(),
            ))?;

            Ok(MerkleProofWithContext {
                proof,
                root,
                leaf_index: leaf_node.leaf_index,
                hash: leaf_node.hash.clone(),
                merkle_tree: leaf_node.tree,
                root_seq: root_seq.unwrap_or(0i64) as u64,
            })
        })
        .collect();
    let proofs = proofs?;
    // Commented because it makes batched state Merkle tree tests 20x slower.
    // TODO: move behind debug flag
    // for proof in proofs.iter() {
    //     validate_proof(proof)?;
    // }

    Ok(proofs)
}
