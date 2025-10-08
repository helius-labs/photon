use crate::common::typedefs::hash::Hash;
use crate::dao::generated::indexed_trees;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::indexed_merkle_tree::HIGHEST_ADDRESS_PLUS_ONE;
use ark_bn254::Fr;
use light_compressed_account::TreeType;
use light_poseidon::{Poseidon, PoseidonBytesHasher};
use sea_orm::{ConnectionTrait, TransactionTrait};
use solana_pubkey::Pubkey;

/// Computes range node hash based on tree type
pub fn compute_hash_by_tree_type(
    range_node: &indexed_trees::Model,
    tree_type: TreeType,
) -> Result<Hash, IngesterError> {
    match tree_type {
        TreeType::AddressV1 => compute_range_node_hash_v1(range_node)
            .map_err(|e| IngesterError::ParserError(format!("Failed to compute V1 hash: {}", e))),
        TreeType::AddressV2 => compute_range_node_hash(range_node)
            .map_err(|e| IngesterError::ParserError(format!("Failed to compute V2 hash: {}", e))),
        _ => Err(IngesterError::ParserError(format!(
            "Unsupported tree type for range node hash computation: {:?}",
            tree_type
        ))),
    }
}

/// Computes range node hash by looking up tree type from tree pubkey
pub async fn compute_hash_by_tree_pubkey<T>(
    conn: &T,
    range_node: &indexed_trees::Model,
    tree_pubkey: &[u8],
) -> Result<Hash, IngesterError>
where
    T: ConnectionTrait + TransactionTrait,
{
    let pubkey = Pubkey::try_from(tree_pubkey)
        .map_err(|e| IngesterError::ParserError(format!("Invalid pubkey bytes: {}", e)))?;
    let tree_type = TreeInfo::get_tree_type(conn, &pubkey)
        .await
        .map_err(|e| IngesterError::ParserError(format!("Failed to get tree type: {}", e)))?;
    compute_hash_by_tree_type(range_node, tree_type)
}

pub fn compute_hash_with_cache(
    range_node: &indexed_trees::Model,
    tree_pubkey: &[u8],
    tree_info_cache: &std::collections::HashMap<
        Pubkey,
        crate::ingester::parser::tree_info::TreeInfo,
    >,
) -> Result<Hash, IngesterError> {
    let pubkey = Pubkey::try_from(tree_pubkey)
        .map_err(|e| IngesterError::ParserError(format!("Invalid pubkey bytes: {}", e)))?;

    let tree_type = tree_info_cache
        .get(&pubkey)
        .map(|info| info.tree_type)
        .unwrap_or(TreeType::AddressV2);

    compute_hash_by_tree_type(range_node, tree_type)
}

pub fn compute_range_node_hash(node: &indexed_trees::Model) -> Result<Hash, IngesterError> {
    let mut poseidon = Poseidon::<Fr>::new_circom(2).unwrap();
    Hash::try_from(
        poseidon
            .hash_bytes_be(&[&node.value, &node.next_value])
            .map_err(|e| IngesterError::ParserError(format!("Failed to compute hash v2: {}", e)))
            .map(|x| x.to_vec())?,
    )
    .map_err(|e| IngesterError::ParserError(format!("Failed to convert hash v2: {}", e)))
}

pub fn compute_range_node_hash_v1(node: &indexed_trees::Model) -> Result<Hash, IngesterError> {
    let mut poseidon = Poseidon::<Fr>::new_circom(3).unwrap();
    let mut next_index_bytes = vec![0u8; 32];
    let index_be = node.next_index.to_be_bytes();
    next_index_bytes[24..32].copy_from_slice(&index_be);

    Hash::try_from(
        poseidon
            .hash_bytes_be(&[&node.value, &next_index_bytes, &node.next_value])
            .map_err(|e| IngesterError::ParserError(format!("Failed to compute hash v1: {}", e)))
            .map(|x| x.to_vec())?,
    )
    .map_err(|e| IngesterError::ParserError(format!("Failed to convert hash v1: {}", e)))
}

pub fn get_zeroeth_exclusion_range(tree: Vec<u8>) -> indexed_trees::Model {
    indexed_trees::Model {
        tree,
        leaf_index: 0,
        value: vec![0; 32],
        next_index: 0,
        next_value: vec![0]
            .into_iter()
            .chain(HIGHEST_ADDRESS_PLUS_ONE.to_bytes_be())
            .collect(),
        seq: Some(0),
    }
}

pub fn get_zeroeth_exclusion_range_v1(tree: Vec<u8>) -> indexed_trees::Model {
    indexed_trees::Model {
        tree,
        leaf_index: 0,
        value: vec![0; 32],
        next_index: 1,
        next_value: vec![0]
            .into_iter()
            .chain(HIGHEST_ADDRESS_PLUS_ONE.to_bytes_be())
            .collect(),
        seq: Some(0),
    }
}

pub fn get_top_element(tree: Vec<u8>) -> indexed_trees::Model {
    indexed_trees::Model {
        tree,
        leaf_index: 1,
        value: vec![0]
            .into_iter()
            .chain(HIGHEST_ADDRESS_PLUS_ONE.to_bytes_be())
            .collect(),
        next_index: 0,
        next_value: vec![0; 32],
        seq: Some(0),
    }
}
