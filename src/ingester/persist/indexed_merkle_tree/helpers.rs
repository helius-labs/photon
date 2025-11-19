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

/// Hardcoded initial root for AddressV2 trees with height 40.
/// This must match ADDRESS_TREE_INIT_ROOT_40 from batched-merkle-tree constants.
/// See: program-libs/batched-merkle-tree/src/constants.rs
pub const ADDRESS_TREE_INIT_ROOT_40: [u8; 32] = [
    28, 65, 107, 255, 208, 234, 51, 3, 131, 95, 62, 130, 202, 177, 176, 26, 216, 81, 64, 184, 200,
    25, 95, 124, 248, 129, 44, 109, 229, 146, 106, 76,
];

/// Computes range node hash based on tree type
pub fn compute_hash_by_tree_type(
    range_node: &indexed_trees::Model,
    tree_type: TreeType,
) -> Result<Hash, IngesterError> {
    match tree_type {
        // AddressV1 uses 3-field hash: H(value, next_index, next_value)
        TreeType::AddressV1 => compute_range_node_hash_v1(range_node).map_err(|e| {
            IngesterError::ParserError(format!("Failed to compute address v1 hash: {}", e))
        }),
        // AddressV2 uses 2-field hash: H(value, next_value)
        // next_index is stored but NOT included in hash (removed in commit e208fa1eb)
        TreeType::AddressV2 => compute_range_node_hash_v2(range_node).map_err(|e| {
            IngesterError::ParserError(format!("Failed to compute address v2 hash: {}", e))
        }),
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

/// Computes range node hash for AddressV1 indexed merkle trees.
/// Uses 3-field Poseidon hash: H(value, next_index, next_value)
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

/// Computes range node hash for AddressV2 indexed merkle trees.
/// Uses 2-field Poseidon hash: H(value, next_value)
/// Note: next_index is stored in the database but NOT included in the hash.
/// This change was introduced in commit e208fa1eb ("remove next_index from circuit").
pub fn compute_range_node_hash_v2(node: &indexed_trees::Model) -> Result<Hash, IngesterError> {
    let mut poseidon = Poseidon::<Fr>::new_circom(2).unwrap();

    Hash::try_from(
        poseidon
            .hash_bytes_be(&[&node.value, &node.next_value])
            .map_err(|e| IngesterError::ParserError(format!("Failed to compute hash v2: {}", e)))
            .map(|x| x.to_vec())?,
    )
    .map_err(|e| IngesterError::ParserError(format!("Failed to convert hash v2: {}", e)))
}

pub fn get_zeroeth_exclusion_range(tree: Vec<u8>) -> indexed_trees::Model {
    use light_hasher::bigint::bigint_to_be_bytes_array;

    indexed_trees::Model {
        tree,
        leaf_index: 0,
        value: vec![0; 32],
        // next_index is 0 initially (not 1!), matching IndexedArray::new behavior
        next_index: 0,
        // Use bigint_to_be_bytes_array to properly encode as 32 bytes (right-aligned)
        next_value: bigint_to_be_bytes_array::<32>(&HIGHEST_ADDRESS_PLUS_ONE)
            .unwrap()
            .to_vec(),
        seq: Some(0),
    }
}

pub fn get_zeroeth_exclusion_range_v1(tree: Vec<u8>) -> indexed_trees::Model {
    use light_hasher::bigint::bigint_to_be_bytes_array;

    indexed_trees::Model {
        tree,
        leaf_index: 0,
        value: vec![0; 32],
        next_index: 1,
        next_value: bigint_to_be_bytes_array::<32>(&HIGHEST_ADDRESS_PLUS_ONE)
            .unwrap()
            .to_vec(),
        seq: Some(0),
    }
}

/// Alias for compute_range_node_hash_v2 to maintain backwards compatibility.
/// Defaults to AddressV2 behavior (2-field hash).
/// For AddressV1, use compute_range_node_hash_v1 directly.
pub fn compute_range_node_hash(node: &indexed_trees::Model) -> Result<Hash, IngesterError> {
    compute_range_node_hash_v2(node)
}

pub fn get_top_element(tree: Vec<u8>) -> indexed_trees::Model {
    use light_hasher::bigint::bigint_to_be_bytes_array;

    indexed_trees::Model {
        tree,
        leaf_index: 1,
        value: bigint_to_be_bytes_array::<32>(&HIGHEST_ADDRESS_PLUS_ONE)
            .unwrap()
            .to_vec(),
        next_index: 0,
        next_value: vec![0; 32],
        seq: Some(0),
    }
}
