use std::collections::HashMap;

use cadence_macros::statsd_count;
use itertools::Itertools;
use log::info;
use sea_orm::{ConnectionTrait, DbErr, EntityTrait, Statement, TransactionTrait, Value};
use serde::{Deserialize, Serialize};
use solana_program::pubkey::Pubkey;
use utoipa::ToSchema;

use super::{compute_parent_hash, get_tree_height};
use crate::ingester::persist::leaf_node::leaf_index_to_node_index;
use crate::{
    api::error::PhotonApiError,
    common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey},
    dao::generated::state_trees,
    metric,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct MerkleProofWithContext {
    pub proof: Vec<Hash>,
    pub root: Hash,
    pub leafIndex: u32,
    pub hash: Hash,
    pub merkleTree: SerializablePubkey,
    pub rootSeq: u64,
}

pub fn validate_proof(proof: &MerkleProofWithContext) -> Result<(), PhotonApiError> {
    info!(
        "Validating proof for leaf index: {} tree: {}",
        proof.leafIndex, proof.merkleTree
    );
    let leaf_index = proof.leafIndex;
    let tree_height = (proof.proof.len() + 1) as u32;
    let node_index = leaf_index_to_node_index(leaf_index, tree_height);
    let mut computed_root = proof.hash.to_vec();
    info!("leaf_index: {}, node_index: {}", leaf_index, node_index);

    for (idx, node) in proof.proof.iter().enumerate() {
        let is_left = (node_index >> idx) & 1 == 0;
        computed_root = compute_parent_hash(
            if is_left {
                computed_root.clone()
            } else {
                node.to_vec()
            },
            if is_left {
                node.to_vec()
            } else {
                computed_root.clone()
            },
        )
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to compute parent hash for proof: {}",
                e
            ))
        })?;
    }
    if computed_root != proof.root.to_vec() {
        metric! {
            statsd_count!("invalid_proof", 1);
        }
        return Err(PhotonApiError::UnexpectedError(format!(
            "Computed root does not match the provided root. Proof; {:?}",
            proof
        )));
    }

    Ok(())
}

pub fn get_proof_path(index: i64, include_leaf: bool) -> Vec<i64> {
    let mut indexes = vec![];
    let mut idx = index;
    if include_leaf {
        indexes.push(index);
    }
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

pub fn get_level_by_node_index(index: i64, tree_height: u32) -> i64 {
    if index >= 2_i64.pow(tree_height - 2) {
        // If it's a leaf index (large number)
        return 0;
    }
    let mut level = 0;
    let mut idx = index;
    while idx > 1 {
        idx >>= 1;
        level += 1;
    }
    level
}

pub async fn get_proof_nodes<T>(
    txn_or_conn: &T,
    leaf_nodes_locations: Vec<(Vec<u8>, i64)>,
    include_leafs: bool,
    include_empty_leaves: bool,
) -> Result<HashMap<(Vec<u8>, i64), state_trees::Model>, DbErr>
where
    T: ConnectionTrait + TransactionTrait,
{
    let all_required_node_indices = leaf_nodes_locations
        .iter()
        .flat_map(|(tree, index)| {
            get_proof_path(*index, include_leafs)
                .iter()
                .map(move |&idx| (tree.clone(), idx))
                .collect::<Vec<(Vec<u8>, i64)>>()
        })
        .sorted_by(|a, b| {
            // Need to sort elements before dedup
            a.0.cmp(&b.0) // Sort by tree
                .then_with(|| a.1.cmp(&b.1)) // Then by node index
        })
        .dedup()
        .collect::<Vec<(Vec<u8>, i64)>>();

    let mut params = Vec::new();
    let mut placeholders = Vec::new();

    for (index, (tree, node_idx)) in all_required_node_indices.into_iter().enumerate() {
        let param_index = index * 2; // each pair contributes two parameters
        params.push(Value::from(tree));
        params.push(Value::from(node_idx));
        placeholders.push(format!("(${}, ${})", param_index + 1, param_index + 2));
    }
    let placeholder_str = placeholders.join(", ");
    let sql = format!(
            "WITH vals(tree, node_idx) AS (VALUES {}) SELECT st.* FROM state_trees st JOIN vals v ON st.tree = v.tree AND st.node_idx = v.node_idx",
            placeholder_str
        );

    let proof_nodes = state_trees::Entity::find()
        .from_raw_sql(Statement::from_sql_and_values(
            txn_or_conn.get_database_backend(),
            &sql,
            params,
        ))
        .all(txn_or_conn)
        .await?;

    let mut result = proof_nodes
        .iter()
        .map(|node| ((node.tree.clone(), node.node_idx), node.clone()))
        .collect::<HashMap<(Vec<u8>, i64), state_trees::Model>>();

    if include_empty_leaves {
        leaf_nodes_locations.iter().for_each(|(tree, index)| {
            result.entry((tree.clone(), *index)).or_insert_with(|| {
                log::warn!(
                    "Missing proof node for tree: {} and index: {}",
                    SerializablePubkey::try_from(tree.clone()).unwrap(),
                    index
                );

                let tree_pubkey = Pubkey::try_from(tree.clone()).unwrap();
                let tree_height = get_tree_height(&tree_pubkey);
                println!("tree_height: {}", tree_height);
                let model = state_trees::Model {
                    tree: tree.clone(),
                    level: get_level_by_node_index(*index, tree_height),
                    node_idx: *index,
                    hash: ZERO_BYTES[get_level_by_node_index(*index, tree_height) as usize]
                        .to_vec(),
                    leaf_idx: None,
                    seq: None,
                };
                model
            });
        });
    }

    Ok(result)
}

pub fn validate_leaf_index(leaf_index: u32, tree_height: u32) -> bool {
    let max_leaves = 2_u64.pow(tree_height - 1);
    (leaf_index as u64) < max_leaves
}

pub fn get_merkle_proof_length(tree_height: u32) -> usize {
    (tree_height - 1) as usize
}

pub const MAX_HEIGHT: usize = 32;
type ZeroBytes = [[u8; 32]; MAX_HEIGHT + 1];

pub const ZERO_BYTES: ZeroBytes = [
    [
        0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
        0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
    ],
    [
        32u8, 152u8, 245u8, 251u8, 158u8, 35u8, 158u8, 171u8, 60u8, 234u8, 195u8, 242u8, 123u8,
        129u8, 228u8, 129u8, 220u8, 49u8, 36u8, 213u8, 95u8, 254u8, 213u8, 35u8, 168u8, 57u8,
        238u8, 132u8, 70u8, 182u8, 72u8, 100u8,
    ],
    [
        16u8, 105u8, 103u8, 61u8, 205u8, 177u8, 34u8, 99u8, 223u8, 48u8, 26u8, 111u8, 245u8, 132u8,
        167u8, 236u8, 38u8, 26u8, 68u8, 203u8, 157u8, 198u8, 141u8, 240u8, 103u8, 164u8, 119u8,
        68u8, 96u8, 177u8, 241u8, 225u8,
    ],
    [
        24u8, 244u8, 51u8, 49u8, 83u8, 126u8, 226u8, 175u8, 46u8, 61u8, 117u8, 141u8, 80u8, 247u8,
        33u8, 6u8, 70u8, 124u8, 110u8, 234u8, 80u8, 55u8, 29u8, 213u8, 40u8, 213u8, 126u8, 178u8,
        184u8, 86u8, 210u8, 56u8,
    ],
    [
        7u8, 249u8, 216u8, 55u8, 203u8, 23u8, 176u8, 211u8, 99u8, 32u8, 255u8, 233u8, 59u8, 165u8,
        35u8, 69u8, 241u8, 183u8, 40u8, 87u8, 26u8, 86u8, 130u8, 101u8, 202u8, 172u8, 151u8, 85u8,
        157u8, 188u8, 149u8, 42u8,
    ],
    [
        43u8, 148u8, 207u8, 94u8, 135u8, 70u8, 179u8, 245u8, 201u8, 99u8, 31u8, 76u8, 93u8, 243u8,
        41u8, 7u8, 166u8, 153u8, 197u8, 140u8, 148u8, 178u8, 173u8, 77u8, 123u8, 92u8, 236u8, 22u8,
        57u8, 24u8, 63u8, 85u8,
    ],
    [
        45u8, 238u8, 147u8, 197u8, 166u8, 102u8, 69u8, 150u8, 70u8, 234u8, 125u8, 34u8, 204u8,
        169u8, 225u8, 188u8, 254u8, 215u8, 30u8, 105u8, 81u8, 185u8, 83u8, 97u8, 29u8, 17u8, 221u8,
        163u8, 46u8, 160u8, 157u8, 120u8,
    ],
    [
        7u8, 130u8, 149u8, 229u8, 162u8, 43u8, 132u8, 233u8, 130u8, 207u8, 96u8, 30u8, 182u8, 57u8,
        89u8, 123u8, 139u8, 5u8, 21u8, 168u8, 140u8, 181u8, 172u8, 127u8, 168u8, 164u8, 170u8,
        190u8, 60u8, 135u8, 52u8, 157u8,
    ],
    [
        47u8, 165u8, 229u8, 241u8, 143u8, 96u8, 39u8, 166u8, 80u8, 27u8, 236u8, 134u8, 69u8, 100u8,
        71u8, 42u8, 97u8, 107u8, 46u8, 39u8, 74u8, 65u8, 33u8, 26u8, 68u8, 76u8, 190u8, 58u8,
        153u8, 243u8, 204u8, 97u8,
    ],
    [
        14u8, 136u8, 67u8, 118u8, 208u8, 216u8, 253u8, 33u8, 236u8, 183u8, 128u8, 56u8, 158u8,
        148u8, 31u8, 102u8, 228u8, 94u8, 122u8, 204u8, 227u8, 226u8, 40u8, 171u8, 62u8, 33u8, 86u8,
        166u8, 20u8, 252u8, 215u8, 71u8,
    ],
    [
        27u8, 114u8, 1u8, 218u8, 114u8, 73u8, 79u8, 30u8, 40u8, 113u8, 122u8, 209u8, 165u8, 46u8,
        180u8, 105u8, 249u8, 88u8, 146u8, 249u8, 87u8, 113u8, 53u8, 51u8, 222u8, 97u8, 117u8,
        229u8, 218u8, 25u8, 10u8, 242u8,
    ],
    [
        31u8, 141u8, 136u8, 34u8, 114u8, 94u8, 54u8, 56u8, 82u8, 0u8, 192u8, 178u8, 1u8, 36u8,
        152u8, 25u8, 166u8, 230u8, 225u8, 228u8, 101u8, 8u8, 8u8, 181u8, 190u8, 188u8, 107u8,
        250u8, 206u8, 125u8, 118u8, 54u8,
    ],
    [
        44u8, 93u8, 130u8, 246u8, 108u8, 145u8, 75u8, 175u8, 185u8, 112u8, 21u8, 137u8, 186u8,
        140u8, 252u8, 251u8, 97u8, 98u8, 176u8, 161u8, 42u8, 207u8, 136u8, 168u8, 208u8, 135u8,
        154u8, 4u8, 113u8, 181u8, 248u8, 90u8,
    ],
    [
        20u8, 197u8, 65u8, 72u8, 160u8, 148u8, 11u8, 184u8, 32u8, 149u8, 127u8, 90u8, 223u8, 63u8,
        161u8, 19u8, 78u8, 245u8, 196u8, 170u8, 161u8, 19u8, 244u8, 100u8, 100u8, 88u8, 242u8,
        112u8, 224u8, 191u8, 191u8, 208u8,
    ],
    [
        25u8, 13u8, 51u8, 177u8, 47u8, 152u8, 111u8, 150u8, 30u8, 16u8, 192u8, 238u8, 68u8, 216u8,
        185u8, 175u8, 17u8, 190u8, 37u8, 88u8, 140u8, 173u8, 137u8, 212u8, 22u8, 17u8, 142u8, 75u8,
        244u8, 235u8, 232u8, 12u8,
    ],
    [
        34u8, 249u8, 138u8, 169u8, 206u8, 112u8, 65u8, 82u8, 172u8, 23u8, 53u8, 73u8, 20u8, 173u8,
        115u8, 237u8, 17u8, 103u8, 174u8, 101u8, 150u8, 175u8, 81u8, 10u8, 165u8, 179u8, 100u8,
        147u8, 37u8, 224u8, 108u8, 146u8,
    ],
    [
        42u8, 124u8, 124u8, 155u8, 108u8, 229u8, 136u8, 11u8, 159u8, 111u8, 34u8, 141u8, 114u8,
        191u8, 106u8, 87u8, 90u8, 82u8, 111u8, 41u8, 198u8, 110u8, 204u8, 238u8, 248u8, 183u8,
        83u8, 211u8, 139u8, 186u8, 115u8, 35u8,
    ],
    [
        46u8, 129u8, 134u8, 229u8, 88u8, 105u8, 142u8, 193u8, 198u8, 122u8, 249u8, 193u8, 77u8,
        70u8, 63u8, 252u8, 71u8, 0u8, 67u8, 201u8, 194u8, 152u8, 139u8, 149u8, 77u8, 117u8, 221u8,
        100u8, 63u8, 54u8, 185u8, 146u8,
    ],
    [
        15u8, 87u8, 197u8, 87u8, 30u8, 154u8, 78u8, 171u8, 73u8, 226u8, 200u8, 207u8, 5u8, 13u8,
        174u8, 148u8, 138u8, 239u8, 110u8, 173u8, 100u8, 115u8, 146u8, 39u8, 53u8, 70u8, 36u8,
        157u8, 28u8, 31u8, 241u8, 15u8,
    ],
    [
        24u8, 48u8, 238u8, 103u8, 181u8, 251u8, 85u8, 74u8, 213u8, 246u8, 61u8, 67u8, 136u8, 128u8,
        14u8, 28u8, 254u8, 120u8, 227u8, 16u8, 105u8, 125u8, 70u8, 228u8, 60u8, 156u8, 227u8, 97u8,
        52u8, 247u8, 44u8, 202u8,
    ],
    [
        33u8, 52u8, 231u8, 106u8, 197u8, 210u8, 26u8, 171u8, 24u8, 108u8, 43u8, 225u8, 221u8,
        143u8, 132u8, 238u8, 136u8, 10u8, 30u8, 70u8, 234u8, 247u8, 18u8, 249u8, 211u8, 113u8,
        182u8, 223u8, 34u8, 25u8, 31u8, 62u8,
    ],
    [
        25u8, 223u8, 144u8, 236u8, 132u8, 78u8, 188u8, 79u8, 254u8, 235u8, 216u8, 102u8, 243u8,
        56u8, 89u8, 176u8, 192u8, 81u8, 216u8, 201u8, 88u8, 238u8, 58u8, 168u8, 143u8, 143u8,
        141u8, 243u8, 219u8, 145u8, 165u8, 177u8,
    ],
    [
        24u8, 204u8, 162u8, 166u8, 107u8, 92u8, 7u8, 135u8, 152u8, 30u8, 105u8, 174u8, 253u8,
        132u8, 133u8, 45u8, 116u8, 175u8, 14u8, 147u8, 239u8, 73u8, 18u8, 180u8, 100u8, 140u8, 5u8,
        247u8, 34u8, 239u8, 229u8, 43u8,
    ],
    [
        35u8, 136u8, 144u8, 148u8, 21u8, 35u8, 13u8, 27u8, 77u8, 19u8, 4u8, 210u8, 213u8, 79u8,
        71u8, 58u8, 98u8, 131u8, 56u8, 242u8, 239u8, 173u8, 131u8, 250u8, 223u8, 5u8, 100u8, 69u8,
        73u8, 210u8, 83u8, 141u8,
    ],
    [
        39u8, 23u8, 31u8, 180u8, 169u8, 123u8, 108u8, 192u8, 233u8, 232u8, 245u8, 67u8, 181u8,
        41u8, 77u8, 232u8, 102u8, 162u8, 175u8, 44u8, 156u8, 141u8, 11u8, 29u8, 150u8, 230u8,
        115u8, 228u8, 82u8, 158u8, 213u8, 64u8,
    ],
    [
        47u8, 246u8, 101u8, 5u8, 64u8, 246u8, 41u8, 253u8, 87u8, 17u8, 160u8, 188u8, 116u8, 252u8,
        13u8, 40u8, 220u8, 178u8, 48u8, 185u8, 57u8, 37u8, 131u8, 229u8, 248u8, 213u8, 150u8,
        150u8, 221u8, 230u8, 174u8, 33u8,
    ],
    [
        18u8, 12u8, 88u8, 241u8, 67u8, 212u8, 145u8, 233u8, 89u8, 2u8, 247u8, 245u8, 39u8, 119u8,
        120u8, 162u8, 224u8, 173u8, 81u8, 104u8, 246u8, 173u8, 215u8, 86u8, 105u8, 147u8, 38u8,
        48u8, 206u8, 97u8, 21u8, 24u8,
    ],
    [
        31u8, 33u8, 254u8, 183u8, 13u8, 63u8, 33u8, 176u8, 123u8, 248u8, 83u8, 213u8, 229u8, 219u8,
        3u8, 7u8, 30u8, 196u8, 149u8, 160u8, 165u8, 101u8, 162u8, 29u8, 162u8, 214u8, 101u8, 210u8,
        121u8, 72u8, 55u8, 149u8,
    ],
    [
        36u8, 190u8, 144u8, 95u8, 167u8, 19u8, 53u8, 225u8, 76u8, 99u8, 140u8, 192u8, 246u8, 106u8,
        134u8, 35u8, 168u8, 38u8, 231u8, 104u8, 6u8, 138u8, 158u8, 150u8, 139u8, 177u8, 161u8,
        221u8, 225u8, 138u8, 114u8, 210u8,
    ],
    [
        15u8, 134u8, 102u8, 182u8, 46u8, 209u8, 116u8, 145u8, 197u8, 12u8, 234u8, 222u8, 173u8,
        87u8, 212u8, 205u8, 89u8, 126u8, 243u8, 130u8, 29u8, 101u8, 195u8, 40u8, 116u8, 76u8,
        116u8, 229u8, 83u8, 218u8, 194u8, 109u8,
    ],
    [
        9u8, 24u8, 212u8, 107u8, 245u8, 45u8, 152u8, 176u8, 52u8, 65u8, 63u8, 74u8, 26u8, 28u8,
        65u8, 89u8, 78u8, 122u8, 122u8, 63u8, 106u8, 224u8, 140u8, 180u8, 61u8, 26u8, 42u8, 35u8,
        14u8, 25u8, 89u8, 239u8,
    ],
    [
        27u8, 190u8, 176u8, 27u8, 76u8, 71u8, 158u8, 205u8, 231u8, 105u8, 23u8, 100u8, 94u8, 64u8,
        77u8, 250u8, 46u8, 38u8, 249u8, 13u8, 10u8, 252u8, 90u8, 101u8, 18u8, 133u8, 19u8, 173u8,
        55u8, 92u8, 95u8, 242u8,
    ],
    [
        47u8, 104u8, 161u8, 197u8, 142u8, 37u8, 126u8, 66u8, 161u8, 122u8, 108u8, 97u8, 223u8,
        245u8, 85u8, 30u8, 213u8, 96u8, 185u8, 146u8, 42u8, 177u8, 25u8, 213u8, 172u8, 142u8, 24u8,
        76u8, 151u8, 52u8, 234u8, 217u8,
    ],
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingester::persist::{BATCH_STATE_TREE_HEIGHT, LEGACY_TREE_HEIGHT};

    fn node_index_to_leaf_index(index: i64, tree_height: u32) -> i64 {
        index - 2_i64.pow(get_level_by_node_index(index, tree_height) as u32)
    }

    #[test]
    fn test_get_level_by_node_index() {
        // Tree of height 3 (root level is 0, max is 3)
        // Node indices in a binary tree: [1, 2, 3, 4, 5, 6, 7]
        assert_eq!(get_level_by_node_index(1, BATCH_STATE_TREE_HEIGHT), 0); // Root node
        assert_eq!(get_level_by_node_index(2, BATCH_STATE_TREE_HEIGHT), 1); // Level 1, left child of root
        assert_eq!(get_level_by_node_index(3, BATCH_STATE_TREE_HEIGHT), 1); // Level 1, right child of root
        assert_eq!(get_level_by_node_index(4, BATCH_STATE_TREE_HEIGHT), 2); // Level 2, left child of node 2
        assert_eq!(get_level_by_node_index(5, BATCH_STATE_TREE_HEIGHT), 2); // Level 2, right child of node 2
        assert_eq!(get_level_by_node_index(6, BATCH_STATE_TREE_HEIGHT), 2); // Level 2, left child of node 3
        assert_eq!(get_level_by_node_index(7, BATCH_STATE_TREE_HEIGHT), 2); // Level 2, right child of node 3
    }

    // Test helper to convert byte arrays to hex strings for easier debugging
    fn bytes_to_hex(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<String>>()
            .join("")
    }

    // Helper to verify node index calculations
    fn verify_node_index_conversion(leaf_index: u32, tree_height: u32) -> bool {
        let node_index = leaf_index_to_node_index(leaf_index, tree_height);
        let recovered_leaf_index = node_index_to_leaf_index(node_index, tree_height);
        recovered_leaf_index == leaf_index as i64
    }

    #[test]
    fn test_zero_bytes_consistency() {
        // Verify that each level's hash in ZERO_BYTES is correctly computed from its children
        for level in (1..MAX_HEIGHT).rev() {
            let parent_hash = compute_parent_hash(
                ZERO_BYTES[level - 1].to_vec(),
                ZERO_BYTES[level - 1].to_vec(),
            )
            .unwrap();

            assert_eq!(
                parent_hash,
                ZERO_BYTES[level].to_vec(),
                "Zero bytes hash mismatch at level {}\nComputed: {}\nExpected: {}",
                level,
                bytes_to_hex(&parent_hash),
                bytes_to_hex(&ZERO_BYTES[level])
            );
        }
    }

    #[ignore = "todo check whether to keep"]
    #[test]
    fn test_debug_leaf_zero() {
        let leaf_index = 0u32;
        let tree_height = 32u32;
        let node_index = leaf_index_to_node_index(leaf_index, tree_height);
        let recovered_leaf_index = node_index_to_leaf_index(node_index, tree_height);

        println!("leaf_index: {}", leaf_index);
        println!("node_index: {}", node_index);
        println!(
            "level: {}",
            get_level_by_node_index(node_index, tree_height)
        );
        println!("recovered_leaf_index: {}", recovered_leaf_index);

        assert_eq!(recovered_leaf_index, leaf_index as i64);
    }

    #[ignore = "todo check whether to keep"]
    #[test]
    fn test_debug_max_leaf() {
        let leaf_index = u32::MAX;
        let tree_height = 32u32;
        let node_index = leaf_index_to_node_index(leaf_index, tree_height);
        let recovered_leaf_index = node_index_to_leaf_index(node_index, tree_height);

        println!("max test:");
        println!("leaf_index: {} (u32)", leaf_index);
        println!("node_index: {} (i64)", node_index);
        println!("2^(tree_height-1): {} (i64)", 2_i64.pow(tree_height - 1));
        println!(
            "level: {}",
            get_level_by_node_index(node_index, tree_height)
        );
        println!("recovered_leaf_index: {} (i64)", recovered_leaf_index);

        assert_eq!(recovered_leaf_index, leaf_index as i64);
    }

    #[ignore = "todo check whether to keep"]
    #[test]
    fn test_leaf_index_conversions() {
        let test_cases = vec![
            (0u32, 32u32),          // First leaf in height 32 tree
            (1u32, 32u32),          // Second leaf
            (4294967295u32, 32u32), // Last possible leaf in u32
            (2147483647u32, 32u32), // i32::MAX
            (2147483648u32, 32u32), // i32::MAX + 1
            (0u32, 3u32),           // Small tree test
            (1u32, 3u32),
            (2u32, 3u32),
            (3u32, 3u32),
        ];

        for (leaf_index, tree_height) in test_cases {
            assert!(
                verify_node_index_conversion(leaf_index, tree_height),
                "Conversion failed for leaf_index={}, tree_height={}",
                leaf_index,
                tree_height
            );
        }
    }

    #[test]
    fn test_proof_validation_components() {
        // Test case for first non-existent leaf (index 0)
        let test_leaf_index = 0u32;
        let tree_height = 32u32;
        // Create proof components
        let node_index = leaf_index_to_node_index(test_leaf_index, tree_height);
        let proof_path = get_proof_path(node_index, false);

        println!("Test leaf index: {}", test_leaf_index);
        println!("Node index: {}", node_index);
        println!("Proof path: {:?}", proof_path);

        // Verify proof path length
        assert_eq!(proof_path.len(), tree_height as usize);

        // Test level calculation for proof path nodes
        for &idx in &proof_path {
            let level = get_level_by_node_index(idx, tree_height);
            println!("Node {} is at level {}", idx, level);
            assert!(level < tree_height as i64);
        }

        // Manually compute root hash using proof path
        let mut current_hash = ZERO_BYTES[0].to_vec(); // Start with leaf level zero bytes

        for (idx, _) in proof_path.iter().enumerate() {
            let is_left = (node_index >> idx) & 1 == 0;
            let sibling_hash = ZERO_BYTES[idx].to_vec();

            let (left_child, right_child) = if is_left {
                (current_hash.clone(), sibling_hash)
            } else {
                (sibling_hash, current_hash.clone())
            };

            current_hash = compute_parent_hash(left_child, right_child).unwrap();

            println!(
                "Level {}: Computed hash: {}",
                idx,
                bytes_to_hex(&current_hash)
            );
            println!(
                "         Expected:     {}",
                bytes_to_hex(&ZERO_BYTES[idx + 1])
            );

            // Verify against precalculated ZERO_BYTES
            assert_eq!(
                current_hash,
                ZERO_BYTES[idx + 1].to_vec(),
                "Hash mismatch at level {}",
                idx + 1
            );
        }
    }

    #[test]
    fn test_validate_proof() {
        let test_leaf_index = 0u32;
        let merkle_tree = SerializablePubkey::try_from(vec![0u8; 32]).unwrap();

        // Create a proof for testing
        let mut proof = Vec::new();
        for i in 0..31 {
            // One less than tree height since root is separate
            proof.push(Hash::try_from(ZERO_BYTES[i].to_vec()).unwrap());
        }

        let proof_context = MerkleProofWithContext {
            proof,
            root: Hash::try_from(ZERO_BYTES[31].to_vec()).unwrap(),
            leafIndex: test_leaf_index,
            hash: Hash::try_from(ZERO_BYTES[0].to_vec()).unwrap(),
            merkleTree: merkle_tree,
            rootSeq: 0,
        };

        // Validate the proof
        let result = validate_proof(&proof_context);
        assert!(result.is_ok(), "Proof validation failed: {:?}", result);
    }

    #[test]
    fn test_validate_leaf_index() {
        // Test for legacy tree height
        assert!(validate_leaf_index(0, LEGACY_TREE_HEIGHT));
        assert!(validate_leaf_index((1 << 26) - 1, LEGACY_TREE_HEIGHT));
        assert!(!validate_leaf_index(1 << 26, LEGACY_TREE_HEIGHT));

        // Test for batch state tree height
        assert!(validate_leaf_index(0, BATCH_STATE_TREE_HEIGHT));
        // assert!(validate_leaf_index((1 << 32) - 1, BATCH_STATE_TREE_HEIGHT));
    }

    #[test]
    fn test_merkle_proof_length() {
        assert_eq!(get_merkle_proof_length(LEGACY_TREE_HEIGHT), 26);
        assert_eq!(get_merkle_proof_length(BATCH_STATE_TREE_HEIGHT), 32);
    }
}
