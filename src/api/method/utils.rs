use crate::dao::generated::{state_trees, utxos};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::dao::typedefs::hash::{Hash, ParseHashError};
use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

use super::super::error::PhotonApiError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Utxo {
    pub hash: Hash,
    pub account: Option<SerializablePubkey>,
    pub owner: SerializablePubkey,
    pub data: String,
    pub tree: Option<SerializablePubkey>,
    pub lamports: u64,
    pub slot_created: u64,
}

pub fn parse_utxo_model(utxo: utxos::Model) -> Result<Utxo, PhotonApiError> {
    Ok(Utxo {
        hash: utxo.hash.try_into()?,
        account: utxo.account.map(SerializablePubkey::try_from).transpose()?,
        #[allow(deprecated)]
        data: base64::encode(utxo.data),
        owner: utxo.owner.try_into()?,
        tree: utxo.tree.map(|tree| tree.try_into()).transpose()?,
        lamports: utxo.lamports as u64,
        slot_created: utxo.slot_updated as u64,
    })
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProofResponse {
    pub root: Hash,
    pub proof: Vec<Hash>,
}

pub fn build_full_proof(
    proof_nodes: Vec<state_trees::Model>,
    required_node_indices: Vec<i64>,
) -> Result<ProofResponse, ParseHashError> {
    let depth = required_node_indices.len();
    let mut full_proof = vec![vec![0; 32]; depth];

    // Important:
    // We assume that the proof_nodes are already sorted in descending order (by level).
    for node in proof_nodes {
        full_proof[node.level as usize] = node.hash
    }

    let hashes: Result<Vec<Hash>, _> = full_proof
        .iter()
        .map(|proof| Hash::try_from(proof.clone()))
        .collect();
    let hashes = hashes?;

    Ok(ProofResponse {
        root: hashes[depth - 1].clone(),
        proof: hashes[..(depth - 1)].to_vec(),
    })
}

#[test]
fn test_build_full_proof() {
    // Beautiful art.
    //           1
    //        /     \
    //       /       \
    //      2          3
    //     / \        /  \
    //    /   \      /    \
    //   4     5    6      7
    //  / \   / \  / \    / \
    // 8  9  10 11 12 13 14 15

    // Let's say we want to prove leaf = 12
    // and we have never indexed any data beyond leaf 12.
    // This means nodes 7, 13, 14, 15 are mising from the DB.
    let required_node_indices: Vec<i64> = vec![1, 2, 7, 13];

    // We don't care about these values for this test.
    let tree = vec![0; 32];
    let seq = 0;
    let slot_updated = 0;

    // These are the nodes we got from the DB.
    // DB response is sorted in ascending order by level (leaf to root; bottom to top).
    let root_node = state_trees::Model {
        id: 1,
        node_idx: 1,
        leaf_idx: None,
        level: 3,
        hash: vec![1; 32],
        tree: tree.clone(),
        seq,
        slot_updated,
    };
    let node_2 = state_trees::Model {
        id: 2,
        node_idx: 2,
        leaf_idx: None,
        level: 2,
        hash: vec![2; 32],
        tree: tree.clone(),
        seq,
        slot_updated,
    };
    let proof_nodes = vec![root_node.clone(), node_2.clone()];
    let ProofResponse { proof, root } =
        build_full_proof(proof_nodes, required_node_indices.clone()).unwrap();

    // The root hash should correspond to the first node.
    assert_eq!(root, Hash::try_from(root_node.hash).unwrap());
    // And the proof size should match the tree depth, minus one value (root hash).
    assert_eq!(proof.len(), required_node_indices.len() - 1);

    // The first two values in the proof correspond to node 13 and 7, respectively.
    // Neither have an indexed node in the DB, and should be replaced with an empty hash.
    //
    // The last value corresponds to node 2 which was indexed in the DB.
    assert_eq!(proof[0], Hash::try_from(vec![0; 32]).unwrap());
    assert_eq!(proof[1], Hash::try_from(vec![0; 32]).unwrap());
    assert_eq!(proof[2], Hash::try_from(node_2.hash).unwrap());
}
