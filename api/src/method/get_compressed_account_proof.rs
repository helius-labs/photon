use dao::generated::{state_trees, utxos};
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};
use serde::{Deserialize, Serialize};

use crate::error::PhotonApiError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountProofRequest {
    pub hash: Option<String>,
    pub account_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountProofResponse {
    pub hash: String,
    pub root: String,
    pub proof: Vec<String>,
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
        let hash_vec = bs58::decode(h)
            .into_vec()
            .map_err(|_| PhotonApiError::InvalidPubkey {
                field: "hash".to_string(),
            })?;
        leaf_hash = hash_vec;
    } else if let Some(a) = account_id {
        let acc_vec = bs58::decode(a)
            .into_vec()
            .map_err(|_| PhotonApiError::InvalidPubkey {
                field: "account_id".to_string(),
            })?;
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

    let ProofResponse { root, proof } = build_full_proof(proof_nodes, required_node_indices);
    let res = GetCompressedAccountProofResponse {
        hash: bs58::encode(leaf_node.hash).into_string(),
        root,
        proof,
    };
    Ok(Some(res))
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

struct ProofResponse {
    root: String,
    proof: Vec<String>,
}

fn build_full_proof(
    proof_nodes: Vec<state_trees::Model>,
    required_node_indices: Vec<i64>,
) -> ProofResponse {
    let depth = required_node_indices.len();
    let mut full_proof = vec![vec![0; 32]; depth];

    // Important:
    // We assume that the proof_nodes are already sorted in descending order (by level).
    for node in proof_nodes {
        full_proof[node.level as usize] = node.hash
    }

    let hashes: Vec<String> = full_proof
        .iter()
        .map(|h| bs58::encode(&h).into_string())
        .collect();

    ProofResponse {
        root: hashes[depth - 1].clone(),
        proof: hashes[..(depth - 1)].to_vec(),
    }
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
        build_full_proof(proof_nodes, required_node_indices.clone());

    // The root hash should correspond to the first node.
    assert_eq!(root, bs58::encode(root_node.hash).into_string());
    // And the proof size should match the tree depth, minus one value (root hash).
    assert_eq!(proof.len(), required_node_indices.len() - 1);

    // The first two values in the proof correspond to node 13 and 7, respectively.
    // Neither have an indexed node in the DB, and should be replaced with an empty hash.
    //
    // The last value corresponds to node 2 which was indexed in the DB.
    assert_eq!(proof[0], bs58::encode(vec![0; 32]).into_string());
    assert_eq!(proof[1], bs58::encode(vec![0; 32]).into_string());
    assert_eq!(proof[2], bs58::encode(node_2.hash).into_string());
}
