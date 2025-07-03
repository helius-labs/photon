use crate::api::error::PhotonApiError;
use crate::common::format_bytes;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::indexed_trees;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::indexed_merkle_tree::{
    compute_hash_by_tree_height, compute_range_node_hash_v1, get_top_element,
    get_zeroeth_exclusion_range, get_zeroeth_exclusion_range_v1,
};
use crate::ingester::persist::persisted_state_tree::ZERO_BYTES;
use crate::ingester::persist::{
    compute_parent_hash, get_multiple_compressed_leaf_proofs_from_full_leaf_info, LeafNode,
    MerkleProofWithContext, TREE_HEIGHT_V1,
};
use light_compressed_account::TreeType;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, Statement, TransactionTrait};
use std::collections::BTreeMap;

pub async fn get_exclusion_range_with_proof_v2(
    txn: &DatabaseTransaction,
    tree: Vec<u8>,
    tree_height: u32,
    value: Vec<u8>,
) -> Result<(indexed_trees::Model, MerkleProofWithContext), PhotonApiError> {
    let btree = query_next_smallest_elements(txn, vec![value.clone()], tree.clone())
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to query next smallest elements: {}",
                e
            ))
        })?;
    if btree.is_empty() {
        return proof_for_empty_tree(tree, tree_height);
    }

    let range_node = btree.values().next().ok_or(PhotonApiError::RecordNotFound(
        "No range proof found".to_string(),
    ))?;
    let hash = compute_hash_by_tree_height(range_node, tree_height)?;

    let leaf_node = LeafNode {
        tree: SerializablePubkey::try_from(range_node.tree.clone()).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to serialize pubkey: {}", e))
        })?,
        leaf_index: range_node.leaf_index as u32,
        hash,
        seq: range_node.seq.map(|x| x as u32),
    };
    let node_index = leaf_node.node_index(tree_height);

    let leaf_proofs: Vec<MerkleProofWithContext> =
        get_multiple_compressed_leaf_proofs_from_full_leaf_info(txn, vec![(leaf_node, node_index)])
            .await
            .map_err(|proof_error| {
                let tree_pubkey = match SerializablePubkey::try_from(range_node.tree.clone()) {
                    Ok(pubkey) => pubkey,
                    Err(e) => {
                        log::error!("Failed to serialize tree pubkey: {}", e);
                        return proof_error;
                    }
                };
                let value_pubkey = match SerializablePubkey::try_from(range_node.value.clone()) {
                    Ok(pubkey) => pubkey,
                    Err(e) => {
                        log::error!("Failed to serialize value pubkey: {}", e);
                        return proof_error;
                    }
                };
                log::error!(
                    "Failed to get multiple compressed leaf proofs for {:?} for value {:?}: {}",
                    tree_pubkey,
                    value_pubkey,
                    proof_error
                );
                proof_error
            })?;

    let leaf_proof = leaf_proofs
        .into_iter()
        .next()
        .ok_or(PhotonApiError::RecordNotFound(
            "No leaf proof found".to_string(),
        ))?;

    Ok((range_node.clone(), leaf_proof))
}

fn proof_for_empty_tree(
    tree: Vec<u8>,
    tree_height: u32,
) -> Result<(indexed_trees::Model, MerkleProofWithContext), PhotonApiError> {
    let tree_bytes: [u8; 32] = tree
        .as_slice()
        .try_into()
        .map_err(|_| PhotonApiError::UnexpectedError("Invalid tree pubkey length".to_string()))?;
    let tree_type = TreeInfo::get_tree_type_from_bytes(&tree_bytes);

    let root_seq = match tree_type {
        TreeType::AddressV1 => 3,
        _ => 0,
    };

    proof_for_empty_tree_with_seq(tree, tree_height, root_seq)
}

fn proof_for_empty_tree_with_seq(
    tree: Vec<u8>,
    tree_height: u32,
    root_seq: u64,
) -> Result<(indexed_trees::Model, MerkleProofWithContext), PhotonApiError> {
    let mut proof: Vec<Hash> = vec![];

    if tree_height == TREE_HEIGHT_V1 + 1 {
        let top_element = get_top_element(tree.clone());
        let top_element_hash = compute_hash_by_tree_height(&top_element, tree_height)?;
        proof.push(top_element_hash);

        for i in 1..(tree_height - 1) {
            let hash = Hash::from(ZERO_BYTES[i as usize]);
            proof.push(hash);
        }
    } else {
        for i in 0..(tree_height - 1) {
            let hash = Hash::from(ZERO_BYTES[i as usize]);
            proof.push(hash);
        }
    }

    let zeroeth_element = if tree_height == TREE_HEIGHT_V1 + 1 {
        get_zeroeth_exclusion_range_v1(tree.clone())
    } else {
        get_zeroeth_exclusion_range(tree.clone())
    };
    let zeroeth_element_hash = compute_hash_by_tree_height(&zeroeth_element, tree_height)?;

    let mut root = zeroeth_element_hash.clone().to_vec();

    for elem in proof.iter() {
        root = compute_parent_hash(root, elem.to_vec())
            .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to compute hash: {e}")))?;
    }

    let merkle_proof = MerkleProofWithContext {
        proof,
        root: Hash::try_from(root.clone())
            .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to convert hash: {e}")))?,
        leaf_index: 0,
        hash: zeroeth_element_hash,
        merkle_tree: SerializablePubkey::try_from(tree.clone()).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to serialize pubkey: {e}"))
        })?,
        root_seq,
    };

    merkle_proof.validate()?;
    Ok((zeroeth_element, merkle_proof))
}

pub async fn get_exclusion_range_with_proof_v1(
    txn: &DatabaseTransaction,
    tree: Vec<u8>,
    tree_height: u32,
    value: Vec<u8>,
) -> Result<(indexed_trees::Model, MerkleProofWithContext), PhotonApiError> {
    let btree = query_next_smallest_elements(txn, vec![value.clone()], tree.clone())
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to query next smallest elements: {e}"))
        })?;

    if btree.is_empty() {
        return proof_for_empty_tree(tree, tree_height);
    }

    let range_node = btree.values().next().ok_or(PhotonApiError::RecordNotFound(
        "No range proof found".to_string(),
    ))?;
    let hash = compute_range_node_hash_v1(range_node)
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to compute hash: {e}")))?;

    let leaf_node = LeafNode {
        tree: SerializablePubkey::try_from(range_node.tree.clone()).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to serialize pubkey: {e}"))
        })?,
        leaf_index: range_node.leaf_index as u32,
        hash,
        seq: range_node.seq.map(|x| x as u32),
    };
    let node_index = leaf_node.node_index(tree_height);

    let leaf_proofs: Vec<MerkleProofWithContext> =
        get_multiple_compressed_leaf_proofs_from_full_leaf_info(txn, vec![(leaf_node, node_index)])
            .await
            .map_err(|proof_error| {
                let tree_pubkey = match SerializablePubkey::try_from(range_node.tree.clone()) {
                    Ok(pubkey) => pubkey,
                    Err(e) => {
                        log::error!("Failed to serialize tree pubkey: {e}");
                        return proof_error;
                    }
                };
                let value_pubkey = match SerializablePubkey::try_from(range_node.value.clone()) {
                    Ok(pubkey) => pubkey,
                    Err(e) => {
                        log::error!("Failed to serialize value pubkey: {e}");
                        return proof_error;
                    }
                };
                log::error!(
                    "Failed to get multiple compressed leaf proofs for {tree_pubkey:?} for value {value_pubkey:?}: {proof_error}"
                );
                proof_error
            })?;

    let leaf_proof = leaf_proofs
        .into_iter()
        .next()
        .ok_or(PhotonApiError::RecordNotFound(
            "No leaf proof found".to_string(),
        ))?;

    Ok((range_node.clone(), leaf_proof))
}

pub async fn query_next_smallest_elements<T>(
    txn_or_conn: &T,
    values: Vec<Vec<u8>>,
    tree: Vec<u8>,
) -> Result<BTreeMap<Vec<u8>, indexed_trees::Model>, IngesterError>
where
    T: ConnectionTrait + TransactionTrait,
{
    let response = match txn_or_conn.get_database_backend() {
        // HACK: I am executing SQL queries one by one in a loop because I am getting a weird syntax
        //       error when I am using parentheses.
        DatabaseBackend::Postgres => {
            let sql_statements = values.iter().map(|value| {
                format!(
                    "( SELECT * FROM indexed_trees WHERE tree = {} AND value < {} ORDER BY value DESC LIMIT 1 )",
                    format_bytes(tree.clone(), txn_or_conn.get_database_backend()),
                    format_bytes(value.clone(), txn_or_conn.get_database_backend())
                )
            });
            let full_query = sql_statements.collect::<Vec<String>>().join(" UNION ALL ");
            txn_or_conn
                .query_all(Statement::from_string(
                    txn_or_conn.get_database_backend(),
                    full_query,
                ))
                .await
                .map_err(|e| {
                    IngesterError::DatabaseError(format!("Failed to execute indexed query: {e}"))
                })?
        }
        DatabaseBackend::Sqlite => {
            let mut response = vec![];

            for value in values {
                let full_query = format!(
                    "SELECT * FROM indexed_trees WHERE tree = {} AND value < {} ORDER BY value DESC LIMIT 1",
                    format_bytes(tree.clone(), txn_or_conn.get_database_backend()),
                    format_bytes(value.clone(), txn_or_conn.get_database_backend())
                );
                let result = txn_or_conn
                    .query_all(Statement::from_string(
                        txn_or_conn.get_database_backend(),
                        full_query,
                    ))
                    .await
                    .map_err(|e| {
                        IngesterError::DatabaseError(format!(
                            "Failed to execute indexed query: {e}"
                        ))
                    })?;
                response.extend(result);
            }
            response
        }
        _ => unimplemented!(),
    };

    let mut indexed_tree: BTreeMap<Vec<u8>, indexed_trees::Model> = BTreeMap::new();
    for row in response {
        let model = indexed_trees::Model {
            tree: row.try_get("", "tree")?,
            leaf_index: row.try_get("", "leaf_index")?,
            value: row.try_get("", "value")?,
            next_index: row.try_get("", "next_index")?,
            next_value: row.try_get("", "next_value")?,
            seq: row.try_get("", "seq")?,
        };
        indexed_tree.insert(model.value.clone(), model);
    }
    Ok(indexed_tree)
}
