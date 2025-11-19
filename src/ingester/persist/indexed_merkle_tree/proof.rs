use crate::api::error::PhotonApiError;
use crate::common::format_bytes;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::indexed_trees;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::indexed_merkle_tree::{
    compute_hash_by_tree_type, get_top_element, get_zeroeth_exclusion_range,
    get_zeroeth_exclusion_range_v1,
};
use crate::ingester::persist::persisted_state_tree::ZERO_BYTES;
use crate::ingester::persist::{
    compute_parent_hash, get_multiple_compressed_leaf_proofs_from_full_leaf_info, LeafNode,
    MerkleProofWithContext,
};
use light_compressed_account::TreeType;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseTransaction, Statement, TransactionTrait};
use std::collections::{BTreeMap, HashMap};

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
        return proof_for_empty_tree(txn, tree, tree_height).await;
    }

    let range_node = btree.values().next().ok_or(PhotonApiError::RecordNotFound(
        "No range proof found".to_string(),
    ))?;
    // Get tree type to determine which hash function to use
    let tree_pubkey = solana_pubkey::Pubkey::from(tree.clone().try_into().unwrap_or([0u8; 32]));
    let tree_type = TreeInfo::get_tree_type(txn, &tree_pubkey).await?;
    let hash = compute_hash_by_tree_type(range_node, tree_type)
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to compute hash: {}", e)))?;

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
        get_multiple_compressed_leaf_proofs_from_full_leaf_info(
            txn,
            vec![(leaf_node, node_index)],
            tree_height,
        )
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

async fn proof_for_empty_tree(
    txn: &DatabaseTransaction,
    tree: Vec<u8>,
    tree_height: u32,
) -> Result<(indexed_trees::Model, MerkleProofWithContext), PhotonApiError> {
    let pubkey: [u8; 32] = tree
        .as_slice()
        .try_into()
        .map_err(|_| PhotonApiError::UnexpectedError("Invalid tree pubkey length".to_string()))?;
    let tree_type = TreeInfo::get_tree_type_from_pubkey(txn, &pubkey).await?;

    let root_seq = match tree_type {
        TreeType::AddressV1 => 3,
        _ => 0,
    };

    proof_for_empty_tree_with_seq(tree, tree_height, root_seq, tree_type)
}

fn proof_for_empty_tree_with_seq(
    tree: Vec<u8>,
    tree_height: u32,
    root_seq: u64,
    tree_type: TreeType,
) -> Result<(indexed_trees::Model, MerkleProofWithContext), PhotonApiError> {
    let mut proof: Vec<Hash> = vec![];

    if matches!(tree_type, TreeType::AddressV1 | TreeType::StateV1) {
        let top_element = get_top_element(tree.clone());
        let top_element_hash = compute_hash_by_tree_type(&top_element, tree_type).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to compute hash: {}", e))
        })?;
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

    let zeroeth_element = if matches!(tree_type, TreeType::AddressV1 | TreeType::StateV1) {
        get_zeroeth_exclusion_range_v1(tree.clone())
    } else {
        get_zeroeth_exclusion_range(tree.clone())
    };
    let zeroeth_element_hash = compute_hash_by_tree_type(&zeroeth_element, tree_type)
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to compute hash: {}", e)))?;

    let mut root = zeroeth_element_hash.clone().to_vec();
    for elem in proof.iter() {
        root = compute_parent_hash(root, elem.to_vec()).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to compute hash: {e}"))
        })?;
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

/// Batched version of get_exclusion_range_with_proof_v2
/// Returns a HashMap mapping each input address to its (model, proof) tuple
pub async fn get_multiple_exclusion_ranges_with_proofs_v2(
    txn: &DatabaseTransaction,
    tree: Vec<u8>,
    tree_height: u32,
    addresses: Vec<Vec<u8>>,
    tree_type: TreeType,
) -> Result<HashMap<Vec<u8>, (indexed_trees::Model, MerkleProofWithContext)>, PhotonApiError> {
    if addresses.is_empty() {
        return Ok(HashMap::new());
    }

    let btree = query_next_smallest_elements(txn, addresses.clone(), tree.clone())
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to query next smallest elements: {}",
                e
            ))
        })?;

    let mut results = HashMap::new();
    let mut leaf_nodes_with_indices = Vec::new();
    let mut address_to_model: HashMap<Vec<u8>, indexed_trees::Model> = HashMap::new();

    // Process addresses that have range proofs
    for address in &addresses {
        let range_node = btree
            .values()
            .filter(|node| node.value < *address)
            .max_by(|a, b| a.value.cmp(&b.value));

        if let Some(range_node) = range_node {
            let hash = compute_hash_by_tree_type(range_node, tree_type).map_err(|e| {
                PhotonApiError::UnexpectedError(format!("Failed to compute hash: {}", e))
            })?;

            let leaf_node = LeafNode {
                tree: SerializablePubkey::try_from(range_node.tree.clone()).map_err(|e| {
                    PhotonApiError::UnexpectedError(format!("Failed to serialize pubkey: {}", e))
                })?,
                leaf_index: range_node.leaf_index as u32,
                hash,
                seq: range_node.seq.map(|x| x as u32),
            };
            let node_index = leaf_node.node_index(tree_height);
            leaf_nodes_with_indices.push((leaf_node, node_index));
            address_to_model.insert(address.clone(), range_node.clone());
        }
    }

    let leaf_proofs = if !leaf_nodes_with_indices.is_empty() {
        get_multiple_compressed_leaf_proofs_from_full_leaf_info(
            txn,
            leaf_nodes_with_indices,
            tree_height,
        )
        .await?
    } else {
        Vec::new()
    };

    for (address, model) in address_to_model {
        if let Some(proof) = leaf_proofs
            .iter()
            .find(|p| p.leaf_index == model.leaf_index as u32)
        {
            results.insert(address, (model, proof.clone()));
        }
    }

    let addresses_needing_empty_proof: Vec<Vec<u8>> = addresses
        .iter()
        .filter(|addr| !results.contains_key(*addr))
        .cloned()
        .collect();

    if !addresses_needing_empty_proof.is_empty() {
        let root_seq = match tree_type {
            TreeType::AddressV1 => 3,
            _ => 0,
        };

        let (empty_model, empty_proof) =
            proof_for_empty_tree_with_seq(tree.clone(), tree_height, root_seq, tree_type)?;

        for address in addresses_needing_empty_proof {
            results.insert(address, (empty_model.clone(), empty_proof.clone()));
        }
    }

    Ok(results)
}
