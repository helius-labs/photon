use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
};

use ark_bn254::Fr;
use itertools::Itertools;
use light_poseidon::Poseidon;
use log::info;
use num_bigint::BigUint;
use sea_orm::{
    sea_query::OnConflict, ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction,
    EntityTrait, QueryFilter, QueryTrait, Set, Statement, TransactionTrait,
};
use solana_pubkey::Pubkey;

use super::{
    compute_parent_hash, get_multiple_compressed_leaf_proofs_from_full_leaf_info,
    persisted_state_tree::ZERO_BYTES, MerkleProofWithContext, MAX_SQL_INSERTS,
};
use crate::ingester::persist::leaf_node::{persist_leaf_nodes, LeafNode, TREE_HEIGHT_V1};
use crate::{
    api::error::PhotonApiError,
    common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey},
    dao::generated::{indexed_trees, state_trees},
    ingester::{
        error::IngesterError,
        parser::{indexer_events::RawIndexedElement, state_update::IndexedTreeLeafUpdate},
    },
};
use lazy_static::lazy_static;
use light_poseidon::PoseidonBytesHasher;

lazy_static! {
    pub static ref HIGHEST_ADDRESS_PLUS_ONE: BigUint = BigUint::from_str(
        "452312848583266388373324160190187140051835877600158453279131187530910662655"
    )
    .unwrap();
}

pub fn compute_range_node_hash(node: &indexed_trees::Model) -> Result<Hash, IngesterError> {
    let mut poseidon = Poseidon::<Fr>::new_circom(2).unwrap();
    Hash::try_from(
        poseidon
            .hash_bytes_be(&[&node.value, &node.next_value])
            .map_err(|e| IngesterError::ParserError(format!("Failed  to compute hash: {}", e)))
            .map(|x| x.to_vec())?,
    )
    .map_err(|e| IngesterError::ParserError(format!("Failed to convert hash: {}", e)))
}

pub fn compute_range_node_hash_v1(node: &indexed_trees::Model) -> Result<Hash, IngesterError> {
    let mut poseidon = Poseidon::<Fr>::new_circom(3).unwrap();
    let next_index = node.next_index.to_be_bytes();
    Hash::try_from(
        poseidon
            .hash_bytes_be(&[&node.value, &next_index, &node.next_value])
            .map_err(|e| IngesterError::ParserError(format!("Failed  to compute hash: {}", e)))
            .map(|x| x.to_vec())?,
    )
    .map_err(|e| IngesterError::ParserError(format!("Failed to convert hash: {}", e)))
}

pub fn compute_range_node_hash_for_subtrees(
    node: &indexed_trees::Model,
) -> Result<Hash, IngesterError> {
    let mut poseidon = Poseidon::<Fr>::new_circom(3).unwrap();
    let next_index = node.next_index.to_be_bytes();
    Hash::try_from(
        poseidon
            .hash_bytes_be(&[&node.value, &next_index, &node.next_value])
            .map_err(|e| IngesterError::ParserError(format!("Failed  to compute hash: {}", e)))
            .map(|x| x.to_vec())?,
    )
    .map_err(|e| IngesterError::ParserError(format!("Failed to convert hash: {}", e)))
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
    let hash = compute_range_node_hash(range_node)
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
    let zeroeth_element = get_zeroeth_exclusion_range(tree.clone());
    let zeroeth_element_hash = compute_range_node_hash(&zeroeth_element)
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to compute hash: {}", e)))?;

    let mut proof: Vec<Hash> = vec![];
    for i in 0..(tree_height - 1) {
        let hash = Hash::try_from(ZERO_BYTES[i as usize]).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to convert hash: {}", e))
        })?;
        proof.push(hash);
    }

    let mut root = zeroeth_element_hash.clone().to_vec();

    for elem in proof.iter() {
        root = compute_parent_hash(root, elem.to_vec()).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to compute hash: {}", e))
        })?;
    }

    let merkle_proof = MerkleProofWithContext {
        proof,
        root: Hash::try_from(root).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to convert hash: {}", e))
        })?,
        leaf_index: 0,
        hash: zeroeth_element_hash,
        merkle_tree: SerializablePubkey::try_from(tree.clone()).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to serialize pubkey: {}", e))
        })?,
        root_seq: if TREE_HEIGHT_V1 == tree_height { 3 } else { 0 },
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
            PhotonApiError::UnexpectedError(format!(
                "Failed to query next smallest elements: {}",
                e
            ))
        })?;

    if btree.is_empty() {
        let zeroeth_element = get_zeroeth_exclusion_range_v1(tree.clone());
        let zeroeth_element_hash = compute_range_node_hash_v1(&zeroeth_element).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to compute hash: {}", e))
        })?;
        let top_element = get_top_element(tree.clone());
        let top_element_hash = compute_range_node_hash_v1(&top_element).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to compute hash: {}", e))
        })?;
        let mut proof: Vec<Hash> = vec![top_element_hash.clone()];
        for i in 1..(tree_height - 1) {
            let hash = Hash::try_from(ZERO_BYTES[i as usize]).map_err(|e| {
                PhotonApiError::UnexpectedError(format!("Failed to convert hash: {}", e))
            })?;
            proof.push(hash);
        }
        let mut root = zeroeth_element_hash.clone().to_vec();

        for elem in proof.iter() {
            root = compute_parent_hash(root, elem.to_vec()).map_err(|e| {
                PhotonApiError::UnexpectedError(format!("Failed to compute hash: {}", e))
            })?;
        }

        let merkle_proof = MerkleProofWithContext {
            proof,
            root: Hash::try_from(root).map_err(|e| {
                PhotonApiError::UnexpectedError(format!("Failed to convert hash: {}", e))
            })?,
            leaf_index: 0,
            hash: zeroeth_element_hash,
            merkle_tree: SerializablePubkey::try_from(tree.clone()).map_err(|e| {
                PhotonApiError::UnexpectedError(format!("Failed to serialize pubkey: {}", e))
            })?,
            root_seq: 3,
        };
        merkle_proof.validate()?;
        return Ok((zeroeth_element, merkle_proof));
    }

    let range_node = btree.values().next().ok_or(PhotonApiError::RecordNotFound(
        "No range proof found".to_string(),
    ))?;
    let hash = compute_range_node_hash_v1(range_node)
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

pub async fn update_indexed_tree_leaves_v1(
    txn: &DatabaseTransaction,
    mut indexed_leaf_updates: HashMap<(Pubkey, u64), IndexedTreeLeafUpdate>,
) -> Result<(), IngesterError> {
    let trees: HashSet<Pubkey> = indexed_leaf_updates.keys().map(|x| x.0).collect();
    for sdk_tree in trees {
        {
            let tree = Pubkey::new_from_array(sdk_tree.to_bytes());
            let leaf = get_zeroeth_exclusion_range(sdk_tree.to_bytes().to_vec());
            let leaf_update = indexed_leaf_updates.get(&(sdk_tree, leaf.leaf_index as u64));
            if leaf_update.is_none() {
                indexed_leaf_updates.insert(
                    (sdk_tree, leaf.leaf_index as u64),
                    IndexedTreeLeafUpdate {
                        tree,
                        hash: compute_range_node_hash(&leaf)
                            .map_err(|e| {
                                IngesterError::ParserError(format!("Failed to compute hash: {}", e))
                            })?
                            .0,
                        leaf: RawIndexedElement {
                            value: leaf.value.clone().try_into().map_err(|_e| {
                                IngesterError::ParserError(format!(
                                    "Failed to convert value to array {:?}",
                                    leaf.value
                                ))
                            })?,
                            next_index: leaf.next_index as usize,
                            next_value: leaf.next_value.try_into().map_err(|_e| {
                                IngesterError::ParserError(
                                    "Failed to convert next value to array".to_string(),
                                )
                            })?,
                            index: leaf.leaf_index as usize,
                        },
                        seq: 0,
                    },
                );
            }
        }
    }
    let chunks = indexed_leaf_updates
        .values()
        .chunks(MAX_SQL_INSERTS)
        .into_iter()
        .map(|x| x.collect_vec())
        .collect_vec();

    for chunk in chunks {
        let models = chunk.iter().map(|x| indexed_trees::ActiveModel {
            tree: Set(x.tree.to_bytes().to_vec()),
            leaf_index: Set(x.leaf.index as i64),
            value: Set(x.leaf.value.to_vec()),
            next_index: Set(x.leaf.next_index as i64),
            next_value: Set(x.leaf.next_value.to_vec()),
            seq: Set(Some(x.seq as i64)),
        });

        let mut query = indexed_trees::Entity::insert_many(models)
            .on_conflict(
                OnConflict::columns([
                    indexed_trees::Column::Tree,
                    indexed_trees::Column::LeafIndex,
                ])
                .update_columns([
                    indexed_trees::Column::Value,
                    indexed_trees::Column::NextIndex,
                    indexed_trees::Column::NextValue,
                    indexed_trees::Column::Seq,
                ])
                .to_owned(),
            )
            .build(txn.get_database_backend());

        query.sql = format!("{} WHERE excluded.seq >= indexed_trees.seq", query.sql);

        txn.execute(query).await.map_err(|e| {
            IngesterError::DatabaseError(format!("Failed to insert indexed tree elements: {}", e))
        })?;

        let state_tree_leaf_nodes = chunk
            .iter()
            .map(|x| {
                Ok(LeafNode {
                    tree: SerializablePubkey::try_from(x.tree).map_err(|e| {
                        IngesterError::DatabaseError(format!("Failed to serialize pubkey: {}", e))
                    })?,
                    leaf_index: x.leaf.index as u32,
                    hash: Hash::try_from(x.hash).map_err(|e| {
                        IngesterError::DatabaseError(format!("Failed to serialize hash: {}", e))
                    })?,
                    seq: Option::from(x.seq as u32),
                })
            })
            .collect::<Result<Vec<LeafNode>, IngesterError>>()?;

        persist_leaf_nodes(txn, state_tree_leaf_nodes, TREE_HEIGHT_V1 + 1).await?;
    }

    Ok(())
}

pub async fn multi_append(
    txn: &DatabaseTransaction,
    values: Vec<Vec<u8>>,
    tree: Vec<u8>,
    tree_height: u32,
) -> Result<(), IngesterError> {
    if txn.get_database_backend() == DatabaseBackend::Postgres {
        txn.execute(Statement::from_string(
            txn.get_database_backend(),
            "LOCK TABLE indexed_trees IN EXCLUSIVE MODE;".to_string(),
        ))
        .await
        .map_err(|e| {
            IngesterError::DatabaseError(format!("Failed to lock indexed_trees table: {}", e))
        })?;
    }

    let index_stmt = Statement::from_string(
        txn.get_database_backend(),
        format!(
            "SELECT leaf_index FROM indexed_trees WHERE tree = {} ORDER BY leaf_index DESC LIMIT 1",
            format_bytes(tree.clone(), txn.get_database_backend())
        ),
    );
    let max_index = txn.query_one(index_stmt).await.map_err(|e| {
        IngesterError::DatabaseError(format!("Failed to execute max index query: {}", e))
    })?;

    let mut current_index = match max_index {
        Some(row) => row.try_get("", "leaf_index").unwrap_or(0),
        None => 0,
    };

    let mut indexed_tree = query_next_smallest_elements(txn, values.clone(), tree.clone()).await?;
    let mut elements_to_update: HashMap<i64, indexed_trees::Model> = HashMap::new();

    if indexed_tree.is_empty() {
        {
            let model = get_zeroeth_exclusion_range(tree.clone());
            elements_to_update.insert(model.leaf_index, model.clone());
            indexed_tree.insert(model.value.clone(), model);
        }
    }

    for value in values.clone() {
        current_index += 1;
        let mut indexed_element = indexed_trees::Model {
            tree: tree.clone(),
            leaf_index: current_index,
            value: value.clone(),
            next_index: 0,
            next_value: vec![],
            seq: Some(0),
        };

        let next_largest = indexed_tree
            .range(..value.clone()) // This ranges from the start up to, but not including, `key`
            .next_back() // Gets the last element in the range, which is the largest key less than `key`
            .map(|(_, v)| v.clone());

        if let Some(mut next_largest) = next_largest {
            indexed_element.next_index = next_largest.next_index;
            indexed_element.next_value = next_largest.next_value.clone();

            next_largest.next_index = current_index;
            next_largest.next_value = value.clone();

            elements_to_update.insert(next_largest.leaf_index, next_largest.clone());
            indexed_tree.insert(next_largest.value.clone(), next_largest);
        }
        elements_to_update.insert(current_index, indexed_element.clone());
        indexed_tree.insert(value, indexed_element);
    }

    let active_elements = elements_to_update
        .values()
        .map(|x| indexed_trees::ActiveModel {
            tree: Set(tree.clone()),
            leaf_index: Set(x.leaf_index),
            value: Set(x.value.clone()),
            next_index: Set(x.next_index),
            next_value: Set(x.next_value.clone()),
            seq: Set(Some(0)),
        });

    indexed_trees::Entity::insert_many(active_elements)
        .on_conflict(
            OnConflict::columns([
                indexed_trees::Column::Tree,
                indexed_trees::Column::LeafIndex,
            ])
            .update_columns([
                indexed_trees::Column::NextIndex,
                indexed_trees::Column::NextValue,
            ])
            .to_owned(),
        )
        .exec(txn)
        .await
        .map_err(|e| {
            IngesterError::DatabaseError(format!("Failed to insert indexed tree elements: {}", e))
        })?;

    let leaf_nodes = elements_to_update
        .values()
        .map(|x| {
            Ok(LeafNode {
                tree: SerializablePubkey::try_from(x.tree.clone()).map_err(|e| {
                    IngesterError::DatabaseError(format!("Failed to serialize pubkey: {}", e))
                })?,
                leaf_index: x.leaf_index as u32,
                hash: compute_range_node_hash(x)?,
                seq: Some(0),
            })
        })
        .collect::<Result<Vec<LeafNode>, IngesterError>>()?;

    persist_leaf_nodes(txn, leaf_nodes, tree_height).await?;

    Ok(())
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
                    IngesterError::DatabaseError(format!("Failed to execute indexed query: {}", e))
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
                            "Failed to execute indexed query: {}",
                            e
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

pub fn format_bytes(bytes: Vec<u8>, database_backend: DatabaseBackend) -> String {
    let hex_bytes = hex::encode(bytes);
    match database_backend {
        DatabaseBackend::Postgres => format!("E'\\\\x{}'", hex_bytes),
        DatabaseBackend::Sqlite => format!("x'{}'", hex_bytes),
        _ => unimplemented!(),
    }
}

pub async fn validate_tree(db_conn: &sea_orm::DatabaseConnection, tree: SerializablePubkey) {
    info!("Fetching state tree nodes for {:?}...", tree);
    let models = state_trees::Entity::find()
        .filter(state_trees::Column::Tree.eq(tree.to_bytes_vec()))
        .all(db_conn)
        .await
        .unwrap();

    let node_to_model = models
        .iter()
        .map(|x| (x.node_idx, x.clone()))
        .collect::<HashMap<i64, state_trees::Model>>();

    info!("Fetched {} nodes", node_to_model.len());

    info!("Validating tree...");

    let mut count = 0;
    for model in node_to_model.values() {
        count += 1;
        if count % 1000 == 0 {
            info!("Validated {} nodes...", count);
        }
        if model.level > 0 {
            let node_index = model.node_idx;
            let child_level = model.level - 1;
            let left_child = node_to_model
                .get(&(node_index * 2))
                .map(|x| x.hash.clone())
                .unwrap_or(ZERO_BYTES[child_level as usize].to_vec());

            let right_child = node_to_model
                .get(&(node_index * 2 + 1))
                .map(|x| x.hash.clone())
                .unwrap_or(ZERO_BYTES[child_level as usize].to_vec());

            let node_hash_pretty = Hash::try_from(model.hash.clone()).unwrap();
            let left_child_pretty = Hash::try_from(left_child.clone()).unwrap();
            let right_child_pretty = Hash::try_from(right_child.clone()).unwrap();

            let parent_hash = compute_parent_hash(left_child, right_child).unwrap();

            assert_eq!(
                model.hash, parent_hash,
                "Unexpected parent hash. Level {}. Hash: {}, Left: {}, Right: {}",
                model.level, node_hash_pretty, left_child_pretty, right_child_pretty
            );
        }
    }
    info!("Finished validating tree");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zeroeth_element_hash_is_not_zero_bytes_0() {
        let dummy_tree_id = vec![1u8; 32];
        let zeroeth_element = get_zeroeth_exclusion_range(dummy_tree_id.clone());
        let zeroeth_element_hash_result = compute_range_node_hash(&zeroeth_element);
        assert!(
            zeroeth_element_hash_result.is_ok(),
            "Failed to compute zeroeth_element_hash: {:?}",
            zeroeth_element_hash_result.err()
        );
        let zeroeth_element_hash = zeroeth_element_hash_result.unwrap();

        let zero_hash_at_level_0 = ZERO_BYTES[0];
        assert_ne!(zeroeth_element_hash.to_vec(), zero_hash_at_level_0.to_vec(),);
    }
}
