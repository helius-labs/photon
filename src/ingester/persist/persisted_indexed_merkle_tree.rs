use std::collections::{BTreeMap, HashMap};

use ark_bn254::Fr;
use light_poseidon::Poseidon;
use sea_orm::{
    sea_query::OnConflict, ConnectionTrait, DatabaseBackend, DatabaseConnection,
    DatabaseTransaction, EntityTrait, Set, Statement, TransactionTrait,
};

use crate::{
    api::error::PhotonApiError,
    common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey},
    dao::generated::indexed_trees,
    ingester::error::IngesterError,
};
use light_poseidon::PoseidonBytesHasher;

use super::persisted_state_tree::{
    get_multiple_compressed_leaf_proofs_from_full_leaf_info, persist_leaf_nodes, LeafNode,
    MerkleProofWithContext,
};

fn compute_range_node_hash(node: &indexed_trees::Model) -> Result<Hash, IngesterError> {
    let mut poseidon = Poseidon::<Fr>::new_circom(3).unwrap();
    let index_to_bytes = node.leaf_index.to_be_bytes();
    Hash::try_from(
        poseidon
            .hash_bytes_be(&[&node.value, &index_to_bytes, &node.next_value])
            .map_err(|e| IngesterError::ParserError(format!("Failed  to compute hash: {}", e)))
            .map(|x| x.to_vec())?,
    )
    .map_err(|e| IngesterError::ParserError(format!("Failed to convert hash: {}", e)))
}

pub async fn get_range_proof(
    conn: &DatabaseConnection,
    tree: Vec<u8>,
    tree_height: u32,
    value: Vec<u8>,
) -> Result<(indexed_trees::Model, MerkleProofWithContext), PhotonApiError> {
    let btree = query_next_smallest_elements(conn, vec![value.clone()], tree.clone())
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to query next smallest elements: {}",
                e
            ))
        })?;
    let range_node = btree
        .values()
        .into_iter()
        .next()
        .ok_or(PhotonApiError::RecordNotFound(
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
        seq: 0,
    };
    let node_index = leaf_node.node_index(tree_height);

    let leaf_proofs: Vec<MerkleProofWithContext> =
        get_multiple_compressed_leaf_proofs_from_full_leaf_info(
            conn,
            vec![(leaf_node, node_index)],
        )
        .await?;

    let leaf_proof = leaf_proofs
        .into_iter()
        .next()
        .ok_or(PhotonApiError::RecordNotFound(
            "No leaf proof found".to_string(),
        ))?;

    Ok((range_node.clone(), leaf_proof))
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
            IngesterError::DatabaseError(format!("Failed to lock state_trees table: {}", e))
        })?;
    }

    let index_stmt = Statement::from_string(
        txn.get_database_backend(),
        // TODO: Use parametrized queries instead
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
        let zeroeth_element = indexed_trees::Model {
            tree: tree.clone(),
            leaf_index: 0,
            value: vec![0],
            next_index: 0,
            next_value: vec![0],
        };
        elements_to_update.insert(0, zeroeth_element.clone());
        indexed_tree.insert(zeroeth_element.value.clone(), zeroeth_element);
    }

    for value in values.clone() {
        current_index += 1;

        let mut indexed_element = indexed_trees::Model {
            tree: tree.clone(),
            leaf_index: current_index,
            value: value.clone(),
            next_index: 0,
            next_value: vec![],
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
                seq: 0,
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
        };
        indexed_tree.insert(model.value.clone(), model);
    }
    Ok(indexed_tree)
}

fn format_bytes(bytes: Vec<u8>, database_backend: DatabaseBackend) -> String {
    let hex_bytes = hex::encode(bytes);
    match database_backend {
        DatabaseBackend::Postgres => format!("E'\\\\x{}'", hex_bytes),
        DatabaseBackend::Sqlite => format!("x'{}'", hex_bytes),
        _ => unimplemented!(),
    }
}
