use std::collections::HashMap;

use super::{compute_parent_hash, persisted_state_tree::ZERO_BYTES, MAX_SQL_INSERTS};
use crate::common::format_bytes;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::indexed_merkle_tree::{
    compute_hash_with_cache, compute_range_node_hash_v1, compute_range_node_hash_v2,
    get_top_element, get_zeroeth_exclusion_range, get_zeroeth_exclusion_range_v1,
    query_next_smallest_elements,
};
use crate::ingester::persist::leaf_node::{persist_leaf_nodes, LeafNode};
use crate::{
    common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey},
    dao::generated::{indexed_trees, state_trees},
    ingester::{
        error::IngesterError,
        parser::{indexer_events::RawIndexedElement, state_update::IndexedTreeLeafUpdate},
    },
};
use itertools::Itertools;
use light_compressed_account::TreeType;
use log::info;
use sea_orm::{
    sea_query::OnConflict, ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction,
    EntityTrait, QueryFilter, QueryTrait, Set, Statement,
};
use solana_pubkey::Pubkey;
use solana_signature::Signature;

/// Ensures the zeroeth element (leaf_index 0) exists if not already present
fn ensure_zeroeth_element_exists(
    indexed_leaf_updates: &mut HashMap<(Pubkey, u64), IndexedTreeLeafUpdate>,
    sdk_tree: Pubkey,
    tree: Pubkey,
    tree_type: TreeType,
) -> Result<(), IngesterError> {
    let zeroeth_update = indexed_leaf_updates.get(&(sdk_tree, 0));
    if zeroeth_update.is_none() {
        let (zeroeth_leaf, zeroeth_hash) = match &tree_type {
            TreeType::AddressV1 => {
                let leaf = get_zeroeth_exclusion_range_v1(sdk_tree.to_bytes().to_vec());
                let hash = compute_range_node_hash_v1(&leaf).map_err(|e| {
                    IngesterError::ParserError(format!(
                        "Failed to compute zeroeth element hash: {}",
                        e
                    ))
                })?;
                (leaf, hash)
            }
            _ => {
                let leaf = get_zeroeth_exclusion_range(sdk_tree.to_bytes().to_vec());
                let hash = compute_range_node_hash_v2(&leaf).map_err(|e| {
                    IngesterError::ParserError(format!(
                        "Failed to compute zeroeth element hash: {}",
                        e
                    ))
                })?;
                (leaf, hash)
            }
        };

        indexed_leaf_updates.insert(
            (sdk_tree, zeroeth_leaf.leaf_index as u64),
            IndexedTreeLeafUpdate {
                tree,
                tree_type,
                hash: zeroeth_hash.0,
                leaf: RawIndexedElement {
                    value: zeroeth_leaf.value.clone().try_into().map_err(|_e| {
                        IngesterError::ParserError(format!(
                            "Failed to convert zeroeth element value to array {:?}",
                            zeroeth_leaf.value
                        ))
                    })?,
                    next_index: zeroeth_leaf.next_index as usize,
                    next_value: zeroeth_leaf.next_value.try_into().map_err(|_e| {
                        IngesterError::ParserError(
                            "Failed to convert zeroeth element next value to array".to_string(),
                        )
                    })?,
                    index: zeroeth_leaf.leaf_index as usize,
                },
                seq: 0,
                signature: Signature::from([0; 64]), // Placeholder for synthetic element
            },
        );
    }
    Ok(())
}

/// Ensures the top element (leaf_index 1) exists for V1 trees if not already present
fn ensure_top_element_exists(
    indexed_leaf_updates: &mut HashMap<(Pubkey, u64), IndexedTreeLeafUpdate>,
    sdk_tree: Pubkey,
    tree: Pubkey,
    tree_type: TreeType,
) -> Result<(), IngesterError> {
    // Check if top element (leaf_index 1) is missing and insert it if needed - ONLY for V1 trees
    if matches!(tree_type, TreeType::AddressV1) {
        let top_update = indexed_leaf_updates.get(&(sdk_tree, 1));
        if top_update.is_none() {
            let top_leaf = get_top_element(sdk_tree.to_bytes().to_vec());
            let top_hash = compute_range_node_hash_v1(&top_leaf).map_err(|e| {
                IngesterError::ParserError(format!("Failed to compute top element hash: {}", e))
            })?;

            indexed_leaf_updates.insert(
                (sdk_tree, top_leaf.leaf_index as u64),
                IndexedTreeLeafUpdate {
                    tree,
                    tree_type,
                    hash: top_hash.0,
                    leaf: RawIndexedElement {
                        value: top_leaf.value.clone().try_into().map_err(|_e| {
                            IngesterError::ParserError(format!(
                                "Failed to convert top element value to array {:?}",
                                top_leaf.value
                            ))
                        })?,
                        next_index: top_leaf.next_index as usize,
                        next_value: top_leaf.next_value.try_into().map_err(|_e| {
                            IngesterError::ParserError(
                                "Failed to convert top element next value to array".to_string(),
                            )
                        })?,
                        index: top_leaf.leaf_index as usize,
                    },
                    seq: 1,
                    signature: Signature::from([0; 64]), // Placeholder for synthetic element
                },
            );
        }
    }
    Ok(())
}

/// Persists indexed Merkle tree updates to the database, maintaining the linked structure
/// required for indexed trees where each element points to the next element in sorted order.
///
/// This function implements indexed Merkle tree operations including both new element
/// appends and the corresponding low element updates that maintain tree integrity.
///
/// ## Steps performed:
/// 1. **Tree Processing**: Iterate through each unique tree in the updates
/// 2. **Tree Type Detection**: Determine if tree is V1 (AddressV1/StateV1) or V2 for proper hash computation
/// 3. **Low Element Updates**:
///    - Query existing tree state from database to build local view
///    - For empty trees, initialize with zeroeth and top elements as needed
///    - For each new element being appended:
///      - Find the "low element" (largest existing element smaller than new value)
///      - Update the low element to point to the new element (update its next_index/next_value)
///      - Configure the new element to point to what the low element was pointing to
///      - Recompute hashes for the updated low element
///      - Add low element update to the batch
/// 4. **Initialization Elements**: Ensure required initialization elements exist:
///    - Zeroeth element (leaf_index 0): Points to first real element or top element
///    - Top element (leaf_index 1): Only for V1 trees, represents the maximum value
/// 5. **Database Persistence**:
///    - Batch updates into chunks to avoid SQL parameter limits
///    - Use upsert logic with sequence number checks to handle conflicts
///    - Insert/update records in indexed_trees table
/// 6. **State Tree Integration**: Create corresponding leaf nodes for the Merkle tree structure
pub async fn persist_indexed_tree_updates(
    txn: &DatabaseTransaction,
    mut indexed_leaf_updates: HashMap<(Pubkey, u64), IndexedTreeLeafUpdate>,
    tree_info_cache: &std::collections::HashMap<
        solana_pubkey::Pubkey,
        crate::ingester::parser::tree_info::TreeInfo,
    >,
) -> Result<(), IngesterError> {
    // Step 1: Tree Processing - Collect unique trees and look up their types from cache
    let unique_trees: Vec<Pubkey> = indexed_leaf_updates
        .values()
        .map(|update| update.tree)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let mut trees: HashMap<Pubkey, (TreeType, u32)> = HashMap::new();
    for tree in unique_trees {
        let tree_info = tree_info_cache.get(&tree).ok_or_else(|| {
            IngesterError::ParserError(format!("Tree metadata not found for tree {}", tree))
        })?;
        trees.insert(tree, (tree_info.tree_type, tree_info.height));
    }

    for (tree, (tree_type, _tree_height)) in trees.clone() {
        let sdk_tree = Pubkey::new_from_array(tree.to_bytes());
        // Step 4: Initialization Elements - Ensure required initialization elements exist
        ensure_zeroeth_element_exists(&mut indexed_leaf_updates, sdk_tree, tree, tree_type)?;
        ensure_top_element_exists(&mut indexed_leaf_updates, sdk_tree, tree, tree_type)?;
    }
    // Step 5: Database Persistence - Batch updates and insert/update records
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

        // Step 6: State Tree Integration - Create corresponding leaf nodes for the Merkle tree structure
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

        // Get the tree height for this specific tree (+1 because indexed trees have one extra level)
        let tree_height = trees
            .get(&chunk[0].tree)
            .map(|(_, height)| height + 1)
            .ok_or_else(|| {
                IngesterError::ParserError(format!(
                    "Tree height not found for tree {} during persist",
                    chunk[0].tree
                ))
            })?;
        persist_leaf_nodes(txn, state_tree_leaf_nodes, tree_height).await?;

        let address_tree_history_models = chunk
            .iter()
            .map(
                |x| crate::dao::generated::state_tree_histories::ActiveModel {
                    tree: Set(x.tree.to_bytes().to_vec()),
                    seq: Set(x.seq as i64),
                    leaf_idx: Set(x.leaf.index as i64),
                    transaction_signature: Set(Into::<[u8; 64]>::into(x.signature).to_vec()),
                },
            )
            .collect::<Vec<_>>();

        if !address_tree_history_models.is_empty() {
            let query = crate::dao::generated::state_tree_histories::Entity::insert_many(
                address_tree_history_models,
            )
            .on_conflict(
                OnConflict::columns([
                    crate::dao::generated::state_tree_histories::Column::Tree,
                    crate::dao::generated::state_tree_histories::Column::Seq,
                ])
                .do_nothing()
                .to_owned(),
            )
            .build(txn.get_database_backend());
            txn.execute(query).await?;
        }
    }

    Ok(())
}

pub async fn multi_append(
    txn: &DatabaseTransaction,
    values: Vec<Vec<u8>>,
    tree: Vec<u8>,
    tree_height: u32,
    seq: Option<u32>,
    tree_info_cache: &std::collections::HashMap<solana_pubkey::Pubkey, TreeInfo>,
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
        let tree_pubkey = Pubkey::from(tree.clone().try_into().unwrap_or([0u8; 32]));
        let tree_type = TreeInfo::get_tree_type(txn, &tree_pubkey)
            .await
            .map_err(|e| IngesterError::ParserError(format!("Failed to get tree type: {}", e)))?;

        let models = if matches!(tree_type, TreeType::AddressV1) {
            vec![
                get_zeroeth_exclusion_range_v1(tree.clone()),
                get_top_element(tree.clone()),
            ]
        } else {
            vec![get_zeroeth_exclusion_range(tree.clone())]
        };
        for model in models {
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
            seq: seq.map(|s| s as i64),
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

    let active_elements: Vec<indexed_trees::ActiveModel> = elements_to_update
        .values()
        .map(|x| indexed_trees::ActiveModel {
            tree: Set(tree.clone()),
            leaf_index: Set(x.leaf_index),
            value: Set(x.value.clone()),
            next_index: Set(x.next_index),
            next_value: Set(x.next_value.clone()),
            seq: Set(seq.map(|s| s as i64)),
        })
        .collect();

    let mut query = indexed_trees::Entity::insert_many(active_elements.clone())
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

    let result = txn.execute(query).await;

    if let Err(e) = result {
        log::error!("Failed to insert/update indexed tree elements: {}", e);
        return Err(IngesterError::DatabaseError(format!(
            "Failed to insert/update indexed tree elements: {}",
            e
        )));
    }

    let mut leaf_nodes = Vec::new();
    for x in elements_to_update.values() {
        let hash = compute_hash_with_cache(x, &tree, tree_info_cache)?;
        leaf_nodes.push(LeafNode {
            tree: SerializablePubkey::try_from(x.tree.clone()).map_err(|e| {
                IngesterError::DatabaseError(format!("Failed to serialize pubkey: {}", e))
            })?,
            leaf_index: x.leaf_index as u32,
            hash,
            seq,
        });
    }

    persist_leaf_nodes(txn, leaf_nodes, tree_height).await?;

    Ok(())
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
    use crate::ingester::persist::indexed_merkle_tree::compute_range_node_hash_v2;

    #[test]
    fn test_zeroeth_element_hash_is_not_zero_bytes_0() {
        let dummy_tree_id = vec![1u8; 32];
        let zeroeth_element = get_zeroeth_exclusion_range(dummy_tree_id.clone());
        let zeroeth_element_hash_result = compute_range_node_hash_v2(&zeroeth_element);
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
