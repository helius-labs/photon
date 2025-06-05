use crate::common::typedefs::account::{Account, AccountWithContext};
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::state_trees;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::state_update::LeafNullification;
use crate::ingester::persist::persisted_state_tree::{get_proof_nodes, ZERO_BYTES};
use crate::ingester::persist::{compute_parent_hash, get_node_direct_ancestors};
use crate::migration::OnConflict;
use itertools::Itertools;
use sea_orm::{ConnectionTrait, DatabaseTransaction, EntityTrait, QueryTrait, Set};
use std::cmp::max;
use std::collections::HashMap;

pub const TREE_HEIGHT_V1: u32 = 26;
pub const STATE_TREE_HEIGHT_V2: u32 = 32;

#[derive(Clone, Debug)]
pub struct LeafNode {
    pub tree: SerializablePubkey,
    pub leaf_index: u32,
    pub hash: Hash,
    pub seq: Option<u32>,
}

impl LeafNode {
    pub fn node_index(&self, tree_height: u32) -> i64 {
        leaf_index_to_node_index(self.leaf_index, tree_height)
    }
}

// leaf_index should be u64 / i64 to avoid overflow
pub fn leaf_index_to_node_index(leaf_index: u32, tree_height: u32) -> i64 {
    2_i64.pow(tree_height - 1) + leaf_index as i64
}

impl From<Account> for LeafNode {
    fn from(account: Account) -> Self {
        Self {
            tree: account.tree,
            leaf_index: account.leaf_index.0 as u32,
            hash: account.hash,
            seq: account.seq.map(|x| x.0 as u32),
        }
    }
}

impl From<AccountWithContext> for LeafNode {
    fn from(account: AccountWithContext) -> Self {
        Self {
            tree: account.account.tree,
            leaf_index: account.account.leaf_index.0 as u32,
            hash: account.account.hash,
            seq: account.account.seq.map(|x| x.0 as u32),
        }
    }
}

impl From<LeafNullification> for LeafNode {
    fn from(leaf_nullification: LeafNullification) -> Self {
        Self {
            tree: SerializablePubkey::from(leaf_nullification.tree),
            leaf_index: leaf_nullification.leaf_index as u32,
            hash: Hash::from(ZERO_BYTES[0]),
            seq: Some(leaf_nullification.seq as u32),
        }
    }
}

pub async fn persist_leaf_nodes(
    txn: &DatabaseTransaction,
    mut leaf_nodes: Vec<LeafNode>,
    tree_height: u32,
) -> Result<(), IngesterError> {
    if leaf_nodes.is_empty() {
        return Ok(());
    }

    leaf_nodes.sort_by_key(|node| node.seq);

    let leaf_locations = leaf_nodes
        .iter()
        .map(|node| (node.tree.to_bytes_vec(), node.node_index(tree_height)))
        .collect::<Vec<_>>();

    let node_locations_to_models =
        get_proof_nodes(txn, leaf_locations, true, false, Some(tree_height)).await?;
    let mut node_locations_to_hashes_and_seq = node_locations_to_models
        .iter()
        .map(|(key, value)| (key.clone(), (value.hash.clone(), value.seq)))
        .collect::<HashMap<_, _>>();

    let mut models_to_updates = HashMap::new();

    for leaf_node in leaf_nodes.clone() {
        let node_idx = leaf_node.node_index(tree_height);
        let tree = leaf_node.tree;
        let key = (tree.to_bytes_vec(), node_idx);

        let model = state_trees::ActiveModel {
            tree: Set(tree.to_bytes_vec()),
            level: Set(0),
            node_idx: Set(node_idx),
            hash: Set(leaf_node.hash.to_vec()),
            leaf_idx: Set(Some(leaf_node.leaf_index as i64)),
            seq: Set(leaf_node.seq.map(|x| x as i64)),
        };

        let existing_seq = node_locations_to_hashes_and_seq
            .get(&key)
            .map(|x| x.1)
            .unwrap_or(Some(0));

        if let Some(existing_seq) = existing_seq {
            if let Some(leaf_node_seq) = leaf_node.seq {
                if leaf_node_seq >= existing_seq as u32 {
                    models_to_updates.insert(key.clone(), model);
                    node_locations_to_hashes_and_seq
                        .insert(key, (leaf_node.hash.to_vec(), Some(leaf_node_seq as i64)));
                }
            }
        }
    }

    let all_ancestors = leaf_nodes
        .iter()
        .flat_map(|leaf_node| {
            get_node_direct_ancestors(leaf_node.node_index(tree_height))
                .iter()
                .enumerate()
                .map(move |(i, &idx)| (leaf_node.tree.to_bytes_vec(), idx, i))
                .collect::<Vec<(Vec<u8>, i64, usize)>>()
        })
        .sorted_by(|a, b| {
            // Need to sort elements before dedup
            a.0.cmp(&b.0) // Sort by tree
                .then_with(|| a.1.cmp(&b.1)) // Then by node index
        }) // Need to sort elements before dedup
        .dedup()
        .collect::<Vec<(Vec<u8>, i64, usize)>>();

    for (tree, node_index, child_level) in all_ancestors.into_iter().rev() {
        let (left_child_hash, left_child_seq) = node_locations_to_hashes_and_seq
            .get(&(tree.clone(), node_index * 2))
            .cloned()
            .unwrap_or((ZERO_BYTES[child_level].to_vec(), Some(0)));

        let (right_child_hash, right_child_seq) = node_locations_to_hashes_and_seq
            .get(&(tree.clone(), node_index * 2 + 1))
            .cloned()
            .unwrap_or((ZERO_BYTES[child_level].to_vec(), Some(0)));

        let level = child_level + 1;

        let hash = compute_parent_hash(left_child_hash.clone(), right_child_hash.clone())?;

        let seq = max(left_child_seq, right_child_seq);
        let model = state_trees::ActiveModel {
            tree: Set(tree.clone()),
            level: Set(level as i64),
            node_idx: Set(node_index),
            hash: Set(hash.clone()),
            leaf_idx: Set(None),
            seq: Set(seq),
        };

        let key = (tree.clone(), node_index);
        models_to_updates.insert(key.clone(), model);
        node_locations_to_hashes_and_seq.insert(key, (hash, seq));
    }

    // We first build the query and then execute it because SeaORM has a bug where it always throws
    // an error if we do not insert a record in an insert statement. However, in this case, it's
    // expected not to insert anything if the key already exists.
    let mut query = state_trees::Entity::insert_many(models_to_updates.into_values())
        .on_conflict(
            OnConflict::columns([state_trees::Column::Tree, state_trees::Column::NodeIdx])
                .update_columns([state_trees::Column::Hash, state_trees::Column::Seq])
                .to_owned(),
        )
        .build(txn.get_database_backend());
    query.sql = format!("{} WHERE excluded.seq >= state_trees.seq", query.sql);
    txn.execute(query).await.map_err(|e| {
        IngesterError::DatabaseError(format!("Failed to persist path nodes: {}", e))
    })?;
    Ok(())
}
