// This test demonstrates that re-indexing fixes the wrong zeroeth element in v1 trees.
//
// The bug: v1 trees were initialized with wrong zeroeth element (next_index = 0 instead of 1)
// The fix: Re-indexing now correctly sets next_index = 1 for v1 trees

use crate::utils::*;
use function_name::named;
use light_compressed_account::TreeType;
use photon_indexer::dao::generated::indexed_trees;
use photon_indexer::ingester::parser::indexer_events::RawIndexedElement;
use photon_indexer::ingester::parser::state_update::IndexedTreeLeafUpdate;
use photon_indexer::ingester::persist::indexed_merkle_tree::{
    compute_range_node_hash_v1, get_zeroeth_exclusion_range_v1,
};
use photon_indexer::ingester::persist::persisted_indexed_merkle_tree::persist_indexed_tree_updates;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set, TransactionTrait};
use serial_test::serial;
use solana_pubkey::Pubkey;
use std::collections::HashMap;

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_reindex_fixes_wrong_zeroeth_element(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    // Setup test database
    let name = trim_test_name(function_name!());
    let setup = setup(name.clone(), db_backend).await;
    let conn = setup.db_conn;

    // Create a v1 address tree
    let tree_pubkey = Pubkey::new_unique();
    let tree_bytes = tree_pubkey.to_bytes().to_vec();

    // Step 1: Insert tree with wrong zeroeth element (simulating the bug)
    let txn = conn.begin().await.unwrap();

    // Get the correct v1 zeroeth element to use its values
    let correct_v1_zeroeth = get_zeroeth_exclusion_range_v1(tree_bytes.clone());

    // Insert wrong zeroeth element (using v2 style with next_index = 0 instead of 1)
    let wrong_zeroeth = indexed_trees::ActiveModel {
        tree: Set(tree_bytes.clone()),
        leaf_index: Set(0),
        value: Set(correct_v1_zeroeth.value.clone()),
        next_index: Set(0), // Wrong! Should be 1 for v1 trees
        next_value: Set(correct_v1_zeroeth.next_value.clone()),
        seq: Set(Some(0)),
    };
    indexed_trees::Entity::insert(wrong_zeroeth)
        .exec(&txn)
        .await
        .unwrap();

    // Get top element for v1 trees
    let top_element_model =
        photon_indexer::ingester::persist::indexed_merkle_tree::get_top_element(tree_bytes.clone());

    // Insert top element at index 1 (required for v1 trees)
    let top_element = indexed_trees::ActiveModel {
        tree: Set(tree_bytes.clone()),
        leaf_index: Set(1),
        value: Set(top_element_model.value.clone()),
        next_index: Set(top_element_model.next_index),
        next_value: Set(top_element_model.next_value.clone()),
        seq: Set(Some(1)),
    };
    indexed_trees::Entity::insert(top_element)
        .exec(&txn)
        .await
        .unwrap();

    txn.commit().await.unwrap();

    // Verify wrong data exists
    let wrong_zeroeth_from_db = indexed_trees::Entity::find()
        .filter(indexed_trees::Column::Tree.eq(tree_bytes.clone()))
        .filter(indexed_trees::Column::LeafIndex.eq(0))
        .one(&*conn)
        .await
        .unwrap()
        .expect("Should find zeroeth element");

    assert_eq!(
        wrong_zeroeth_from_db.next_index, 0,
        "Zeroeth element should have wrong next_index"
    );

    // Step 2: Simulate re-indexing that will fix the zeroeth element
    let txn = conn.begin().await.unwrap();

    // Create the correct zeroeth element update
    let correct_zeroeth_leaf = RawIndexedElement {
        value: [0u8; 32],
        next_index: 1, // Correct value for v1
        next_value: wrong_zeroeth_from_db.next_value.clone().try_into().unwrap(),
        index: 0,
    };

    // Compute the correct hash for v1 tree
    let correct_zeroeth_model = indexed_trees::Model {
        tree: tree_bytes.clone(),
        leaf_index: 0,
        value: vec![0u8; 32],
        next_index: 1, // Correct
        next_value: wrong_zeroeth_from_db.next_value.clone(),
        seq: Some(2),
    };
    let correct_hash = compute_range_node_hash_v1(&correct_zeroeth_model).unwrap();

    let update = IndexedTreeLeafUpdate {
        tree: tree_pubkey,
        tree_type: TreeType::AddressV1,
        leaf: correct_zeroeth_leaf,
        hash: correct_hash.0,
        seq: 2, // Higher seq number to ensure update
        signature: Default::default(),
    };

    // Create HashMap with the update
    let mut updates = HashMap::new();
    updates.insert((tree_pubkey, 0u64), update);

    // Persist the update which should fix the zeroeth element
    persist_indexed_tree_updates(&txn, updates).await.unwrap();
    txn.commit().await.unwrap();

    // Step 3: Verify the zeroeth element is now correct
    let fixed_zeroeth = indexed_trees::Entity::find()
        .filter(indexed_trees::Column::Tree.eq(tree_bytes.clone()))
        .filter(indexed_trees::Column::LeafIndex.eq(0))
        .one(&*conn)
        .await
        .unwrap()
        .expect("Should find zeroeth element");

    assert_eq!(
        fixed_zeroeth.next_index, 1,
        "Zeroeth element should now have correct next_index"
    );
    assert_eq!(fixed_zeroeth.seq, Some(2), "Seq should be updated");

    // Verify it matches what get_zeroeth_exclusion_range_v1 would create
    let expected_zeroeth = get_zeroeth_exclusion_range_v1(tree_bytes.clone());
    assert_eq!(fixed_zeroeth.next_index, expected_zeroeth.next_index);
    assert_eq!(fixed_zeroeth.value, expected_zeroeth.value);

    println!("✅ Re-indexing successfully fixed zeroeth element from wrong v2-style (next_index=0) to correct v1-style (next_index=1)");
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_reindex_preserves_correct_zeroeth_element(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    // This test verifies that re-indexing doesn't break already correct trees
    let name = trim_test_name(function_name!());
    let setup = setup(name.clone(), db_backend).await;
    let conn = setup.db_conn;

    // Create a v1 address tree
    let tree_pubkey = Pubkey::new_unique();
    let tree_bytes = tree_pubkey.to_bytes().to_vec();

    // Insert correct zeroeth element
    let txn = conn.begin().await.unwrap();

    let correct_v1_zeroeth = get_zeroeth_exclusion_range_v1(tree_bytes.clone());

    let correct_zeroeth = indexed_trees::ActiveModel {
        tree: Set(tree_bytes.clone()),
        leaf_index: Set(0),
        value: Set(correct_v1_zeroeth.value.clone()),
        next_index: Set(1), // Correct for v1
        next_value: Set(correct_v1_zeroeth.next_value.clone()),
        seq: Set(Some(0)),
    };
    indexed_trees::Entity::insert(correct_zeroeth)
        .exec(&txn)
        .await
        .unwrap();

    txn.commit().await.unwrap();

    // Simulate re-indexing with the same correct data
    let txn = conn.begin().await.unwrap();

    let leaf = RawIndexedElement {
        value: correct_v1_zeroeth.value.clone().try_into().unwrap(),
        next_index: 1,
        next_value: correct_v1_zeroeth.next_value.clone().try_into().unwrap(),
        index: 0,
    };

    let model = indexed_trees::Model {
        tree: tree_bytes.clone(),
        leaf_index: 0,
        value: correct_v1_zeroeth.value.clone(),
        next_index: 1,
        next_value: correct_v1_zeroeth.next_value.clone(),
        seq: Some(1),
    };
    let hash = compute_range_node_hash_v1(&model).unwrap();

    let update = IndexedTreeLeafUpdate {
        tree: tree_pubkey,
        tree_type: TreeType::AddressV1,
        leaf,
        hash: hash.0,
        seq: 1,
        signature: Default::default(),
    };

    let mut updates = HashMap::new();
    updates.insert((tree_pubkey, 0u64), update);

    persist_indexed_tree_updates(&txn, updates).await.unwrap();
    txn.commit().await.unwrap();

    // Verify it's still correct
    let zeroeth = indexed_trees::Entity::find()
        .filter(indexed_trees::Column::Tree.eq(tree_bytes.clone()))
        .filter(indexed_trees::Column::LeafIndex.eq(0))
        .one(&*conn)
        .await
        .unwrap()
        .expect("Should find zeroeth element");

    assert_eq!(
        zeroeth.next_index, 1,
        "Zeroeth element should still be correct"
    );
    assert_eq!(zeroeth.seq, Some(1), "Seq should be updated");

    println!("✅ Re-indexing preserves already correct zeroeth elements");
}
