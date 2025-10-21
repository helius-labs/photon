use function_name::named;
use photon_indexer::dao::generated::{prelude::*, tree_metadata};
use photon_indexer::ingester::parser::tree_info::TreeInfo;
use sea_orm::{ColumnTrait, DatabaseBackend, EntityTrait, QueryFilter};
use solana_pubkey::Pubkey;
use solana_sdk::pubkey::Pubkey as SdkPubkey;

use light_compressed_account::TreeType;
use serial_test::serial;
use crate::utils::*;

// Helper function to convert solana_pubkey::Pubkey to solana_sdk::pubkey::Pubkey
fn to_sdk_pubkey(pubkey: &Pubkey) -> SdkPubkey {
    SdkPubkey::from(pubkey.to_bytes())
}

/// Test that verifies tree metadata is correctly persisted and can be retrieved
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_tree_metadata_upsert_and_retrieval(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Devnet,
            db_backend,
        },
    )
    .await;

    let tree_pubkey = "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT"
        .parse::<Pubkey>()
        .unwrap();
    let queue_pubkey = "nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148"
        .parse::<Pubkey>()
        .unwrap();

    // Test data - tree is pre-populated by populate_test_tree_metadata
    // Update it with new values to test upsert behavior
    let tree_type = TreeType::StateV1;
    let height = 26;
    let root_history_capacity = 2400;
    let sequence_number = 42;
    let next_index = 100;

    // Update tree metadata with new sequence and index
    upsert_tree_metadata_for_test(
        setup.db_conn.as_ref(),
        to_sdk_pubkey(&tree_pubkey),
        root_history_capacity,
        height,
        tree_type as i32,
        sequence_number,
        next_index,
        to_sdk_pubkey(&queue_pubkey),
    )
    .await
    .unwrap();

    // Verify using TreeMetadata entity
    let metadata = TreeMetadata::find()
        .filter(tree_metadata::Column::TreePubkey.eq(tree_pubkey.to_bytes().to_vec()))
        .one(setup.db_conn.as_ref())
        .await
        .unwrap()
        .expect("Tree metadata should exist");

    assert_eq!(metadata.tree_type, tree_type as i32);
    assert_eq!(metadata.height, height);
    assert_eq!(metadata.root_history_capacity, root_history_capacity);
    assert_eq!(metadata.sequence_number, sequence_number as i64);
    assert_eq!(metadata.next_index, next_index as i64);
    assert_eq!(metadata.queue_pubkey, queue_pubkey.to_bytes().to_vec());

    // Verify using TreeInfo::get_by_pubkey
    let tree_info = TreeInfo::get_by_pubkey(setup.db_conn.as_ref(), &tree_pubkey)
        .await
        .unwrap()
        .expect("TreeInfo should be retrievable");

    assert_eq!(tree_info.tree_type, tree_type);
    assert_eq!(tree_info.height, height as u32);
    assert_eq!(tree_info.queue, queue_pubkey);
}

/// Test that verifies batch retrieval of tree metadata works correctly
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_tree_info_batch_retrieval(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Devnet,
            db_backend,
        },
    )
    .await;

    // Test trees are pre-populated by populate_test_tree_metadata in setup
    let test_trees = vec![
        (
            "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT",
            "nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148",
            TreeType::StateV1,
            26,
        ),
        (
            "amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2",
            "aq1S9z4reTSQAdgWHGD2zDaS39sjGrAxbR31vxJ2F4F",
            TreeType::AddressV1,
            26,
        ),
        (
            "HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu",
            "6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU",
            TreeType::StateV2,
            32,
        ),
        (
            "EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK",
            "EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK",
            TreeType::AddressV2,
            40,
        ),
    ];

    let tree_pubkeys: Vec<Pubkey> = test_trees
        .iter()
        .map(|(tree_str, _, _, _)| tree_str.parse::<Pubkey>().unwrap())
        .collect();

    // Test batch retrieval
    let tree_info_cache = TreeInfo::get_tree_info_batch(setup.db_conn.as_ref(), &tree_pubkeys)
        .await
        .unwrap();

    // Verify all trees are in the cache
    assert_eq!(tree_info_cache.len(), test_trees.len());

    // Verify each tree has correct data
    for (i, (tree_str, queue_str, expected_type, expected_height)) in test_trees.iter().enumerate()
    {
        let tree_pubkey = tree_str.parse::<Pubkey>().unwrap();
        let queue_pubkey = queue_str.parse::<Pubkey>().unwrap();

        let tree_info = tree_info_cache
            .get(&tree_pubkey)
            .expect(&format!("Tree {} should be in cache", i));

        assert_eq!(tree_info.tree_type, *expected_type);
        assert_eq!(tree_info.height, *expected_height as u32);
        assert_eq!(tree_info.queue, queue_pubkey);
    }
}

/// Test that verifies tree metadata updates work correctly (upsert behavior)
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_tree_metadata_update(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Devnet,
            db_backend,
        },
    )
    .await;

    let tree_pubkey = "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT"
        .parse::<Pubkey>()
        .unwrap();
    let queue_pubkey = "nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148"
        .parse::<Pubkey>()
        .unwrap();

    // Tree is pre-populated by populate_test_tree_metadata with sequence=0, next_index=0
    // Update with new sequence number and next index to test upsert behavior
    let new_sequence = 100;
    let new_next_index = 500;

    upsert_tree_metadata_for_test(
        setup.db_conn.as_ref(),
        to_sdk_pubkey(&tree_pubkey),
        2400,
        26,
        TreeType::StateV1 as i32,
        new_sequence,
        new_next_index,
        to_sdk_pubkey(&queue_pubkey),
    )
    .await
    .unwrap();

    // Verify the update
    let metadata = TreeMetadata::find()
        .filter(tree_metadata::Column::TreePubkey.eq(tree_pubkey.to_bytes().to_vec()))
        .one(setup.db_conn.as_ref())
        .await
        .unwrap()
        .expect("Tree metadata should exist");

    assert_eq!(metadata.sequence_number, new_sequence as i64);
    assert_eq!(metadata.next_index, new_next_index as i64);
    assert_eq!(metadata.height, 26); // Height should remain unchanged
}

/// Test that verifies TreeInfo handles missing trees correctly
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_tree_info_missing_tree(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Devnet,
            db_backend,
        },
    )
    .await;

    // Try to get a tree that doesn't exist
    let nonexistent_tree = "11111111111111111111111111111111"
        .parse::<Pubkey>()
        .unwrap();

    let result = TreeInfo::get_by_pubkey(setup.db_conn.as_ref(), &nonexistent_tree).await;

    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

/// Test that verifies batch retrieval handles empty input correctly
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_tree_info_batch_empty(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Devnet,
            db_backend,
        },
    )
    .await;

    let empty_pubkeys: Vec<Pubkey> = vec![];
    let tree_info_cache = TreeInfo::get_tree_info_batch(setup.db_conn.as_ref(), &empty_pubkeys)
        .await
        .unwrap();

    assert!(tree_info_cache.is_empty());
}

/// Test that verifies all tree types are handled correctly
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_all_tree_types(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Devnet,
            db_backend,
        },
    )
    .await;

    let all_tree_types = vec![
        (TreeType::StateV1, "State V1 tree"),
        (TreeType::AddressV1, "Address V1 tree"),
        (TreeType::StateV2, "State V2 tree"),
        (TreeType::AddressV2, "Address V2 tree"),
    ];

    for (i, (tree_type, description)) in all_tree_types.iter().enumerate() {
        // Generate unique pubkeys for each test
        let tree_bytes = [(i + 1) as u8; 32];
        let queue_bytes = [(i + 100) as u8; 32];
        let tree_pubkey = Pubkey::from(tree_bytes);
        let queue_pubkey = Pubkey::from(queue_bytes);

        upsert_tree_metadata_for_test(
            setup.db_conn.as_ref(),
            to_sdk_pubkey(&tree_pubkey),
            2400,
            26,
            *tree_type as i32,
            0,
            0,
            to_sdk_pubkey(&queue_pubkey),
        )
        .await
        .unwrap();

        // Verify retrieval
        let tree_info = TreeInfo::get_by_pubkey(setup.db_conn.as_ref(), &tree_pubkey)
            .await
            .unwrap()
            .expect(&format!("{} should be retrievable", description));

        assert_eq!(
            tree_info.tree_type, *tree_type,
            "{} type mismatch",
            description
        );
    }
}
