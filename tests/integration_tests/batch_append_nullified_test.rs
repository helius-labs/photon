use crate::utils::*;
use photon_indexer::dao::generated::state_trees;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use serial_test::serial;

/// Test batch nullify events followed by batch append
/// This test reproduces a scenario where:
/// 1. BatchAppend seq 1 - adds accounts 0-10
/// 2. BatchAppend seq 2 - adds accounts 10-20
/// 3. BatchNullify seq 3 - nullifies accounts with nullifier queue indices 0-10
/// 4. BatchAppend seq 4 - adds accounts 20-30 (root mismatch occurs here)
///
/// The bug manifests at sequence 4 where the indexer produces an incorrect root
#[rstest]
#[tokio::test]
#[serial]
async fn test_batch_append_followed_by_nullify_in_queue(
    #[values(DatabaseBackend::Sqlite)] db_backend: DatabaseBackend,
) {
    let name = "batch_append_nullify";
    let setup = setup_with_options(
        name.to_string(),
        TestSetupOptions {
            network: Network::Localnet,
            db_backend,
        },
    )
    .await;

    reset_tables(setup.db_conn.as_ref()).await.unwrap();

    let sort_by_slot = true;
    let signatures = read_file_names(&name, sort_by_slot);
    let index_individually = true;

    println!("Total transactions to index: {}", signatures.len());

    let mut batch_events_seen = Vec::new();

    let batch_append_1 =
        "4BWyAeXyMaobHcrX6UrUn5qwSv9forkNPtdUbKcMcYjmbVK4fYMEkFokbGbLKCSsmB6WTFy1jpCAnPEKjRh1Z5R9";
    let batch_append_2 =
        "3UdMPT7UDWdFh32G3JUNgvL1Jwmb546jhsW5PGV63G1m4dy5ypc6q9JqMamQ59pLRbBRtq8nQKw3s2BZrucviQHG";
    let batch_nullify_3 =
        "2u8yxJ95DYaHJTYB6D3cF6KCfkhvzFkGraYdri3fnK6xKPeyw8zLpJqstEYdQcu1vZZsPXDMSUidBajgrbpAJRPM";
    let batch_append_4 =
        "2h1vPiv1WvkjfujPyS5vPGEMYMp5DEfBxdFPGo9kStp1neXVPXAarWfKM6nnhcs8c59LKqpGo9aBtkrDUF91nDnP";

    // Process all transactions in order
    for (_, signature) in signatures.iter().enumerate() {
        index(
            &name,
            setup.db_conn.clone(),
            setup.client.clone(),
            &[signature.to_string()],
            index_individually,
        )
        .await;

        // Track batch events
        if signature == batch_append_1 {
            batch_events_seen.push(format!("BatchAppend seq 1 (indices 0-10): {}", signature));
            println!("  ✓ BatchAppend seq 1 processed");
        } else if signature == batch_append_2 {
            batch_events_seen.push(format!("BatchAppend seq 2 (indices 10-20): {}", signature));
            println!("  ✓ BatchAppend seq 2 processed");
        } else if signature == batch_nullify_3 {
            batch_events_seen.push(format!(
                "BatchNullify seq 3 (nullifier queue 0-10): {}",
                signature
            ));
            println!("  ✓ BatchNullify seq 3 processed");
        } else if signature == batch_append_4 {
            batch_events_seen.push(format!("BatchAppend seq 4 (indices 20-30): {}", signature));
            println!("  ✓ BatchAppend seq 4 processed");
        }
    }

    println!("\nAll transactions indexed successfully");

    if !batch_events_seen.is_empty() {
        println!("\nBatch events processed:");
        for event in &batch_events_seen {
            println!("  - {}", event);
        }
    }

    let tree_bytes = bs58::decode("HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu")
        .into_vec()
        .unwrap();

    let root_node = state_trees::Entity::find()
        .filter(state_trees::Column::Tree.eq(tree_bytes.clone()))
        .filter(state_trees::Column::NodeIdx.eq(1i64))
        .one(setup.db_conn.as_ref())
        .await
        .unwrap();

    if let Some(root) = root_node {
        let root_hash = bs58::encode(&root.hash).into_string();
        let seq = root.seq.unwrap_or(0);
        println!("State:");
        println!("  Tree root hash: {}", root_hash);
        println!("  Sequence: {}", seq);

        // On-chain root history from photon.log error message
        // These are the actual roots from the on-chain account
        const ON_CHAIN_ROOT_HISTORY: &[&str] = &[
            "4C4hXkTeb81GhLj8GBTwDxRxvBy2xZxVFh4YfthKA6XJ", // [0] - initial root
            "433kYLWvdMNY8SVGamZakDQFNZRu569gR37RxrM4Q4Vk", // [1] - after BatchAppend seq 1
            "3SgZfe7CoZEfYP1YbEyqKpK3qkoTypobm2oXsqsNLQKL", // [2] - after BatchAppend seq 2
            "4D1x7SoCWSu6KTTDwyKsphwNCb4er7wr5Rw3qAv2o1wg", // [3] - after BatchNullify seq 3
            "et9ihtqjKu8uszGmNDQA9ErNzT8tbyVzmN1833fGrUF", // [4] - after BatchAppend seq 4 (should be this)
        ];

        println!("\nExpected root progression:");
        for (i, root) in ON_CHAIN_ROOT_HISTORY.iter().enumerate() {
            let marker = if i as i64 == seq { " <-- Current" } else { "" };
            println!("  Seq {}: {}{}", i, root, marker);
        }

        // Get the expected root for the current sequence
        if (seq as usize) < ON_CHAIN_ROOT_HISTORY.len() {
            let expected_on_chain_root = ON_CHAIN_ROOT_HISTORY[seq as usize];

            println!("\nRoot comparison at sequence {}:", seq);
            println!("  Expected on-chain root: {}", expected_on_chain_root);
            println!("  Actual indexer root:    {}", root_hash);

            // Always assert that roots should match
            assert_eq!(
                root_hash, expected_on_chain_root,
                "\nROOT MISMATCH at sequence {}!\n\
                Expected on-chain root: {}\n\
                Got indexer root:       {}\n\n\
                This occurs after BatchNullify (seq 3) followed by BatchAppend (seq 4).\n\
                The indexer incorrectly processes nullified accounts during batch operations.",
                seq, expected_on_chain_root, root_hash
            );
        } else {
            println!(
                "  Warning: Sequence {} is beyond known root history (max: {})",
                seq,
                ON_CHAIN_ROOT_HISTORY.len() - 1
            );
        }

        println!("\nTransaction pattern summary:");
        println!("  1. Compress transactions creating accounts");
        println!("  2. BatchAppend (seq 1) - processes accounts 0-10");
        println!("  3. More compress transactions");
        println!("  4. BatchAppend (seq 2) - processes accounts 10-20");
        println!("  5. Nullification transactions");
        println!("  6. BatchNullify (seq 3) - nullifies accounts in queue");
        println!("  7. More compress transactions");
        println!("  8. BatchAppend (seq 4) - processes accounts 20-30 (BUG MANIFESTS HERE)");
    } else {
        panic!("No root node found in state_trees for tree HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu - transactions were not indexed properly");
    }
}
