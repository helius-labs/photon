use crate::utils::*;
use photon_indexer::dao::generated::{accounts, state_trees};
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
        // Check state right before BatchAppend seq 4
        if signature == batch_append_4 {
            println!("\n  Checking state BEFORE BatchAppend seq 4:");

            let tree_bytes = bs58::decode("HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu")
                .into_vec()
                .unwrap();

            // Check for accounts that are spent and will be processed in this batch
            let spent_accounts = accounts::Entity::find()
                .filter(accounts::Column::Tree.eq(tree_bytes.clone()))
                .filter(accounts::Column::Spent.eq(true))
                .all(setup.db_conn.as_ref())
                .await
                .unwrap();

            let accounts_in_queue = accounts::Entity::find()
                .filter(accounts::Column::Tree.eq(tree_bytes.clone()))
                .filter(accounts::Column::InOutputQueue.eq(true))
                .all(setup.db_conn.as_ref())
                .await
                .unwrap();

            // Check for accounts that are spent AND in the output queue
            let spent_in_queue = accounts::Entity::find()
                .filter(accounts::Column::Tree.eq(tree_bytes.clone()))
                .filter(accounts::Column::Spent.eq(true))
                .filter(accounts::Column::InOutputQueue.eq(true))
                .all(setup.db_conn.as_ref())
                .await
                .unwrap();

            println!("    - Total spent accounts: {}", spent_accounts.len());
            println!(
                "    - Accounts in output queue: {}",
                accounts_in_queue.len()
            );
            println!(
                "    - Spent accounts in output queue: {}",
                spent_in_queue.len()
            );

            // Show details of accounts in the output queue
            for (i, acc) in accounts_in_queue.iter().take(5).enumerate() {
                println!(
                    "      Queue account {}: spent={}, leaf_index={}, nullifier_queue_index={:?}",
                    i + 1,
                    acc.spent,
                    acc.leaf_index,
                    acc.nullifier_queue_index
                );
            }

            // Critical assertion: There should be accounts in the queue that will be processed
            // Some of these may have been nullified (spent=true) before being appended to the tree
            assert!(
                !accounts_in_queue.is_empty(),
                "There should be accounts in the output queue for BatchAppend seq 4 to process"
            );

            // This is the key check - some of the accounts being appended were already nullified
            if !spent_in_queue.is_empty() {
                println!(
                    "    ✓ Found {} spent accounts that will be appended",
                    spent_in_queue.len()
                );
            }
        }

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

        if (seq as usize) < ON_CHAIN_ROOT_HISTORY.len() {
            let expected_on_chain_root = ON_CHAIN_ROOT_HISTORY[seq as usize];

            println!("\nRoot comparison at sequence {}:", seq);
            println!("  Expected on-chain root: {}", expected_on_chain_root);
            println!("  Actual indexer root:    {}", root_hash);

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
        println!("  8. BatchAppend (seq 4) - processes accounts 20-30");
    } else {
        panic!("No root node found in state_trees for tree HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu - transactions were not indexed properly");
    }
}

/// Test batch append of spent accounts
/// This test reproduces a scenario where:
/// 1. BatchNullify seq 1 - nullifies accounts with nullifier queue indices 0-10
/// 2. BatchNullify seq 2 - nullifies accounts with nullifier queue indices 10-20
/// 3. BatchAppend seq 3 - adds accounts 0-10 (spent accounts)
/// 4. BatchAppend seq 4 - adds accounts 10-20 (spent accounts)
/// 5. BatchAppend seq 5 - adds accounts 20-30 (spent accounts)
///
/// This tests that the indexer correctly handles appending accounts that were already nullified/spent
#[rstest]
#[tokio::test]
#[serial]
async fn test_batch_append_spent(#[values(DatabaseBackend::Sqlite)] db_backend: DatabaseBackend) {
    let name = "batch_append_spent";
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

    let batch_nullify_1 =
        "5uc2Zz7xkiwLbk6d5bKkoBTAxCwALsFio8q8jmfR3JYJKywCA5vov9NYXerCjaYqY4ha7dN4ikWoyHHFv3H87d7w";
    let batch_nullify_2 =
        "4wmWrV2Rz63AdMzUZmfocFGikXk2NYU8ruVMX1Y7fq7Z6zX81RYjferV7yu9ksoaa9wHrk9nAiQbVJZnTcFg7hGX";
    let batch_append_3 =
        "48ZQPG9tpJmNahSC8wP3oriscphxrid3B3c1D61pxPzkVmjpoNSE4dNgrFeakZabHVzzMWtMc23EZdwJCcTgzxG8";
    let batch_append_4 =
        "2x5R3u75m9gSRrYRKTJbtbKRVBATUcCNLqX4XYQDkqoTxes7sHkmaTci7iYtBPLyWFNM7Y72XSjsq2XKt5zTjYBR";
    let batch_append_5 =
        "4JRPve1xBXXPHkscVBafAizEgWY8pgxFYCcqn9LYyQT9SwZHiktw4xvED8ssxXr6PeLxaKMsVxs2E9gn8ccZBDMY";

    for (_, signature) in signatures.iter().enumerate() {
        if signature == batch_append_5 {
            println!("\n  Checking state BEFORE BatchAppend seq 5:");

            let tree_bytes = bs58::decode("HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu")
                .into_vec()
                .unwrap();

            let spent_accounts = accounts::Entity::find()
                .filter(accounts::Column::Tree.eq(tree_bytes.clone()))
                .filter(accounts::Column::Spent.eq(true))
                .all(setup.db_conn.as_ref())
                .await
                .unwrap();

            let accounts_in_queue = accounts::Entity::find()
                .filter(accounts::Column::Tree.eq(tree_bytes.clone()))
                .filter(accounts::Column::InOutputQueue.eq(true))
                .all(setup.db_conn.as_ref())
                .await
                .unwrap();

            let spent_in_queue = accounts::Entity::find()
                .filter(accounts::Column::Tree.eq(tree_bytes.clone()))
                .filter(accounts::Column::Spent.eq(true))
                .filter(accounts::Column::InOutputQueue.eq(true))
                .all(setup.db_conn.as_ref())
                .await
                .unwrap();

            println!("    - Total spent accounts: {}", spent_accounts.len());
            println!(
                "    - Accounts in output queue: {}",
                accounts_in_queue.len()
            );
            println!(
                "    - Spent accounts in output queue: {}",
                spent_in_queue.len()
            );

            for (i, acc) in accounts_in_queue.iter().take(5).enumerate() {
                println!(
                    "      Queue account {}: spent={}, leaf_index={}, nullifier_queue_index={:?}",
                    i + 1,
                    acc.spent,
                    acc.leaf_index,
                    acc.nullifier_queue_index
                );
            }

            assert!(
                !accounts_in_queue.is_empty(),
                "There should be accounts in the output queue for BatchAppend seq 5 to process"
            );

            assert!(
                !spent_in_queue.is_empty(),
                "There should be spent accounts in the output queue being appended"
            );

            println!(
                "    ✓ Found {} spent accounts that will be appended",
                spent_in_queue.len()
            );
        }

        index(
            &name,
            setup.db_conn.clone(),
            setup.client.clone(),
            &[signature.to_string()],
            index_individually,
        )
        .await;

        let mut should_check_root = false;
        let mut expected_seq = 0u64;

        if signature == batch_nullify_1 {
            batch_events_seen.push(format!(
                "BatchNullify seq 1 (nullifier queue 0-10): {}",
                signature
            ));
            println!("  ✓ BatchNullify seq 1 processed");
            should_check_root = true;
            expected_seq = 1;
        } else if signature == batch_nullify_2 {
            batch_events_seen.push(format!(
                "BatchNullify seq 2 (nullifier queue 10-20): {}",
                signature
            ));
            println!("  ✓ BatchNullify seq 2 processed");
            should_check_root = true;
            expected_seq = 2;
        } else if signature == batch_append_3 {
            batch_events_seen.push(format!(
                "BatchAppend seq 3 (indices 0-10, spent): {}",
                signature
            ));
            println!("  ✓ BatchAppend seq 3 processed (spent accounts)");
            should_check_root = true;
            expected_seq = 3;
        } else if signature == batch_append_4 {
            batch_events_seen.push(format!(
                "BatchAppend seq 4 (indices 10-20, spent): {}",
                signature
            ));
            println!("  ✓ BatchAppend seq 4 processed (spent accounts)");
            should_check_root = true;
            expected_seq = 4;
        } else if signature == batch_append_5 {
            batch_events_seen.push(format!(
                "BatchAppend seq 5 (indices 20-30, spent): {}",
                signature
            ));
            println!("  ✓ BatchAppend seq 5 processed (spent accounts)");
            should_check_root = true;
            expected_seq = 5;
        }

        if should_check_root {
            let tree_bytes = bs58::decode("HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu")
                .into_vec()
                .unwrap();

            let root_node = state_trees::Entity::find()
                .filter(state_trees::Column::Tree.eq(tree_bytes.clone()))
                .filter(state_trees::Column::NodeIdx.eq(1i64))
                .one(setup.db_conn.as_ref())
                .await
                .unwrap();

            const ON_CHAIN_ROOT_HISTORY: &[&str] = &[
                "4C4hXkTeb81GhLj8GBTwDxRxvBy2xZxVFh4YfthKA6XJ", // [0] - initial root
                "3URxLevuDY3as6511nB9NjjK97AMbuW4CqHVskpUzL5z", // [1] - after BatchNullify seq 1
                "2MCdDFbK4BB9vKEaU5pU4gTt73Mpr8Dj3UARcMGJFrxY", // [2] - after BatchNullify seq 2
                "2MCdDFbK4BB9vKEaU5pU4gTt73Mpr8Dj3UARcMGJFrxY", // [3] - after BatchAppend seq 3 (same root - spent accounts)
                "2g28e2qGhzE3gtiGdHLmk62RRKMzfJE2r9sc6Ftqkiq8", // [4] - after BatchAppend seq 4
                "2qjXpKKXF3bsGduLknigLb8oVrZMP3tAnAAKbLVbx3PQ", // [5] - after BatchAppend seq 5 (should be this)
            ];

            if let Some(root) = root_node {
                let root_hash = bs58::encode(&root.hash).into_string();
                let actual_seq = root.seq.unwrap_or(0) as u64;
                let expected_root = ON_CHAIN_ROOT_HISTORY[expected_seq as usize];

                println!(
                    "    Root check at expected seq {}: indexer_seq={}, indexer_root={}, expected_root={}, root_match={}, seq_match={}",
                    expected_seq,
                    actual_seq,
                    root_hash,
                    expected_root,
                    root_hash == expected_root,
                    actual_seq == expected_seq
                );

                if root_hash != expected_root {
                    println!(
                        "ROOT MISMATCH DETECTED at sequence {}!",
                        expected_seq
                    );
                    println!("       Expected: {}", expected_root);
                    println!("       Got:      {}", root_hash);
                }
                
                if actual_seq != expected_seq {
                    println!(
                        "SEQUENCE MISMATCH DETECTED!",
                    );
                    println!("       Expected seq: {}", expected_seq);
                    println!("       Got seq:      {}", actual_seq);
                    panic!("Sequence number mismatch: expected {}, got {}", expected_seq, actual_seq);
                }
            } else {
                println!("No root found for sequence {}", expected_seq);
            }
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

        const ON_CHAIN_ROOT_HISTORY: &[&str] = &[
            "4C4hXkTeb81GhLj8GBTwDxRxvBy2xZxVFh4YfthKA6XJ", // [0] - initial root
            "3URxLevuDY3as6511nB9NjjK97AMbuW4CqHVskpUzL5z", // [1] - after BatchNullify seq 1
            "2MCdDFbK4BB9vKEaU5pU4gTt73Mpr8Dj3UARcMGJFrxY", // [2] - after BatchNullify seq 2
            "2MCdDFbK4BB9vKEaU5pU4gTt73Mpr8Dj3UARcMGJFrxY", // [3] - after BatchAppend seq 3 (same root - spent accounts)
            "2g28e2qGhzE3gtiGdHLmk62RRKMzfJE2r9sc6Ftqkiq8", // [4] - after BatchAppend seq 4
            "2qjXpKKXF3bsGduLknigLb8oVrZMP3tAnAAKbLVbx3PQ", // [5] - after BatchAppend seq 5 (should be this)
        ];

        println!("\nExpected root progression:");
        for (i, root) in ON_CHAIN_ROOT_HISTORY.iter().enumerate() {
            let marker = if i as i64 == seq { " <-- Current" } else { "" };
            println!("  Seq {}: {}{}", i, root, marker);
        }

        if (seq as usize) < ON_CHAIN_ROOT_HISTORY.len() {
            let expected_on_chain_root = ON_CHAIN_ROOT_HISTORY[seq as usize];

            println!("\nRoot comparison at sequence {}:", seq);
            println!("  Expected on-chain root: {}", expected_on_chain_root);
            println!("  Actual indexer root:    {}", root_hash);

            assert_eq!(
                root_hash, expected_on_chain_root,
                "\nROOT MISMATCH at sequence {}!\n\
                Expected on-chain root: {}\n\
                Got indexer root:       {}\n\n\
                This occurs after BatchNullify operations followed by BatchAppend of spent accounts.\n\
                The indexer incorrectly processes spent accounts during batch append operations.",
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
        println!("  2. BatchNullify (seq 1) - nullifies accounts in queue 0-10");
        println!("  3. BatchNullify (seq 2) - nullifies accounts in queue 10-20");
        println!("  4. BatchAppend (seq 3) - processes spent accounts 0-10");
        println!("  5. BatchAppend (seq 4) - processes spent accounts 10-20");
        println!("  6. BatchAppend (seq 5) - processes spent accounts 20-30");
    } else {
        panic!("No root node found in state_trees for tree HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu - transactions were not indexed properly");
    }
}
