use crate::utils::*;
use function_name::named;
use light_hasher::hash_to_field_size::hashv_to_bn254_field_size_be_const_array;
use light_hasher::Poseidon;
use num_bigint::BigUint;
use photon_indexer::api::method::get_multiple_new_address_proofs::{
    AddressListWithTrees, AddressWithTree,
};
use photon_indexer::api::method::get_queue_elements::{GetQueueElementsRequest, QueueRequest};
use photon_indexer::common::typedefs::serializable_pubkey::SerializablePubkey;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use serial_test::serial;
use solana_pubkey::Pubkey;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use std::str::FromStr;

/// Derives a compressed address from the provided seed, merkle tree pubkey, and program ID.
fn derive_address(
    seed: &[u8; 32],
    merkle_tree_pubkey: &[u8; 32],
    program_id_bytes: &[u8; 32],
) -> [u8; 32] {
    let slices = [
        seed.as_slice(),
        merkle_tree_pubkey.as_slice(),
        program_id_bytes.as_slice(),
    ];
    hashv_to_bn254_field_size_be_const_array::<4>(&slices).unwrap()
}

/// Tests the batched address update process with varying transaction configurations.
///
/// Test flow:
/// 1. Index transactions creating compressed addresses via CPI.
/// 2. Verify address queue population reflects the indexed state.
/// 3. Index transaction performing BatchAddressUpdate instruction.
/// 4. Verify address queue is cleared by the indexer processing the update.
/// 5. Verify the final Merkle tree root and proofs against a reference tree.
///
/// # Parameters
/// * `db_backend` - The database backend to use (Sqlite or Postgres)
/// * `addresses_per_tx` - Number of addresses generated per transaction
/// * `num_creation_txs` - Number of creation transactions to process
/// * `test_name` - The original test name to use for finding test data files
async fn run_batched_address_test(
    db_backend: DatabaseBackend,
    addresses_per_tx: usize,
    num_creation_txs: usize,
    test_name: &str,
) {
    // --- Test Setup ---
    let name = test_name;
    let setup = setup_with_options(
        name.parse().unwrap(),
        TestSetupOptions {
            network: Network::Localnet,
            db_backend,
        },
    )
    .await;
    reset_tables(setup.db_conn.as_ref()).await.unwrap();
    let sort_by_slot = true;
    let signatures = read_file_names(&name, sort_by_slot);
    let index_individually = true; // Index one by one

    for (i, sig) in signatures.iter().enumerate() {
        println!("{} signature {}", i, sig);
    }

    assert!(
        !signatures.is_empty(),
        "No transaction signatures found for test {}",
        name
    );

    // =========================================================================
    // Constants and known values
    let address_tree_pubkey =
        Pubkey::from_str("EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK").expect("Invalid Pubkey");

    let program_id =
        Pubkey::from_str("FNt7byTHev1k5x2cXZLBr8TdWiC3zoP5vcnZR4P682Uy").expect("Invalid Pubkey");

    // Generate expected addresses
    let total_addresses = num_creation_txs * addresses_per_tx;
    let mut expected_addresses: Vec<([u8; 32], u64)> = Vec::new();
    let seed = 0;
    let mut rng = StdRng::seed_from_u64(seed);

    for i in 0..total_addresses {
        let seed = rng.gen();
        let address = derive_address(
            &seed,
            &address_tree_pubkey.to_bytes(),
            &program_id.to_bytes(),
        );
        println!("{} address: {:?}", i, address);
        expected_addresses.push((address, i as u64));
    }

    assert!(
        !expected_addresses.is_empty(),
        "expected_addresses list cannot be empty. Provide the known data."
    );

    assert!(
        signatures.len() > num_creation_txs,
        "Signatures list should contain creation txs + at least one batch update tx"
    );

    // --- Phase 1: Index Address Creation Transactions ---
    let creation_signatures = &signatures[..num_creation_txs]; // Assume first N are creations
    let batch_update_signatures = &signatures[num_creation_txs..]; // Assume the transaction *immediately following* the creations is the batch update

    println!(
        "Indexing {} address creation transactions ({} addresses per tx)...",
        creation_signatures.len(),
        addresses_per_tx
    );

    for (i, signature) in creation_signatures.iter().enumerate() {
        println!(
            "Indexing creation signature {}/{}: {}",
            i + 1,
            num_creation_txs,
            signature
        );
        index(
            &name,
            setup.db_conn.clone(),
            setup.client.clone(),
            &[signature.clone()],
            index_individually,
        )
        .await;

        // Verify the tree pubkey derived from tx matches the expected one (optional sanity check)
        let json_str =
            std::fs::read_to_string(format!("tests/data/transactions/{}/{}", name, signature))
                .unwrap();
        let tx_meta: EncodedConfirmedTransactionWithStatusMeta =
            serde_json::from_str(&json_str).expect("Failed to parse transaction JSON");
        let accounts = tx_meta
            .transaction
            .transaction
            .decode()
            .map(|tx| tx.message.static_account_keys().to_vec())
            .unwrap_or_default()
            .into_iter()
            .map(|key| Pubkey::from(key.to_bytes()))
            .collect::<Vec<_>>();

        assert!(
            accounts.contains(&address_tree_pubkey),
            "Indexed tx {} does not involve the expected address tree {}",
            signature,
            address_tree_pubkey
        );
    }

    // --- Verify Address Queue State BEFORE Batch Update ---
    println!("Verifying address queue state before batch update...");
    let queue_elements_before = setup
        .api
        .get_queue_elements(GetQueueElementsRequest {
            tree: address_tree_pubkey.to_bytes().into(),
            output_queue: None,
            input_queue: None,
            address_queue: Some(QueueRequest {
                limit: 100,
                start_index: None,
                zkp_batch_size: None,
            }),
        })
        .await
        .expect("Failed to get address queue elements before batch update");

    let address_queue_before = queue_elements_before
        .address_queue
        .expect("Address queue should be present");

    assert_eq!(
        address_queue_before.addresses.len(),
        total_addresses,
        "Address queue length mismatch before batch update"
    );

    println!("expected_addresses len: {}", expected_addresses.len());
    println!(
        "addresses in queue len: {}",
        address_queue_before.addresses.len()
    );

    for (i, element) in address_queue_before.addresses.iter().enumerate() {
        assert_eq!(
            element.0.to_bytes(),
            expected_addresses[i].0, // Compare the underlying [u8; 32]
            "Address queue content mismatch at index {} before batch update",
            i
        );
    }

    println!("Address queue state verified before batch update.");

    // --- Phase 2: Index Batch Update Transaction ---
    for signature in batch_update_signatures {
        println!("Indexing batch update signature: {}", signature);
        index(
            &name,
            setup.db_conn.clone(),
            setup.client.clone(),
            &[signature.clone()],
            index_individually,
        )
        .await;
    }

    // --- Verify Address Queue State AFTER Batch Update ---
    println!("Verifying address queue state after batch update...");
    let queue_elements_after = setup
        .api
        .get_queue_elements(GetQueueElementsRequest {
            tree: address_tree_pubkey.to_bytes().into(),
            output_queue: None,
            input_queue: None,
            address_queue: Some(QueueRequest {
                limit: 100,
                start_index: None,
                zkp_batch_size: None,
            }),
        })
        .await
        .expect("Failed to get address queue elements after batch update");

    let address_queue_after = queue_elements_after.address_queue;
    println!("Queue elements after update: {:?}", address_queue_after);
    let addresses_after = address_queue_after.map(|q| q.addresses).unwrap_or_default();
    assert!(
        addresses_after.is_empty(),
        "Address queue should be empty after batch update, but found {} elements",
        addresses_after.len()
    );
    println!("Address queue state verified after batch update (empty).");

    // --- Phase 3: Verify Final Tree State and Proofs ---
    let mut reference_tree =
        light_merkle_tree_reference::indexed::IndexedMerkleTree::<Poseidon, usize>::new(40, 0)
            .unwrap();

    for (hash, _) in &expected_addresses {
        let hash_bn = BigUint::from_bytes_be(hash);
        reference_tree
            .append(&hash_bn)
            .expect("Failed to update reference tree");
    }
    let final_reference_root = reference_tree.root();
    let new_addresses: Vec<AddressWithTree> = vec![AddressWithTree {
        address: SerializablePubkey::from(Pubkey::from(expected_addresses[0].0)),
        tree: SerializablePubkey::from(Pubkey::new_from_array(address_tree_pubkey.to_bytes())),
    }];
    let proof = setup
        .api
        .get_multiple_new_address_proofs_v2(AddressListWithTrees(new_addresses))
        .await
        .expect("Failed to get multiple new address proofs");

    let proof_root = proof.value.first().unwrap().root.0;
    assert_eq!(final_reference_root, proof_root, "Final tree root mismatch");

    println!("Final tree state and proofs verified.");
}

/// Test with 1 address per transaction (50 transactions for 50 addresses)
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_batched_address_transactions(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    // 1 address per transaction, 50 transactions
    let test_name = trim_test_name(function_name!());
    run_batched_address_test(db_backend, 1, 50, &test_name).await;
}

/// Test with 2 addresses per transaction (25 transactions for 50 addresses)
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_batched_address_2_transactions(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    // 2 addresses per transaction, 25 transactions
    let test_name = trim_test_name(function_name!());
    run_batched_address_test(db_backend, 2, 25, &test_name).await;
}
