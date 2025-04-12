use crate::utils::*;
use borsh::BorshSerialize;
use function_name::named;
use light_compressed_account::QueueType;
use light_hasher::hash_to_field_size::hashv_to_bn254_field_size_be_const_array;
use light_hasher::zero_bytes::poseidon::ZERO_BYTES;
use light_hasher::Poseidon;
use light_merkle_tree_reference::MerkleTree;
use photon_indexer::api::method::get_batch_address_update_info::GetBatchAddressUpdateInfoRequest;
use photon_indexer::api::method::get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest;
use photon_indexer::api::method::get_compressed_token_balances_by_owner::{
    GetCompressedTokenBalancesByOwnerRequest, TokenBalance,
};
use photon_indexer::api::method::get_multiple_compressed_account_proofs::HashList;
use photon_indexer::api::method::get_queue_elements::GetQueueElementsRequest;
use photon_indexer::api::method::get_transaction_with_compression_info::{
    get_transaction_helper, get_transaction_helper_v2,
};
use photon_indexer::api::method::get_validity_proof::GetValidityProofRequestV2;
use photon_indexer::api::method::utils::GetCompressedTokenAccountsByOwner;
use photon_indexer::common::typedefs::hash::Hash;
use photon_indexer::common::typedefs::serializable_pubkey::SerializablePubkey;
use photon_indexer::common::typedefs::serializable_signature::SerializableSignature;
use photon_indexer::common::typedefs::token_data::TokenData;
use photon_indexer::common::typedefs::unsigned_integer::UnsignedInteger;
use photon_indexer::ingester::index_block;
use photon_indexer::ingester::persist::COMPRESSED_TOKEN_PROGRAM;
use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use sea_orm::DatabaseConnection;
use serial_test::serial;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use std::str::FromStr;
use std::sync::Arc;

/// Test:
/// 1. get compressed account by owner
/// 2. get compressed account proofs
/// 3. correct root update after batch append and batch nullify events
/// 4. get_validity_proof_v2
/// 5. get_queue_elements
///
/// Data:
/// - 50 active compressed accounts with 1_000_000 each owned by Pubkey::new_unique()
/// - 50 nullified compressed accounts
/// - all accounts are inserted into the batched Merkle tree
/// - 10 append events and 5 nullify events (zero knowledge proof size 10)
/// - queues are empty once all transactions are indexed
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_batched_tree_transactions(
    #[values(DatabaseBackend::Sqlite)] db_backend: DatabaseBackend,
) {
    let trim_test_name = trim_test_name(function_name!());
    let name = trim_test_name;
    let setup = setup_with_options(
        name.clone(),
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

    for (i, signature) in signatures.iter().enumerate() {
        println!("{} signature {}", i, signature);
    }
    // build tree
    let mut merkle_tree =
        light_merkle_tree_reference::MerkleTree::<light_hasher::Poseidon>::new(32, 0);
    let mut merkle_tree_pubkey = Pubkey::default();
    let mut queue_pubkey = Pubkey::default();
    let mut output_queue_len = 0;
    let mut input_queue_len = 0;
    let mut output_queue_elements = Vec::new();
    let mut input_queue_elements = Vec::new();
    let num_append_events = 10;
    let num_nullify_events = 5;
    for signature in
        signatures[..signatures.len() - (num_append_events + num_nullify_events)].iter()
    {
        // Index transactions.
        index(
            &name,
            setup.db_conn.clone(),
            setup.client.clone(),
            &[signature.to_string()],
            index_individually,
        )
        .await;
        let json_str =
            std::fs::read_to_string(format!("tests/data/transactions/{}/{}", name, signature))
                .unwrap();
        let transaction: EncodedConfirmedTransactionWithStatusMeta =
            serde_json::from_str(&json_str).unwrap();

        // use get_transaction_helper because get_transaction_with_compression_info requires an rpc endpoint.
        // It fetches the instruction and parses the data.
        let accounts = get_transaction_helper_v2(
            &setup.db_conn,
            SerializableSignature(Signature::from_str(signature).unwrap()),
            transaction,
        )
        .await
        .unwrap()
        .compressionInfo;

        for account in accounts.closedAccounts.iter() {
            input_queue_elements
                .push((account.account.account.hash.0, account.account.nullifier.0));
            merkle_tree
                .update(
                    &account.account.nullifier.0,
                    account.account.account.leaf_index.0 as usize,
                )
                .unwrap();
        }
        for account in accounts.openedAccounts.iter() {
            output_queue_elements.push(account.account.hash.0);
            while merkle_tree.rightmost_index <= account.account.leaf_index.0 as usize + 2 {
                merkle_tree.append(&[0u8; 32]).unwrap();
            }
            merkle_tree
                .update(
                    &account.account.hash.0,
                    account.account.leaf_index.0 as usize,
                )
                .unwrap();
        }

        // Get output queue elements
        if !accounts.openedAccounts.is_empty() {
            output_queue_len += accounts.openedAccounts.len();
            merkle_tree_pubkey = accounts.openedAccounts[0].account.merkle_context.tree.0;
            queue_pubkey = accounts.openedAccounts[0].account.merkle_context.queue.0;
            let get_queue_elements_result = setup
                .api
                .get_queue_elements(GetQueueElementsRequest {
                    tree: merkle_tree_pubkey.to_bytes().into(),
                    start_offset: None,
                    queue_type: QueueType::BatchedOutput as u8,
                    num_elements: 100,
                })
                .await
                .unwrap();
            assert_eq!(get_queue_elements_result.value.len(), output_queue_len);
            for (i, element) in get_queue_elements_result.value.iter().enumerate() {
                assert_eq!(element.account_hash.0, output_queue_elements[i]);
                let proof = element.proof.iter().map(|x| x.0).collect::<Vec<[u8; 32]>>();
                assert_eq!(proof, ZERO_BYTES[..proof.len()].to_vec());
            }
        }
        // Get input queue elements
        if !accounts.closedAccounts.is_empty() {
            input_queue_len += accounts.closedAccounts.len();
            merkle_tree_pubkey = accounts.closedAccounts[0]
                .account
                .account
                .merkle_context
                .tree
                .0;
            let get_queue_elements_result = setup
                .api
                .get_queue_elements(GetQueueElementsRequest {
                    tree: merkle_tree_pubkey.to_bytes().into(),
                    start_offset: None,
                    queue_type: QueueType::BatchedInput as u8,
                    num_elements: 100,
                })
                .await
                .unwrap();
            assert_eq!(get_queue_elements_result.value.len(), input_queue_len);
            for (i, element) in get_queue_elements_result.value.iter().enumerate() {
                assert_eq!(element.account_hash.0, input_queue_elements[i].0);
                let proof = element.proof.iter().map(|x| x.0).collect::<Vec<[u8; 32]>>();
                assert_eq!(proof, ZERO_BYTES[..proof.len()].to_vec());
            }
        }
    }
    let filtered_outputs = output_queue_elements
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 2 == 1)
        .map(|(_, x)| x)
        .collect::<Vec<&[u8; 32]>>();
    for (_, chunk) in filtered_outputs.chunks(4).enumerate() {
        let validity_proof = setup
            .api
            .get_validity_proof_v2(GetValidityProofRequestV2 {
                hashes: chunk
                    .iter()
                    .map(|x| Hash::new(&x[..]).unwrap())
                    .collect::<Vec<_>>(),
                newAddressesWithTrees: vec![],
            })
            .await
            .unwrap();

        // No value has been inserted into the tree yet -> all proof by index.
        assert!(validity_proof
            .value
            .rootIndices
            .iter()
            .all(|x| x.prove_by_index));
        assert!(validity_proof
            .value
            .merkle_contexts
            .iter()
            .all(|x| x.tree.0.to_string() == merkle_tree_pubkey.to_string()));
        assert!(validity_proof
            .value
            .merkle_contexts
            .iter()
            .all(|x| x.queue.0.to_string() == queue_pubkey.to_string()));
        assert!(validity_proof.value.roots.iter().all(|x| x.is_empty()));
    }

    // Merkle tree which is created along side indexing the event transactions.
    let mut event_merkle_tree =
        light_merkle_tree_reference::MerkleTree::<light_hasher::Poseidon>::new(32, 0);
    // Init all 100 elements so that we can just update.
    for _ in 0..100 {
        event_merkle_tree.append(&[0u8; 32]).unwrap();
    }
    let mut last_inserted_index = 0;

    println!("====");
    for (i, signature) in signatures[signatures.len() - (num_append_events + num_nullify_events)..]
        .iter()
        .take(15)
        .enumerate()
    {
        println!("{} {}", i, signature);
    }
    println!("====");
    // Index and assert the batch events.
    // 10 append events and 5 nullify events.
    // The order is:
    // append, nullify, append, nullify, append, nullify, append, nullify, append, append, append, append, append, append, nullify
    for (i, signature) in signatures[signatures.len() - (num_append_events + num_nullify_events)..]
        .iter()
        .take(15)
        .enumerate()
    {
        let pre_output_queue_elements = setup
            .api
            .get_queue_elements(GetQueueElementsRequest {
                tree: merkle_tree_pubkey.to_bytes().into(),
                start_offset: None,
                queue_type: QueueType::BatchedOutput as u8,
                num_elements: 100,
            })
            .await
            .unwrap();
        let pre_input_queue_elements = setup
            .api
            .get_queue_elements(GetQueueElementsRequest {
                tree: merkle_tree_pubkey.to_bytes().into(),
                start_offset: None,
                queue_type: QueueType::BatchedInput as u8,
                num_elements: 100,
            })
            .await
            .unwrap();
        // Index transactions.
        index(
            &name,
            setup.db_conn.clone(),
            setup.client.clone(),
            &[signature.to_string()],
            index_individually,
        )
        .await;
        let post_output_queue_elements = setup
            .api
            .get_queue_elements(GetQueueElementsRequest {
                tree: merkle_tree_pubkey.to_bytes().into(),
                start_offset: None,
                queue_type: QueueType::BatchedOutput as u8,
                num_elements: 100,
            })
            .await
            .unwrap();
        let post_input_queue_elements = setup
            .api
            .get_queue_elements(GetQueueElementsRequest {
                tree: merkle_tree_pubkey.to_bytes().into(),
                start_offset: None,
                queue_type: QueueType::BatchedInput as u8,
                num_elements: 100,
            })
            .await
            .unwrap();
        let is_nullify_event = i > 9;
        if is_nullify_event {
            println!("nullify event {} {}", i, signature);
            assert_eq!(
                post_output_queue_elements.value.len(),
                pre_output_queue_elements.value.len(),
                "Nullify event should not change the length of the output queue."
            );
            assert_eq!(
                post_input_queue_elements.value.len(),
                pre_input_queue_elements.value.len() - 10,
                "Nullify event should decrease the length of the input queue by 10."
            );
            // Insert 1 batch.
            for element in pre_input_queue_elements.value[..10].iter() {
                println!("nullify leaf index {}", element.leaf_index);
                let nullifier = input_queue_elements
                    .iter()
                    .find(|x| x.0 == element.account_hash.0)
                    .unwrap()
                    .1;
                event_merkle_tree
                    .update(&nullifier, element.leaf_index as usize)
                    .unwrap();
            }
            for element in post_input_queue_elements.value.iter() {
                let proof_result = event_merkle_tree
                    .get_proof_of_leaf(element.leaf_index as usize, true)
                    .unwrap()
                    .to_vec();
                let proof = element.proof.iter().map(|x| x.0).collect::<Vec<[u8; 32]>>();
                assert_eq!(proof, proof_result);
            }
        } else {
            last_inserted_index += 10;
            assert_eq!(
                post_input_queue_elements.value.len(),
                pre_input_queue_elements.value.len(),
                "Append event should not change the length of the input queue."
            );
            assert_eq!(
                post_output_queue_elements.value.len(),
                pre_output_queue_elements.value.len().saturating_sub(10),
                "Append event should decrease the length of the output queue by 10."
            );

            println!(
                "post input queue len {}",
                post_input_queue_elements.value.len(),
            );
            println!(
                "pre input queue len {}",
                pre_input_queue_elements.value.len(),
            );
            // Insert 1 batch.

            let slice_length = pre_output_queue_elements.value.len().min(10);
            for element in pre_output_queue_elements.value[..slice_length].iter() {
                // for element in pre_output_queue_elements.value[..10].iter() {
                let leaf = event_merkle_tree.leaf(element.leaf_index as usize);
                if leaf == [0u8; 32] {
                    event_merkle_tree
                        .update(&element.account_hash.0, element.leaf_index as usize)
                        .unwrap();
                    println!("append leaf index {}", element.leaf_index);
                }
            }
            for element in post_output_queue_elements.value.iter() {
                let proof_result = event_merkle_tree
                    .get_proof_of_leaf(element.leaf_index as usize, true)
                    .unwrap()
                    .to_vec();
                let proof = element.proof.iter().map(|x| x.0).collect::<Vec<[u8; 32]>>();
                assert_eq!(proof, proof_result);
            }
        }
        for (j, chunk) in filtered_outputs.chunks(4).enumerate() {
            let validity_proof = setup
                .api
                .get_validity_proof_v2(GetValidityProofRequestV2 {
                    hashes: chunk
                        .iter()
                        .map(|x| Hash::new(&x[..]).unwrap())
                        .collect::<Vec<_>>(),
                    newAddressesWithTrees: vec![],
                })
                .await
                .unwrap();
            println!("j {}, validity_proof {:?}", j, validity_proof.value);

            // No value has been inserted into the tree yet -> all proof by index.
            let mut base_index = j * 8;
            for (z, (root_index, root)) in validity_proof
                .value
                .rootIndices
                .iter()
                .zip(validity_proof.value.roots.iter())
                .enumerate()
            {
                println!("z + base index {} {}", z, base_index);
                println!("last inserted index {}", last_inserted_index);
                if base_index < last_inserted_index {
                    assert!(!root_index.prove_by_index);
                } else {
                    assert!(root_index.prove_by_index);
                    assert_eq!(root, "");
                }
                base_index += 2;
            }
            assert!(validity_proof
                .value
                .merkle_contexts
                .iter()
                .all(|x| x.tree.0.to_string() == merkle_tree_pubkey.to_string()));
            assert!(validity_proof
                .value
                .merkle_contexts
                .iter()
                .all(|x| x.queue.0.to_string() == queue_pubkey.to_string()));
        }
    }
    assert_eq!(event_merkle_tree.root(), merkle_tree.root());
    assert_eq!(output_queue_len, 100);
    assert_eq!(input_queue_len, 50);
    let get_queue_elements_result = setup
        .api
        .get_queue_elements(GetQueueElementsRequest {
            tree: merkle_tree_pubkey.to_bytes().into(),
            start_offset: None,
            queue_type: QueueType::BatchedOutput as u8,
            num_elements: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        get_queue_elements_result.value.len(),
        0,
        "Batched append events not indexed correctly."
    );

    let get_queue_elements_result = setup
        .api
        .get_queue_elements(GetQueueElementsRequest {
            tree: merkle_tree_pubkey.to_bytes().into(),
            start_offset: None,
            queue_type: QueueType::BatchedInput as u8,
            num_elements: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        get_queue_elements_result.value.len(),
        0,
        "Batched nullify events not indexed correctly."
    );

    // Reprocess the same transactions.
    index(
        &name,
        setup.db_conn.clone(),
        setup.client.clone(),
        &signatures,
        index_individually,
    )
    .await;
    // Slot created is wrong likely because of test environment.
    let mut leaf_index = 1;
    for i in 0..50 {
        let owner = Pubkey::new_unique();
        let accounts = setup
            .api
            .get_compressed_accounts_by_owner_v2(GetCompressedAccountsByOwnerRequest {
                owner: SerializablePubkey::from(owner.to_bytes()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(accounts.value.items.len(), 1);
        let account = &accounts.value.items[0];
        assert_eq!(account.lamports.0, 1_000_000u64);
        assert_eq!(account.owner.0, owner);
        assert_eq!(
            account.leaf_index.0,
            leaf_index,
            "owner {:?} i {}",
            owner.to_bytes(),
            i
        );

        let reference_merkle_proof = merkle_tree
            .get_proof_of_leaf(leaf_index as usize, true)
            .unwrap()
            .to_vec();
        let merkle_proof = setup
            .api
            .get_multiple_compressed_account_proofs(HashList(vec![account.hash.clone()]))
            .await
            .unwrap();
        assert_eq!(merkle_proof.value[0].hash.0, account.hash.0,);
        assert_eq!(
            merkle_proof.value[0].hash.0,
            merkle_tree.leaf(leaf_index as usize)
        );
        assert_eq!(account.hash.0, merkle_tree.leaf(leaf_index as usize));
        assert_eq!(merkle_proof.value.len(), 1);
        assert_eq!(merkle_proof.value[0].root.0, merkle_tree.root());
        assert_eq!(
            merkle_proof.value[0]
                .proof
                .iter()
                .map(|x| x.0)
                .collect::<Vec<[u8; 32]>>(),
            reference_merkle_proof
        );
        leaf_index += 2;
    }
}

/// Test correct indexing of token accounts in a batched state Merkle tree.
/// Data:
/// - 4 recipients with 1 token account each
/// - 1 sender with 3 token accounts
///
/// Asserts:
/// 1. Sender has 3 token accounts with 12341 balance each.
/// 2. Recipients have 1 token account each with 9255, 9255, 9255, 9258 balance.
/// 3. Sender's token balances are correct.
/// 4. Recipients' token balances are correct.
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_batched_tree_token_transactions(
    #[values(DatabaseBackend::Sqlite)] db_backend: DatabaseBackend,
) {
    for index_individually in [true] {
        let trim_test_name = trim_test_name(function_name!());
        let name = trim_test_name;
        let setup = setup_with_options(
            name.clone(),
            TestSetupOptions {
                network: Network::Localnet,
                db_backend,
            },
        )
        .await;
        // reset_tables doesn't seem to work.
        reset_tables(setup.db_conn.as_ref()).await.unwrap();
        let sort_by_slot = true;
        let signatures = read_file_names(&name, sort_by_slot);
        println!("signatures {:?}", signatures);
        // Index first transaction.
        index(
            &name,
            setup.db_conn.clone(),
            setup.client.clone(),
            &[signatures[0].clone()],
            index_individually,
        )
        .await;

        let mint = SerializablePubkey::from(
            Pubkey::from_str("7LasgAqT1hWtGEHo6BRPPLN56CWnZKdNk1GwWk7k9zjK").unwrap(),
        );
        let recipients = [
            Pubkey::from_str("DyRWDm81iYePWsdw1Yn2ue8CPcp7Lba6XsB8DVSGM7HK").unwrap(),
            Pubkey::from_str("3YzfcCyqUPE9oubX2Ct9xWn1u5urqmGu6wfcFavHsCQZ").unwrap(),
            Pubkey::from_str("2ShDKqkcMmacgYeSsEjwjLVJcoERZ9jgZ8tFyssxd82S").unwrap(),
            Pubkey::from_str("24fLJv6tHmsxQg5vDD7XWy85TMhFzJdkqZ9Ta3LtVReU").unwrap(),
        ];

        // sender En9a97stB3Ek2n6Ey3NJwCUJnmTzLMMEA5C69upGDuQP should have spent 3 inputs with 12341 each.
        let sender = Pubkey::from_str("En9a97stB3Ek2n6Ey3NJwCUJnmTzLMMEA5C69upGDuQP").unwrap();
        let expected_sender_token_data = TokenData {
            mint,
            owner: SerializablePubkey::from(sender),
            tlv: None,
            amount: UnsignedInteger(12341),
            delegate: None,
            state: photon_indexer::common::typedefs::token_data::AccountState::initialized,
        };

        // 1. assert sender created token accounts
        //      3 accounts with balance 12341 each
        {
            let mut accounts = setup
                .api
                .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwner {
                    owner: SerializablePubkey::from(sender),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(accounts.value.items.len(), 3);
            accounts.value.items.sort_by_key(|a| a.account.leaf_index.0);
            for (i, account) in accounts.value.items.iter().enumerate() {
                assert_eq!(
                    account.token_data, expected_sender_token_data,
                    "Expected sender token data to be {:?}",
                    expected_sender_token_data
                );
                let mut token_data_bytes = Vec::new();
                account.token_data.serialize(&mut token_data_bytes).unwrap();

                assert_eq!(account.account.address, None);
                assert_eq!(account.account.lamports, UnsignedInteger(1_000_000));
                assert_eq!(
                    account.account.owner,
                    SerializablePubkey::from(COMPRESSED_TOKEN_PROGRAM)
                );
                assert_eq!(account.account.leaf_index.0, i as u64);
                assert_eq!(account.account.seq, None);
                assert!(account.account.data.is_some());
            }
            let sender_balance = setup
                .api
                .get_compressed_token_balances_by_owner(GetCompressedTokenBalancesByOwnerRequest {
                    owner: SerializablePubkey::from(sender),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(sender_balance.value.token_balances.len(), 1);
            assert_eq!(
                sender_balance.value.token_balances[0],
                TokenBalance {
                    mint,
                    balance: UnsignedInteger(12341 * 3),
                }
            );
        }
        let mut expected_recipient_token_data_vec = Vec::new();

        // Reprocess the first transaction, and process the second transaction.
        index(
            &name,
            setup.db_conn.clone(),
            setup.client.clone(),
            &signatures,
            index_individually,
        )
        .await;

        // 2. assert sender nullified token accounts when sent to recipients
        {
            let json_str = std::fs::read_to_string(format!(
                "tests/data/transactions/{}/{}",
                name, signatures[1]
            ))
            .unwrap();
            let transaction: EncodedConfirmedTransactionWithStatusMeta =
                serde_json::from_str(&json_str).unwrap();

            // use get_transaction_helper because get_transaction_with_compression_info requires an rpc endpoint.
            // It fetches the instruction and parses the data.
            let accounts = get_transaction_helper(
                &setup.db_conn,
                SerializableSignature(Signature::from_str(&signatures[1]).unwrap()),
                transaction,
            )
            .await
            .unwrap()
            .compressionInfo;
            assert_eq!(accounts.closedAccounts.len(), 3);
            // 4 recipients + 1 change account for sol
            assert_eq!(accounts.openedAccounts.len(), 4 + 1);
            for account in accounts.closedAccounts.iter() {
                assert_eq!(
                    *account.optionalTokenData.as_ref().unwrap(),
                    expected_sender_token_data
                );
            }

            for (i, account) in accounts.openedAccounts.iter().enumerate() {
                // Skip sol change account.
                let account_token_data = if let Some(account) = &account.optionalTokenData {
                    account
                } else {
                    continue;
                };
                let mut amount = 9255;
                if i == 3 {
                    amount += 3;
                }
                let owner = recipients[i];
                let expected_recipient_token_data = TokenData {
                    mint,
                    owner: SerializablePubkey::from(owner),
                    tlv: None,
                    amount: UnsignedInteger(amount),
                    delegate: None,
                    state: photon_indexer::common::typedefs::token_data::AccountState::initialized,
                };
                expected_recipient_token_data_vec.push(expected_recipient_token_data.clone());

                assert_eq!(*account_token_data, expected_recipient_token_data);
            }
        }
        // 3. assert recipients:
        //      1. created token accounts (get_compressed_token_accounts_by_owner)
        //      2. token balances (get_compressed_token_balances_by_owner)
        for expected_recipient_token_data in expected_recipient_token_data_vec.iter() {
            let accounts = setup
                .api
                .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwner {
                    owner: expected_recipient_token_data.owner,
                    ..Default::default()
                })
                .await
                .unwrap();

            assert_eq!(
                accounts.value.items.len(),
                1,
                "Expected 1 compressed token account per recipient."
            );
            let account = &accounts.value.items[0];
            assert_eq!(account.token_data, *expected_recipient_token_data);

            let accounts = setup
                .api
                .get_compressed_token_balances_by_owner(GetCompressedTokenBalancesByOwnerRequest {
                    owner: expected_recipient_token_data.owner,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(accounts.value.token_balances.len(), 1);
            let balance = &accounts.value.token_balances[0];
            let expected_balance = TokenBalance {
                mint,
                balance: expected_recipient_token_data.amount,
            };
            assert_eq!(*balance, expected_balance);
        }
    }
}

/// Test indexes a transaction which creates
/// 4 compressed accounts in 4 cpis that create a transaction event each
/// in one outer instruction.
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_four_cpi_events(#[values(DatabaseBackend::Postgres)] db_backend: DatabaseBackend) {
    let trim_test_name = trim_test_name(function_name!());
    let name = trim_test_name;
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Localnet,
            db_backend,
        },
    )
    .await;
    reset_tables(setup.db_conn.as_ref()).await.unwrap();
    let sort_by_slot = true;
    let signatures = read_file_names(&name, sort_by_slot);
    let index_individually = false;
    let owner = Pubkey::from_str("FNt7byTHev1k5x2cXZLBr8TdWiC3zoP5vcnZR4P682Uy").unwrap();

    index(
        &name,
        setup.db_conn.clone(),
        setup.client.clone(),
        &signatures,
        index_individually,
    )
    .await;
    let mut leaf_index = 0;
    let accounts = setup
        .api
        .get_compressed_accounts_by_owner_v2(GetCompressedAccountsByOwnerRequest {
            owner: SerializablePubkey::from(owner.to_bytes()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(accounts.value.items.len(), 4);
    for (i, account) in accounts.value.items.iter().enumerate() {
        assert_eq!(account.lamports.0, 0u64);
        assert_eq!(account.owner.0, owner);
        assert_eq!(
            account.leaf_index.0,
            leaf_index,
            "owner {:?} i {}",
            owner.to_bytes(),
            i
        );
        leaf_index += 1;
    }
}

/// Test:
/// 1. Index transactions creating compressed addresses via CPI.
/// 2. Verify address queue population reflects indexed state.
/// 3. Index transaction performing BatchUpdateAddressTree.
/// 4. Verify address queue is cleared by the indexer processing the update.
/// 5. Verify final Merkle tree root and proofs against a reference tree.
///
/// Data:
/// - Transactions generated from `test_create_v2_address` run.
/// - Includes multiple address creation CPIs (`InsertIntoQueues`).
/// - Includes one `BatchUpdateAddressTree` instruction.
///
/// Assumption: The exact sequence of (address hash, leaf index) pairs and the
/// address tree pubkey created during the `test_create_v2_address` run are known
/// and provided/hardcoded below.
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_batched_address_transactions(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    pub fn derive_address(
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

    // --- Test Setup ---
    let trim_test_name = trim_test_name(function_name!());
    let name = trim_test_name;
    let setup = setup_with_options(
        name.clone(),
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
    let address_tree_pubkey =
        Pubkey::from_str("EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK").expect("Invalid Pubkey");

    let program_id =
        Pubkey::from_str("FNt7byTHev1k5x2cXZLBr8TdWiC3zoP5vcnZR4P682Uy").expect("Invalid Pubkey");

    let mut expected_addresses: Vec<([u8; 32], u64)> = Vec::new();
    let seed = 0;
    let mut rng = StdRng::seed_from_u64(seed);
    let num_creation_txs: usize = 50;
    for i in 0..num_creation_txs {
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

    let mut reference_address_tree = MerkleTree::<Poseidon>::new(40, 0);

    // --- Phase 1: Index Address Creation Transactions ---
    let creation_signatures = &signatures[..num_creation_txs]; // Assume first N are creations
    let batch_update_signatures = &signatures[num_creation_txs..]; // Assume the transaction *immediately following* the creations is the batch update

    println!(
        "Indexing {} address creation transactions...",
        creation_signatures.len()
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
            .unwrap_or_default();
        assert!(
            accounts.contains(&address_tree_pubkey),
            "Indexed tx {} does not involve the expected address tree {}",
            signature,
            address_tree_pubkey
        );

        // Append zero leaves to reference tree - we update hashes later after the batch update is processed conceptually
        while reference_address_tree.get_next_index() <= 100 {
            reference_address_tree
                .append(&ZERO_BYTES[0])
                .expect("Failed to append to reference tree");
        }
    }

    // --- Verify Address Queue State BEFORE Batch Update ---
    println!("Verifying address queue state before batch update...");
    let queue_elements_before = setup
        .api
        .get_batch_address_update_info(GetBatchAddressUpdateInfoRequest {
            tree: address_tree_pubkey.to_bytes().into(),
            batch_size: 50,
        })
        .await
        .expect("Failed to get address queue elements before batch update");

    assert_eq!(
        queue_elements_before.addresses.len(),
        50,
        "Address queue length mismatch before batch update"
    );

    for (i, element) in queue_elements_before.addresses.iter().take(3).enumerate() {
        assert_eq!(
            element.address.0.to_bytes(),
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
        .get_batch_address_update_info(GetBatchAddressUpdateInfoRequest {
            tree: address_tree_pubkey.to_bytes().into(),
            batch_size: 100,
        })
        .await
        .expect("Failed to get address queue elements after batch update");

    assert!(
        queue_elements_after.addresses.is_empty(),
        "Address queue should be empty after batch update, but found {} elements",
        queue_elements_after.addresses.len()
    );
    println!("Address queue state verified after batch update (empty).");

    // --- Phase 3: Verify Final Tree State and Proofs ---
    println!("Verifying final tree state...");
    for (hash, leaf_index) in &expected_addresses {
        reference_address_tree
            .update(&hash, *leaf_index as usize)
            .expect("Failed to update reference tree");
    }
    let final_reference_root = reference_address_tree.root();
    println!(
        "Final Reference Merkle Tree Root: {:?}",
        final_reference_root
    );

    println!("Final tree state and proofs verified.");
}

/// Reset table
/// Index transactions individually or in one batch
pub async fn index(
    test_name: &str,
    db_conn: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    txns: &[String],
    index_transactions_individually: bool,
) {
    let txs_permutations = txns
        .iter()
        .map(|x| vec![x.to_string()])
        .collect::<Vec<Vec<_>>>();

    for index_transactions_individually in [index_transactions_individually] {
        for (i, txs) in txs_permutations.clone().iter().enumerate() {
            println!(
                "indexing tx {} {}/{}",
                index_transactions_individually,
                i + 1,
                txs_permutations.len()
            );
            println!("tx {:?}", txs);

            // HACK: We index a block so that API methods can fetch the current slot.
            index_block(
                db_conn.as_ref(),
                &BlockInfo {
                    metadata: BlockMetadata {
                        slot: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            if index_transactions_individually {
                for tx in txs {
                    index_transaction(test_name, db_conn.clone(), rpc_client.clone(), tx).await;
                }
            } else {
                index_multiple_transactions(
                    test_name,
                    db_conn.clone(),
                    rpc_client.clone(),
                    txs.iter().map(|x| x.as_str()).collect(),
                )
                .await;
            }
        }
    }
}

/// Reads file names from tests/data/transactions/<name>
/// returns vector of file names sorted by slot
fn read_file_names(name: &String, sort_by_slot: bool) -> Vec<String> {
    let signatures = std::fs::read_dir(format!("tests/data/transactions/{}", name))
        .unwrap()
        .filter_map(|entry| {
            entry
                .ok()
                .and_then(|e| e.file_name().to_str().map(|s| s.to_string()))
        })
        .collect::<Vec<String>>();
    if sort_by_slot {
        let mut sorted_files: Vec<(String, u64)> = Vec::new();
        for filename in signatures {
            let json_str =
                std::fs::read_to_string(format!("tests/data/transactions/{}/{}", name, filename))
                    .unwrap();
            let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();
            let slot = json["slot"].as_u64().unwrap_or(0);
            sorted_files.push((filename, slot));
        }
        sorted_files.sort_by_key(|k| k.1);
        sorted_files.into_iter().map(|(name, _)| name).collect()
    } else {
        signatures
    }
}
