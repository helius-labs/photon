use crate::utils::*;
use borsh::BorshSerialize;
use function_name::named;
use light_compressed_account::QueueType;
use light_hasher::zero_bytes::poseidon::ZERO_BYTES;
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
use photon_indexer::ingester::persist::COMPRESSED_TOKEN_PROGRAM;
use serial_test::serial;
use solana_pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use std::str::FromStr;

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

    let mut collected_target_owners: Vec<Pubkey> = Vec::new();
    const TARGET_OWNER_COUNT: usize = 51;
    const TARGET_ACCOUNT_LAMPORTS: u64 = 1_000_000;

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

        println!("signature {}", signature);
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

        for opened_account_info in accounts.openedAccounts.iter() {
            let account_data = &opened_account_info.account;
            if account_data.lamports.0 == TARGET_ACCOUNT_LAMPORTS {
                let owner_pk = account_data.owner.0;
                if collected_target_owners.len() < TARGET_OWNER_COUNT
                    && !collected_target_owners.contains(&owner_pk)
                {
                    collected_target_owners.push(owner_pk);
                    println!(
                        "Collected target owner ({} of {}): {}",
                        collected_target_owners.len(),
                        TARGET_OWNER_COUNT,
                        owner_pk
                    );
                }
            }
        }

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

        println!("accounts {:?}", accounts);

        // Get output queue elements
        if !accounts.openedAccounts.is_empty() {
            output_queue_len += accounts.openedAccounts.len();
            merkle_tree_pubkey = accounts.openedAccounts[0].account.merkle_context.tree.0;
            queue_pubkey = accounts.openedAccounts[0].account.merkle_context.queue.0;
            let get_queue_elements_result = setup
                .api
                .get_queue_elements(GetQueueElementsRequest {
                    tree: merkle_tree_pubkey.to_bytes().into(),
                    start_queue_index: None,
                    queue_type: QueueType::OutputStateV2 as u8,
                    limit: 100,
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
                    start_queue_index: None,
                    queue_type: QueueType::InputStateV2 as u8,
                    limit: 100,
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
                new_addresses_with_trees: vec![],
            })
            .await
            .unwrap();

        // No value has been inserted into the tree yet -> all proof by index.
        assert!(validity_proof
            .value
            .accounts
            .iter()
            .all(|x| x.root_index.prove_by_index));
        assert!(validity_proof
            .value
            .accounts
            .iter()
            .all(|x| x.merkle_context.tree.to_string() == merkle_tree_pubkey.to_string()));
        assert!(validity_proof
            .value
            .accounts
            .iter()
            .all(|x| x.merkle_context.queue.to_string() == queue_pubkey.to_string()));
        assert!(validity_proof
            .value
            .accounts
            .iter()
            .all(|x| x.root.is_empty()));
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
                start_queue_index: None,
                queue_type: QueueType::OutputStateV2 as u8,
                limit: 100,
            })
            .await
            .unwrap();
        let pre_input_queue_elements = setup
            .api
            .get_queue_elements(GetQueueElementsRequest {
                tree: merkle_tree_pubkey.to_bytes().into(),
                start_queue_index: None,
                queue_type: QueueType::InputStateV2 as u8,
                limit: 100,
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
                start_queue_index: None,
                queue_type: QueueType::OutputStateV2 as u8,
                limit: 100,
            })
            .await
            .unwrap();
        let post_input_queue_elements = setup
            .api
            .get_queue_elements(GetQueueElementsRequest {
                tree: merkle_tree_pubkey.to_bytes().into(),
                start_queue_index: None,
                queue_type: QueueType::InputStateV2 as u8,
                limit: 100,
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
                    new_addresses_with_trees: vec![],
                })
                .await
                .unwrap();
            println!("j {}, validity_proof {:?}", j, validity_proof.value);

            // No value has been inserted into the tree yet -> all proof by index.
            let mut base_index = j * 8;
            for (z, account_proof) in validity_proof.value.accounts.iter().enumerate() {
                println!("z + base index {} {}", z, base_index);
                println!("last inserted index {}", last_inserted_index);
                if base_index < last_inserted_index {
                    assert!(!account_proof.root_index.prove_by_index);
                } else {
                    assert!(account_proof.root_index.prove_by_index);
                    assert_eq!(account_proof.root, "");
                }
                base_index += 2;
            }
            assert!(validity_proof.value.accounts.iter().all(|x| x
                .merkle_context
                .tree
                .to_string()
                == merkle_tree_pubkey.to_string()));
            assert!(validity_proof.value.accounts.iter().all(|x| x
                .merkle_context
                .queue
                .to_string()
                == queue_pubkey.to_string()));
        }
    }
    assert_eq!(event_merkle_tree.root(), merkle_tree.root());
    assert_eq!(output_queue_len, 100);
    assert_eq!(input_queue_len, 50);
    let get_queue_elements_result = setup
        .api
        .get_queue_elements(GetQueueElementsRequest {
            tree: merkle_tree_pubkey.to_bytes().into(),
            start_queue_index: None,
            queue_type: QueueType::OutputStateV2 as u8,
            limit: 100,
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
            start_queue_index: None,
            queue_type: QueueType::InputStateV2 as u8,
            limit: 100,
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

    assert_eq!(
        collected_target_owners.len(),
        TARGET_OWNER_COUNT,
        "Expected to collect {} target owners from initial transactions, but found {}. Check test data and collection logic.",
        TARGET_OWNER_COUNT,
        collected_target_owners.len()
    );
    let mut expected_owners = collected_target_owners;
    expected_owners.remove(0); // closed account owner

    for owner in expected_owners.iter().enumerate() {
        println!("{} {}", owner.0, owner.1);
    }
    // Slot created is wrong likely because of the test environment.
    let mut leaf_index = 1;
    for (i, current_owner_pk) in expected_owners.iter().enumerate() {
        let owner = *current_owner_pk;

        println!(
            "Verifying owner (iteration {} from collected list): {}",
            i, owner
        );

        println!("Querying for owner: {}", owner);
        let accounts = setup
            .api
            .get_compressed_accounts_by_owner_v2(GetCompressedAccountsByOwnerRequest {
                owner: SerializablePubkey::from(owner.to_bytes()),
                ..Default::default()
            })
            .await
            .unwrap();

        for acc in accounts.value.items.iter() {
            println!("acc owner {}", acc.owner);
        }
        assert_eq!(
            accounts.value.items.len(),
            1,
            "Expected 1 account for owner {}, but found {}. Iteration index: {}.",
            owner,
            accounts.value.items.len(),
            i
        );

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
            Pubkey::from_str("D2J2AZChFBGxn4gYeE3gQsR85u3dWBL3foGobGeGxQfJ").unwrap(),
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
