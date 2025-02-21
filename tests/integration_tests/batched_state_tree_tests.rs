use crate::utils::*;
use borsh::BorshSerialize;
use function_name::named;
use light_merkle_tree_reference;
use photon_indexer::api::method::get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest;
use photon_indexer::api::method::get_compressed_token_balances_by_owner::{
    GetCompressedTokenBalancesByOwnerRequest, TokenBalance,
};
use photon_indexer::api::method::get_multiple_compressed_account_proofs::HashList;
use photon_indexer::api::method::get_transaction_with_compression_info::{
    get_transaction_helper, get_transaction_helper_v2,
};
use photon_indexer::api::method::utils::GetCompressedTokenAccountsByOwner;
use photon_indexer::common::typedefs::serializable_pubkey::SerializablePubkey;
use photon_indexer::common::typedefs::serializable_signature::SerializableSignature;
use photon_indexer::common::typedefs::token_data::TokenData;
use photon_indexer::common::typedefs::unsigned_integer::UnsignedInteger;
use photon_indexer::ingester::index_block;
use photon_indexer::ingester::persist::COMPRESSED_TOKEN_PROGRAM;
use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use sea_orm::DatabaseConnection;
use serial_test::serial;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Signature;

use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use std::str::FromStr;
use std::sync::Arc;

/// Test:
/// 1. get compressed account by owner
/// 2. get compressed account proofs
/// 3. correct root update after batch append and batch nullify events
#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_batched_tree_transactions(
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
        reset_tables(setup.db_conn.as_ref()).await.unwrap();
        let sort_by_slot = true;
        let signatures = read_file_names(&name, sort_by_slot);

        // build tree
        let mut merkle_tree =
            light_merkle_tree_reference::MerkleTree::<light_hasher::Poseidon>::new(32, 0);
        for signature in signatures.iter() {
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
                merkle_tree
                    .update(
                        &account.account.nullifier.0,
                        account.account.account.leaf_index.0 as usize,
                    )
                    .unwrap();
            }
            for account in accounts.openedAccounts.iter() {
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
        }

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
}

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
            Pubkey::from_str("4HV5oEidH1QGY55kNTHb1yqjcHmKyT7gTgNSCL8TiWe9").unwrap(),
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
                    index_transaction(test_name, db_conn.clone(), rpc_client.clone(), &tx).await;
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
