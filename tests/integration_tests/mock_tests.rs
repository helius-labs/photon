use crate::utils::*;
use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use function_name::named;
use photon_indexer::api::error::PhotonApiError;
use photon_indexer::api::method::get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest;
use photon_indexer::api::method::get_compressed_balance_by_owner::GetCompressedBalanceByOwnerRequest;
use photon_indexer::api::method::get_compressed_token_balances_by_owner::GetCompressedTokenBalancesByOwner;
use photon_indexer::api::method::get_multiple_compressed_accounts::GetMultipleCompressedAccountsRequest;
use photon_indexer::api::method::utils::{
    CompressedAccountRequest, GetCompressedTokenAccountsByDelegate,
    GetCompressedTokenAccountsByOwner,
};
use photon_indexer::common::typedefs::account::Account;
use photon_indexer::common::typedefs::bs64_string::Base64String;
use photon_indexer::common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};
use photon_indexer::dao::generated::accounts;
use photon_indexer::ingester::index_block;
use photon_indexer::ingester::parser::state_update::StateUpdate;
use photon_indexer::ingester::persist::{
    persist_state_update, persist_token_accounts, EnrichedTokenAccount,
};
use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use sea_orm::{EntityTrait, Set};
use serial_test::serial;

use photon_indexer::{
    common::typedefs::account::AccountData,
    ingester::parser::{indexer_events::PathNode, state_update::EnrichedPathNode},
};
use std::collections::HashMap;

use photon_indexer::common::typedefs::token_data::{AccountState, TokenData};
use sqlx::types::Decimal;

use photon_indexer::api::method::utils::Limit;
use solana_sdk::pubkey::Pubkey;
use std::vec;

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone)]
struct Person {
    name: String,
    age: u64,
}

// TODO:
// - Add tests for duplicate inserts.
// - Add tests for accounts input spends without existing accounts.
// - Add test for multi-input/output transitions.

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_persist_state_update_basic(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;

    // HACK: We index a block so that API methods can fetch the current slot.
    index_block(
        &setup.db_conn,
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

    let mut state_update = StateUpdate::new();
    let account = Account {
        hash: Hash::new_unique(),
        address: Some(SerializablePubkey::new_unique()),
        data: Some(AccountData {
            discriminator: 1,
            data: Base64String(vec![1; 500]),
            data_hash: Hash::new_unique(),
        }),
        owner: SerializablePubkey::new_unique(),
        lamports: 1000,
        tree: SerializablePubkey::new_unique(),
        leaf_index: 0,
        seq: Some(0),
        slot_updated: 0,
    };

    state_update.out_accounts.push(account.clone());
    persist_state_update_using_connection(&setup.db_conn, state_update)
        .await
        .unwrap();

    let request = CompressedAccountRequest {
        address: None,
        hash: Some(Hash::from(account.hash.clone())),
    };

    let res = setup
        .api
        .get_compressed_account(request.clone())
        .await
        .unwrap()
        .value;

    assert_eq!(res, account);

    let res = setup
        .api
        .get_compressed_balance(request)
        .await
        .unwrap()
        .value;

    assert_eq!(res, account.lamports as u64);

    // Assert that we get an error if we input a non-existent account.
    // TODO: Test spent accounts
    let err = setup
        .api
        .get_compressed_account(CompressedAccountRequest {
            hash: Some(Hash::from(Pubkey::new_unique().to_bytes())),
            address: None,
        })
        .await
        .unwrap_err();

    match err {
        PhotonApiError::RecordNotFound(_) => {}
        _ => panic!("Expected NotFound error"),
    }
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_multiple_accounts(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;

    // HACK: We index a block so that API methods can fetch the current slot.
    index_block(
        &setup.db_conn,
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

    let owner1 = SerializablePubkey::new_unique();
    let owner2 = SerializablePubkey::new_unique();
    let mut state_update = StateUpdate::default();

    let accounts = vec![
        Account {
            hash: Hash::new_unique(),
            address: Some(SerializablePubkey::new_unique()),
            data: Some(AccountData {
                discriminator: 0,
                data: Base64String(vec![1; 500]),
                data_hash: Hash::new_unique(),
            }),
            owner: owner1,
            lamports: 1000,
            tree: SerializablePubkey::new_unique(),
            leaf_index: 10,
            seq: Some(1),
            slot_updated: 0,
        },
        Account {
            hash: Hash::new_unique(),
            address: None,
            data: Some(AccountData {
                discriminator: 1,
                data: Base64String(vec![2; 500]),
                data_hash: Hash::new_unique(),
            }),
            owner: owner1,
            lamports: 1030,
            tree: SerializablePubkey::new_unique(),
            leaf_index: 11,
            seq: Some(2),
            slot_updated: 0,
        },
        Account {
            hash: Hash::new_unique(),
            address: Some(SerializablePubkey::new_unique()),
            data: Some(AccountData {
                discriminator: 4,
                data: Base64String(vec![4; 500]),
                data_hash: Hash::new_unique(),
            }),
            owner: owner2,
            lamports: 10020,
            tree: SerializablePubkey::new_unique(),
            leaf_index: 13,
            seq: Some(3),
            slot_updated: 1,
        },
        Account {
            hash: Hash::new_unique(),
            address: Some(SerializablePubkey::new_unique()),
            data: Some(AccountData {
                discriminator: 10,
                data: Base64String(vec![5; 500]),
                data_hash: Hash::new_unique(),
            }),
            owner: owner2,
            lamports: 10100,
            tree: SerializablePubkey::new_unique(),
            leaf_index: 23,
            seq: Some(1),
            slot_updated: 0,
        },
    ];
    state_update.out_accounts = accounts.clone();
    persist_state_update_using_connection(&setup.db_conn, state_update)
        .await
        .unwrap();

    for owner in [owner1, owner2] {
        let res = setup
            .api
            .get_compressed_accounts_by_owner(GetCompressedAccountsByOwnerRequest {
                owner: SerializablePubkey::from(owner),
                ..Default::default()
            })
            .await
            .unwrap()
            .value;

        let mut response_accounts = res.items;

        let mut paginated_response_accounts = Vec::new();
        let mut cursor = None;
        loop {
            let res = setup
                .api
                .get_compressed_accounts_by_owner(GetCompressedAccountsByOwnerRequest {
                    owner: SerializablePubkey::from(owner),
                    cursor: cursor.clone(),
                    limit: Some(Limit::new(1).unwrap()),
                })
                .await
                .unwrap()
                .value;

            paginated_response_accounts.extend(res.items.clone());
            cursor = res.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(response_accounts, paginated_response_accounts);

        let mut accounts_of_interest = accounts
            .clone()
            .into_iter()
            .filter(|x| x.owner == owner)
            .collect::<Vec<Account>>();

        assert_account_response_list_matches_input(
            &mut response_accounts,
            &mut accounts_of_interest,
        );

        let total_balance = accounts_of_interest
            .iter()
            .fold(0, |acc, x| acc + x.lamports);

        let res = setup
            .api
            .get_compressed_balance_by_owner(GetCompressedBalanceByOwnerRequest {
                owner: SerializablePubkey::from(owner),
            })
            .await
            .unwrap()
            .value;

        assert_eq!(res, total_balance);
    }

    let mut accounts_of_interest = vec![accounts[0].clone(), accounts[2].clone()];
    let mut res = setup
        .api
        .get_multiple_compressed_accounts(GetMultipleCompressedAccountsRequest {
            addresses: None,
            hashes: Some(
                accounts_of_interest
                    .iter()
                    .map(|x| x.hash.clone())
                    .collect(),
            ),
        })
        .await
        .unwrap()
        .value;

    assert_account_response_list_matches_input(&mut res.items, &mut accounts_of_interest);
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_persist_token_data(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;
    let mint1 = SerializablePubkey::new_unique();
    let mint2 = SerializablePubkey::new_unique();
    let mint3 = SerializablePubkey::new_unique();
    let owner1 = SerializablePubkey::new_unique();
    let owner2 = SerializablePubkey::new_unique();
    let delegate1 = SerializablePubkey::new_unique();
    let delegate2 = SerializablePubkey::new_unique();

    // HACK: We index a block so that API methods can fetch the current slot.
    index_block(
        &setup.db_conn,
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

    let token_data1 = TokenData {
        mint: mint1,
        owner: owner1,
        amount: 1,
        delegate: Some(delegate1),
        state: AccountState::frozen,
        is_native: Some(1),
        delegated_amount: 1,
    };

    let token_data2 = TokenData {
        mint: mint2,
        owner: owner1,
        amount: 2,
        delegate: Some(delegate2),
        state: AccountState::initialized,
        is_native: None,
        delegated_amount: 2,
    };

    let token_data3 = TokenData {
        mint: mint3,
        owner: owner2,
        amount: 3,
        delegate: Some(delegate1),
        state: AccountState::frozen,
        is_native: Some(1000),
        delegated_amount: 3,
    };
    let all_token_data = vec![token_data1, token_data2, token_data3];

    let txn = sea_orm::TransactionTrait::begin(setup.db_conn.as_ref())
        .await
        .unwrap();

    let mut token_datas = Vec::new();

    for (i, token_data) in all_token_data.iter().enumerate() {
        let slot = 11;
        let hash = Hash::new_unique();
        let model = accounts::ActiveModel {
            hash: Set(hash.clone().into()),
            address: Set(Some(Pubkey::new_unique().to_bytes().to_vec())),
            spent: Set(false),
            data: Set(Some(to_vec(&token_data).unwrap())),
            owner: Set(token_data.owner.to_bytes_vec()),
            lamports: Set(Decimal::from(10)),
            slot_updated: Set(slot),
            leaf_index: Set(i as i64),
            discriminator: Set(Some(Decimal::from(1))),
            data_hash: Set(Some(Hash::new_unique().to_vec())),
            tree: Set(Pubkey::new_unique().to_bytes().to_vec()),
            ..Default::default()
        };
        accounts::Entity::insert(model).exec(&txn).await.unwrap();
        token_datas.push(EnrichedTokenAccount {
            hash,
            token_data: token_data.clone(),
        });
    }

    persist_token_accounts(&txn, token_datas).await.unwrap();
    txn.commit().await.unwrap();

    let owner_tlv = all_token_data
        .iter()
        .filter(|x| x.owner == owner1 && x.mint == mint1)
        .map(Clone::clone)
        .collect();

    let res = setup
        .api
        .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwner {
            owner: SerializablePubkey::from(owner1),
            mint: Some(SerializablePubkey::from(mint1)),
            ..Default::default()
        })
        .await
        .unwrap()
        .value;
    verify_responses_match_tlv_data(res.clone(), owner_tlv);

    for owner in [owner1, owner2] {
        let owner_tlv = all_token_data
            .iter()
            .filter(|x| x.owner == owner)
            .map(Clone::clone)
            .collect();
        let res = setup
            .api
            .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwner {
                owner: SerializablePubkey::from(owner),
                ..Default::default()
            })
            .await
            .unwrap()
            .value;

        let mut paginated_res = Vec::new();
        let mut cursor = None;
        loop {
            let res = setup
                .api
                .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwner {
                    owner: SerializablePubkey::from(owner),
                    cursor: cursor.clone(),
                    limit: Some(photon_indexer::api::method::utils::Limit::new(1).unwrap()),
                    ..Default::default()
                })
                .await
                .unwrap()
                .value;

            paginated_res.extend(res.items.clone());
            cursor = res.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(paginated_res, res.items);

        let mut mint_to_balance: HashMap<SerializablePubkey, u64> = HashMap::new();

        for token_account in paginated_res.iter() {
            let balance = mint_to_balance
                .entry(token_account.token_data.mint.clone())
                .or_insert(0);
            *balance += token_account.token_data.amount;
        }
        for (mint, balance) in mint_to_balance.iter() {
            let request = GetCompressedTokenBalancesByOwner {
                owner: SerializablePubkey::from(owner),
                mint: Some(mint.clone()),
                ..Default::default()
            };
            let res = setup
                .api
                .get_compressed_token_balances_by_owner(request)
                .await
                .unwrap()
                .value;
            assert_eq!(res.token_balances[0].balance, *balance);
        }

        verify_responses_match_tlv_data(res.clone(), owner_tlv);
        for token_account in res.items {
            let request = CompressedAccountRequest {
                address: None,
                hash: Some(token_account.account.hash),
            };
            let balance = setup
                .api
                .get_compressed_token_account_balance(request)
                .await
                .unwrap()
                .value;
            assert_eq!(
                balance.amount,
                Into::<u64>::into(token_account.token_data.amount)
            );
        }
    }
    for delegate in [delegate1, delegate2] {
        let delegate_tlv = all_token_data
            .clone()
            .into_iter()
            .filter(|x| x.delegate == Some(delegate))
            .collect();
        let res = setup
            .api
            .get_compressed_token_accounts_by_delegate(GetCompressedTokenAccountsByDelegate {
                delegate: SerializablePubkey::from(delegate),
                ..Default::default()
            })
            .await
            .unwrap()
            .value;
        let mut paginated_res = Vec::new();
        let mut cursor = None;
        loop {
            let res = setup
                .api
                .get_compressed_token_accounts_by_delegate(GetCompressedTokenAccountsByDelegate {
                    delegate: SerializablePubkey::from(delegate),
                    cursor: cursor.clone(),
                    limit: Some(photon_indexer::api::method::utils::Limit::new(1).unwrap()),
                    ..Default::default()
                })
                .await
                .unwrap()
                .value;

            paginated_res.extend(res.items.clone());
            cursor = res.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(paginated_res, res.items);
        verify_responses_match_tlv_data(res, delegate_tlv)
    }
}

#[named]
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 25)]
#[serial]
#[ignore]
/// Test for testing how fast we can index accounts.
async fn test_load_test(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use std::collections::HashSet;

    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;

    fn generate_random_account(_tree: Pubkey, _seq: i64) -> Account {
        Account {
            hash: Hash::new_unique(),
            address: Some(SerializablePubkey::new_unique()),
            data: Some(AccountData {
                discriminator: 10,
                data: Base64String(vec![5; 500]),
                data_hash: Hash::new_unique(),
            }),
            owner: SerializablePubkey::new_unique(),
            lamports: 10100,
            tree: SerializablePubkey::new_unique(),
            leaf_index: 23,
            seq: Some(1),
            slot_updated: 0,
        }
    }

    fn generate_random_leaf_index(tree: Pubkey, node_index: u32, seq: i64) -> EnrichedPathNode {
        EnrichedPathNode {
            node: PathNode {
                node: Pubkey::new_unique().to_bytes(),
                index: node_index,
            },
            slot: 0,
            tree: tree.to_bytes(),
            seq: seq as u64,
            level: 0,
            leaf_index: Some(seq as u32),
            tree_depth: 20,
        }
    }

    let loops = 25;
    for _ in 0..loops {
        let tree: Pubkey = Pubkey::new_unique();
        let txn = sea_orm::TransactionTrait::begin(setup.db_conn.as_ref())
            .await
            .unwrap();
        let num_elements = 2000;
        let state_update = StateUpdate {
            in_accounts: vec![],
            out_accounts: (0..num_elements)
                .map(|i| generate_random_account(tree, i))
                .collect(),
            // We only include the leaf index because we think the most path nodes will be
            // overwritten anyways. So the amortized number of writes will be in each tree
            // will be close to 1.
            path_nodes: (0..num_elements)
                .map(|i| {
                    let leaf = generate_random_leaf_index(tree, i as u32, i);
                    let key = (leaf.tree.clone(), leaf.leaf_index.unwrap());
                    (key, leaf)
                })
                .collect(),
            account_transactions: HashSet::new(),
        };
        persist_state_update(&txn, state_update).await.unwrap();
        txn.commit().await.unwrap();
    }
}
