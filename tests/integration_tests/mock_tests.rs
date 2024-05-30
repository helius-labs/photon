use crate::utils::*;
use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use function_name::named;
use photon_indexer::api::error::PhotonApiError;
use photon_indexer::api::method::get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest;
use photon_indexer::api::method::get_compressed_balance_by_owner::GetCompressedBalanceByOwnerRequest;
use photon_indexer::api::method::get_compressed_token_balances_by_owner::GetCompressedTokenBalancesByOwnerRequest;
use photon_indexer::api::method::get_multiple_compressed_accounts::GetMultipleCompressedAccountsRequest;
use photon_indexer::api::method::utils::{
    CompressedAccountRequest, GetCompressedTokenAccountsByDelegate,
    GetCompressedTokenAccountsByOwner,
};
use photon_indexer::common::typedefs::unsigned_integer::UnsignedInteger;
use photon_indexer::ingester::persist::persisted_state_tree::{
    get_multiple_compressed_leaf_proofs, ZERO_BYTES,
};
use sea_orm::TransactionTrait;

use photon_indexer::common::typedefs::account::Account;
use photon_indexer::common::typedefs::bs64_string::Base64String;
use photon_indexer::common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};
use photon_indexer::dao::generated::accounts;
use photon_indexer::ingester::index_block;
use photon_indexer::ingester::parser::state_update::StateUpdate;
use photon_indexer::ingester::persist::persisted_state_tree::{persist_leaf_nodes, LeafNode};
use photon_indexer::ingester::persist::{
    compute_parent_hash, persist_token_accounts, EnrichedTokenAccount,
};
use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use sea_orm::{EntityTrait, Set};
use serial_test::serial;

use photon_indexer::common::typedefs::account::AccountData;
use std::collections::{HashMap, HashSet};

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
            discriminator: UnsignedInteger(1),
            data: Base64String(vec![1; 500]),
            data_hash: Hash::new_unique(),
        }),
        owner: SerializablePubkey::new_unique(),
        lamports: UnsignedInteger(1000),
        tree: SerializablePubkey::new_unique(),
        leaf_index: UnsignedInteger(0),
        seq: UnsignedInteger(0),
        slot_created: UnsignedInteger(0),
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

    assert_eq!(res, account.lamports);

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
                discriminator: UnsignedInteger(0),
                data: Base64String(vec![1; 500]),
                data_hash: Hash::new_unique(),
            }),
            owner: owner1,
            lamports: UnsignedInteger(1000),
            tree: SerializablePubkey::new_unique(),
            leaf_index: UnsignedInteger(10),
            seq: UnsignedInteger(1),
            slot_created: UnsignedInteger(0),
        },
        Account {
            hash: Hash::new_unique(),
            address: None,
            data: Some(AccountData {
                discriminator: UnsignedInteger(1),
                data: Base64String(vec![2; 500]),
                data_hash: Hash::new_unique(),
            }),
            owner: owner1,
            lamports: UnsignedInteger(1030),
            tree: SerializablePubkey::new_unique(),
            leaf_index: UnsignedInteger(11),
            seq: UnsignedInteger(2),
            slot_created: UnsignedInteger(0),
        },
        Account {
            hash: Hash::new_unique(),
            address: Some(SerializablePubkey::new_unique()),
            data: Some(AccountData {
                discriminator: UnsignedInteger(4),
                data: Base64String(vec![4; 500]),
                data_hash: Hash::new_unique(),
            }),
            owner: owner2,
            lamports: UnsignedInteger(10020),
            tree: SerializablePubkey::new_unique(),
            leaf_index: UnsignedInteger(13),
            seq: UnsignedInteger(3),
            slot_created: UnsignedInteger(1),
        },
        Account {
            hash: Hash::new_unique(),
            address: Some(SerializablePubkey::new_unique()),
            data: Some(AccountData {
                discriminator: UnsignedInteger(10),
                data: Base64String(vec![5; 500]),
                data_hash: Hash::new_unique(),
            }),
            owner: owner2,
            lamports: UnsignedInteger(10100),
            tree: SerializablePubkey::new_unique(),
            leaf_index: UnsignedInteger(23),
            seq: UnsignedInteger(1),
            slot_created: UnsignedInteger(0),
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
            .fold(0, |acc, x| acc + x.lamports.0);

        let res = setup
            .api
            .get_compressed_balance_by_owner(GetCompressedBalanceByOwnerRequest {
                owner: SerializablePubkey::from(owner),
            })
            .await
            .unwrap()
            .value;

        assert_eq!(res.0, total_balance);
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
        amount: UnsignedInteger(1),
        delegate: Some(delegate1),
        state: AccountState::frozen,
        is_native: Some(UnsignedInteger(1)),
        delegated_amount: UnsignedInteger(1),
    };

    let token_data2 = TokenData {
        mint: mint2,
        owner: owner1,
        amount: UnsignedInteger(2),
        delegate: Some(delegate2),
        state: AccountState::initialized,
        is_native: None,
        delegated_amount: UnsignedInteger(2),
    };

    let token_data3 = TokenData {
        mint: mint3,
        owner: owner2,
        amount: UnsignedInteger(3),
        delegate: Some(delegate1),
        state: AccountState::frozen,
        is_native: Some(UnsignedInteger(1000)),
        delegated_amount: UnsignedInteger(3),
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
            slot_created: Set(slot),
            leaf_index: Set(i as i64),
            discriminator: Set(Some(Decimal::from(1))),
            data_hash: Set(Some(Hash::new_unique().to_vec())),
            tree: Set(Pubkey::new_unique().to_bytes().to_vec()),
            seq: Set(0),
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
            *balance += token_account.token_data.amount.0;
        }
        for (mint, balance) in mint_to_balance.iter() {
            let request = GetCompressedTokenBalancesByOwnerRequest {
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
            assert_eq!(res.token_balances[0].balance.0, *balance);
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

            assert_eq!(balance.amount, token_account.token_data.amount);
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

#[tokio::test]
async fn test_compute_parent_hash() {
    let child = ZERO_BYTES[0];
    let parent = ZERO_BYTES[1];
    let computed_parent = compute_parent_hash(child.to_vec(), child.to_vec()).unwrap();
    assert_eq!(computed_parent, parent.to_vec());
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_persisted_state_trees(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;
    let tree = SerializablePubkey::new_unique();
    let num_nodes = 1;

    let leaf_nodes: Vec<LeafNode> = (0..num_nodes)
        .map(|i| LeafNode {
            hash: Hash::new_unique(),
            leaf_index: i,
            tree: tree.clone(),
            seq: i,
        })
        .collect();
    let txn = setup.db_conn.as_ref().begin().await.unwrap();
    let tree_height = 2;
    persist_leaf_nodes(&txn, leaf_nodes.clone(), tree_height)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let proofs = get_multiple_compressed_leaf_proofs(
        &setup.db_conn,
        leaf_nodes
            .iter()
            .map(|x| Hash::try_from(x.hash.clone()).unwrap())
            .collect(),
    )
    .await
    .unwrap();

    let proof_hashes: HashSet<Hash> = proofs.iter().map(|x| x.hash.clone()).collect();
    let leaf_hashes: HashSet<Hash> = leaf_nodes.iter().map(|x| x.hash.clone()).collect();
    assert_eq!(proof_hashes, leaf_hashes);

    for proof in proofs {
        assert_eq!(proof.merkle_tree, tree);
        assert_eq!(num_nodes as u64 - 1, proof.root_seq);
        assert_eq!(tree_height - 1, proof.proof.len() as u32);
        let proof_path: Vec<Hash> = vec![proof.hash.clone()]
            .into_iter()
            .chain(proof.proof.clone())
            .collect();
        // Use the reduce function to reduce proof path using compute_parent_hash
        let computed_root = proof_path
            .into_iter()
            .reduce(|acc, x| {
                Hash::try_from(compute_parent_hash(acc.to_vec(), x.to_vec()).unwrap()).unwrap()
            })
            .unwrap();
        assert_eq!(computed_root, proof.root);
    }
}
