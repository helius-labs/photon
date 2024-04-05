use crate::utils::*;
use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use function_name::named;
use photon_indexer::api::error::PhotonApiError;
use photon_indexer::api::method::get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest;
use photon_indexer::api::method::get_multiple_compressed_accounts::GetMultipleCompressedAccountsRequest;
use photon_indexer::api::method::utils::{
    CompressedAccountRequest, GetCompressedTokenAccountsByAuthority,
    GetCompressedTokenAccountsByAuthorityOptions,
};
use photon_indexer::common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};
use photon_indexer::dao::generated::accounts;
use photon_indexer::ingester::index_block;
use photon_indexer::ingester::parser::indexer_events::TokenData;
use photon_indexer::ingester::parser::state_update::{EnrichedAccount, StateUpdate};
use photon_indexer::ingester::persist::{
    persist_state_update, persist_token_accounts, EnrichedTokenAccount,
};
use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use sea_orm::{EntityTrait, Set};
use serial_test::serial;

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
    use photon_indexer::ingester::parser::indexer_events::{
        CompressedAccount, CompressedAccountData,
    };

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
    let account = EnrichedAccount {
        account: CompressedAccount {
            data: Some(CompressedAccountData {
                discriminator: [1; 8],
                data: vec![1; 500],
                data_hash: [1; 32],
            }),
            address: Some(Pubkey::new_unique().to_bytes()),
            lamports: 1000,
            owner: Pubkey::new_unique(),
        },
        tree: Pubkey::new_unique(),
        seq: Some(0),
        hash: [0; 32],
        slot: 0,
    };
    state_update.out_accounts.push(account.clone());
    persist_state_update_using_connection(&setup.db_conn, state_update)
        .await
        .unwrap();

    let request = CompressedAccountRequest {
        address: None,
        hash: Some(Hash::from(account.hash)),
    };

    let res = setup
        .api
        .get_compressed_account(request.clone())
        .await
        .unwrap()
        .value;

    #[allow(deprecated)]
    let raw_data = base64::decode(res.data).unwrap();
    assert_eq!(account.account.data.unwrap().data, raw_data);
    assert_eq!(res.lamports, account.account.lamports);
    assert_eq!(res.slot_updated, account.slot);
    assert_eq!(
        res.address,
        account.account.address.map(SerializablePubkey::from)
    );
    assert_eq!(res.owner, SerializablePubkey::from(account.account.owner));

    let res = setup
        .api
        .get_compressed_balance(request)
        .await
        .unwrap()
        .value;

    assert_eq!(res, account.account.lamports as u64);

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
    use photon_indexer::{
        api::method::utils::Limit,
        ingester::parser::indexer_events::{CompressedAccount, CompressedAccountData},
    };

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

    let owner1 = Pubkey::new_unique();
    let owner2 = Pubkey::new_unique();
    let mut state_update = StateUpdate::default();

    let accounts = vec![
        EnrichedAccount {
            account: CompressedAccount {
                data: Some(CompressedAccountData {
                    discriminator: [0; 8],
                    data: vec![1; 500],
                    data_hash: [1; 32],
                }),
                address: Some(Pubkey::new_unique().to_bytes()),
                lamports: 1000,
                owner: owner1,
            },
            tree: Pubkey::new_unique(),
            slot: 0,
            seq: Some(1),
            hash: [0; 32],
        },
        EnrichedAccount {
            account: CompressedAccount {
                data: Some(CompressedAccountData {
                    discriminator: [1; 8],
                    data: vec![2; 500],
                    data_hash: [1; 32],
                }),
                address: None,
                lamports: 1030,
                owner: owner1,
            },
            tree: Pubkey::new_unique(),
            slot: 0,
            seq: Some(2),
            hash: [0; 32],
        },
        EnrichedAccount {
            account: CompressedAccount {
                data: Some(CompressedAccountData {
                    discriminator: [4; 8],
                    data: vec![4; 500],
                    data_hash: [3; 32],
                }),
                address: Some(Pubkey::new_unique().to_bytes()),
                lamports: 10020,
                owner: owner2,
            },
            tree: Pubkey::new_unique(),
            slot: 1,
            seq: Some(3),
            hash: [0; 32],
        },
        EnrichedAccount {
            account: CompressedAccount {
                data: Some(CompressedAccountData {
                    discriminator: [10; 8],
                    data: vec![5; 500],
                    data_hash: [6; 32],
                }),
                address: Some(Pubkey::new_unique().to_bytes()),
                lamports: 10100,
                owner: owner2,
            },
            tree: Pubkey::new_unique(),
            slot: 0,
            seq: Some(1),
            hash: [0; 32],
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
            .filter(|x| x.account.owner == owner)
            .collect::<Vec<EnrichedAccount>>();

        assert_account_response_list_matches_input(
            &mut response_accounts,
            &mut accounts_of_interest,
        );
    }

    let mut accounts_of_interest = vec![accounts[0].clone(), accounts[2].clone()];
    let mut res = setup
        .api
        .get_multiple_compressed_accounts(GetMultipleCompressedAccountsRequest {
            addresses: None,
            hashes: Some(accounts_of_interest.iter().map(|x| x.hash.into()).collect()),
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
    use photon_indexer::ingester::parser::indexer_events::AccountState;
    use sqlx::types::Decimal;

    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;
    let mint1 = Pubkey::new_unique();
    let mint2 = Pubkey::new_unique();
    let mint3 = Pubkey::new_unique();
    let owner1 = Pubkey::new_unique();
    let owner2 = Pubkey::new_unique();
    let delegate1 = Pubkey::new_unique();
    let delegate2 = Pubkey::new_unique();

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
        state: AccountState::Frozen,
        is_native: Some(1),
        delegated_amount: 1,
    };

    let token_data2 = TokenData {
        mint: mint2,
        owner: owner1,
        amount: 2,
        delegate: Some(delegate2),
        state: AccountState::Initialized,
        is_native: None,
        delegated_amount: 2,
    };

    let token_data3 = TokenData {
        mint: mint3,
        owner: owner2,
        amount: 3,
        delegate: Some(delegate1),
        state: AccountState::Frozen,
        is_native: Some(1000),
        delegated_amount: 3,
    };
    let all_token_data = vec![token_data1, token_data2, token_data3];

    let txn = sea_orm::TransactionTrait::begin(setup.db_conn.as_ref())
        .await
        .unwrap();

    let mut token_datas = Vec::new();

    for token_data in all_token_data.iter() {
        let slot = 11;
        let hash = Hash::new_unique();
        let model = accounts::ActiveModel {
            hash: Set(hash.clone().into()),
            address: Set(Some(Pubkey::new_unique().to_bytes().to_vec())),
            spent: Set(false),
            data: Set(to_vec(&token_data).unwrap()),
            owner: Set(token_data.owner.to_bytes().to_vec()),
            lamports: Set(Decimal::from(10)),
            slot_updated: Set(slot),
            discriminator: Set(Vec::new()),
            ..Default::default()
        };
        accounts::Entity::insert(model).exec(&txn).await.unwrap();
        token_datas.push(EnrichedTokenAccount {
            hash,
            token_data: *token_data,
            slot_updated: slot as u64,
            address: None,
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
        .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByAuthority(
            SerializablePubkey::from(owner1),
            Some(GetCompressedTokenAccountsByAuthorityOptions {
                mint: Some(SerializablePubkey::from(mint1)),
                ..Default::default()
            }),
        ))
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
            .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByAuthority(
                SerializablePubkey::from(owner),
                None,
            ))
            .await
            .unwrap()
            .value;

        let mut paginated_res = Vec::new();
        let mut cursor = None;
        loop {
            let res = setup
                .api
                .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByAuthority(
                    SerializablePubkey::from(owner),
                    Some(GetCompressedTokenAccountsByAuthorityOptions {
                        cursor: cursor.clone(),
                        limit: Some(photon_indexer::api::method::utils::Limit::new(1).unwrap()),
                        mint: None,
                    }),
                ))
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
        verify_responses_match_tlv_data(res.clone(), owner_tlv);
        for token_account in res.items {
            let request = CompressedAccountRequest {
                address: None,
                hash: Some(token_account.hash),
            };
            let balance = setup
                .api
                .get_compressed_token_account_balance(request)
                .await
                .unwrap()
                .value;
            assert_eq!(balance.amount.parse::<u64>().unwrap(), token_account.amount);
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
            .get_compressed_token_accounts_by_delegate(GetCompressedTokenAccountsByAuthority(
                SerializablePubkey::from(delegate),
                None,
            ))
            .await
            .unwrap()
            .value;
        let mut paginated_res = Vec::new();
        let mut cursor = None;
        loop {
            let res = setup
                .api
                .get_compressed_token_accounts_by_delegate(GetCompressedTokenAccountsByAuthority(
                    SerializablePubkey::from(delegate),
                    Some(GetCompressedTokenAccountsByAuthorityOptions {
                        cursor: cursor.clone(),
                        limit: Some(photon_indexer::api::method::utils::Limit::new(1).unwrap()),
                        mint: None,
                    }),
                ))
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
    use photon_indexer::ingester::parser::{
        indexer_events::{CompressedAccount, CompressedAccountData, PathNode},
        state_update::EnrichedPathNode,
    };

    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;

    fn generate_random_account(tree: Pubkey, seq: i64) -> EnrichedAccount {
        EnrichedAccount {
            account: CompressedAccount {
                data: Some(CompressedAccountData {
                    discriminator: [0; 8],
                    data: vec![1; 500],
                    data_hash: [0; 32],
                }),
                address: Some(Pubkey::new_unique().to_bytes()),
                lamports: 1000,
                owner: Pubkey::new_unique(),
            },
            tree,
            seq: Some(seq as u64),
            hash: [0; 32],
            slot: 0,
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
                .map(|i| generate_random_leaf_index(tree, i as u32, i))
                .collect(),
        };
        persist_state_update(&txn, state_update).await.unwrap();
        txn.commit().await.unwrap();
    }
}
