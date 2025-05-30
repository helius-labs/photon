use crate::utils::compare_account_with_account_v2;
use crate::utils::*;
use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use function_name::named;
use photon_indexer::api::method::get_compressed_accounts_by_owner::{
    DataSlice, FilterSelector, GetCompressedAccountsByOwnerRequest, Memcmp,
};
use photon_indexer::api::method::get_compressed_balance_by_owner::GetCompressedBalanceByOwnerRequest;
use photon_indexer::api::method::get_compressed_token_balances_by_owner::GetCompressedTokenBalancesByOwnerRequest;
use photon_indexer::api::method::get_multiple_compressed_accounts::GetMultipleCompressedAccountsRequest;
use photon_indexer::api::method::get_validity_proof::{
    get_validity_proof, get_validity_proof_v2, GetValidityProofRequest, GetValidityProofRequestV2,
};
use photon_indexer::api::method::utils::{
    CompressedAccountRequest, GetCompressedTokenAccountsByDelegate,
    GetCompressedTokenAccountsByOwner,
};
use photon_indexer::common::typedefs::bs58_string::Base58String;
use photon_indexer::ingester::persist::persisted_indexed_merkle_tree::{
    get_exclusion_range_with_proof_v2, update_indexed_tree_leaves_v1, validate_tree,
};

use photon_indexer::common::typedefs::unsigned_integer::UnsignedInteger;
use photon_indexer::dao::generated::{indexed_trees, state_trees};
use photon_indexer::ingester::persist::persisted_indexed_merkle_tree::multi_append;
use photon_indexer::ingester::persist::persisted_state_tree::ZERO_BYTES;
use sea_orm::{QueryFilter, TransactionTrait};

use photon_indexer::common::typedefs::account::{Account, AccountContext, AccountWithContext};
use photon_indexer::common::typedefs::bs64_string::Base64String;
use photon_indexer::common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};
use photon_indexer::dao::generated::accounts;
use photon_indexer::ingester::index_block;
use photon_indexer::ingester::parser::state_update::StateUpdate;
use photon_indexer::ingester::persist::{
    compute_parent_hash, get_multiple_compressed_leaf_proofs, persist_leaf_nodes,
    persist_token_accounts, EnrichedTokenAccount, LeafNode,
};

use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use sea_orm::{EntityTrait, Set};
use serial_test::serial;

use photon_indexer::common::typedefs::account::AccountData;
use std::collections::{HashMap, HashSet};

use photon_indexer::common::typedefs::token_data::{AccountState, TokenData};
use sqlx::types::Decimal;

use light_compressed_account::TreeType;
use photon_indexer::common::typedefs::limit::Limit;
use sea_orm::ColumnTrait;
use solana_pubkey::Pubkey;
use std::vec;

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone)]
struct Person {
    name: String,
    age: u64,
}

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
        seq: Some(UnsignedInteger(0)),
        slot_created: UnsignedInteger(0),
    };

    state_update.out_accounts.push(AccountWithContext {
        account: account.clone(),
        context: AccountContext::default(),
    });
    persist_state_update_using_connection(&setup.db_conn, state_update)
        .await
        .unwrap();

    let request = CompressedAccountRequest {
        address: None,
        hash: Some(account.hash.clone()),
    };

    let res = setup
        .api
        .get_compressed_account(request.clone())
        .await
        .unwrap()
        .value;

    assert_eq!(res, Some(account.clone()));

    let res = setup
        .api
        .get_compressed_account_balance(request)
        .await
        .unwrap()
        .value;

    assert_eq!(res, account.lamports);

    let null_value = setup
        .api
        .get_compressed_account(CompressedAccountRequest {
            hash: Some(Hash::from(Pubkey::new_unique().to_bytes())),
            address: None,
        })
        .await
        .unwrap();

    assert_eq!(null_value.value, None);
}
#[named]
#[rstest]
#[tokio::test]
#[serial]
// Test V1 accounts with V1 and V2 endpoints:
// get_compressed_accounts_by_owner
// get_compressed_accounts_by_owner_v2
// get_multiple_compressed_accounts
// get_multiple_compressed_accounts_v2
// get_compressed_account
// get_compressed_account_v2
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
        AccountWithContext {
            account: Account {
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
                seq: Some(UnsignedInteger(1)),
                slot_created: UnsignedInteger(0),
            },
            context: AccountContext {
                tree_type: TreeType::StateV1 as u16,
                ..AccountContext::default()
            },
        },
        AccountWithContext {
            account: Account {
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
                seq: Some(UnsignedInteger(2)),
                slot_created: UnsignedInteger(0),
            },
            context: AccountContext {
                tree_type: TreeType::StateV1 as u16,
                ..AccountContext::default()
            },
        },
        AccountWithContext {
            account: Account {
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
                seq: Some(UnsignedInteger(3)),
                slot_created: UnsignedInteger(1),
            },
            context: AccountContext {
                tree_type: TreeType::StateV1 as u16,
                ..AccountContext::default()
            },
        },
        AccountWithContext {
            account: Account {
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
                seq: Some(UnsignedInteger(1)),
                slot_created: UnsignedInteger(0),
            },
            context: AccountContext {
                tree_type: TreeType::StateV1 as u16,
                ..AccountContext::default()
            },
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
                owner,
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
                    owner,
                    cursor: cursor.clone(),
                    limit: Some(Limit::new(1).unwrap()),
                    ..Default::default()
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
            .map(|x| x.account)
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
            .get_compressed_balance_by_owner(GetCompressedBalanceByOwnerRequest { owner })
            .await
            .unwrap()
            .value;

        assert_eq!(res.0, total_balance);

        // V2 Endpoint
        let res_v2 = setup
            .api
            .get_compressed_accounts_by_owner_v2(GetCompressedAccountsByOwnerRequest {
                owner,
                ..Default::default()
            })
            .await
            .unwrap()
            .value;

        for (account, account_v2) in response_accounts.iter().zip(res_v2.items.iter()) {
            compare_account_with_account_v2(account, account_v2);
        }
    }

    let mut accounts_of_interest = vec![accounts[0].account.clone(), accounts[2].account.clone()];
    let res = setup
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

    assert_account_response_list_matches_input(
        &mut res.items.iter().map(|x| x.clone().unwrap()).collect(),
        &mut accounts_of_interest,
    );

    // V2 Endpoint
    let res_v2 = setup
        .api
        .get_multiple_compressed_accounts_v2(GetMultipleCompressedAccountsRequest {
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

    assert_account_response_list_matches_input_v2(
        &mut res_v2.items.iter().map(|x| x.clone().unwrap()).collect(),
        &mut accounts_of_interest,
    );

    for account in accounts.iter() {
        let request = CompressedAccountRequest {
            address: account.account.address,
            hash: Some(account.account.hash.clone()),
        };

        let res = setup
            .api
            .get_compressed_account(request.clone())
            .await
            .unwrap()
            .value;

        assert_eq!(res, Some(account.account.clone()));

        let res_v2 = setup
            .api
            .get_compressed_account_v2(request)
            .await
            .unwrap()
            .value;

        compare_account_with_account_v2(&res.unwrap(), &res_v2.unwrap());
    }
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
// Test V1 token accounts with V1 and V2 endpoints:
// get_compressed_token_accounts_by_owner
// get_compressed_token_accounts_by_owner_v2
// get_compressed_token_balances_by_owner
// get_compressed_token_balances_by_owner_v2
// get_compressed_token_account_balance
// get_compressed_token_accounts_by_delegate
// get_compressed_token_accounts_by_delegate_v2
async fn test_persist_token_data(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use photon_indexer::api::method::get_compressed_mint_token_holders::GetCompressedMintTokenHoldersRequest;

    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;
    let mint1 = SerializablePubkey::new_unique();
    let mint2 = SerializablePubkey::new_unique();
    let mint3 = SerializablePubkey::new_unique();
    let owner1 = SerializablePubkey::new_unique();
    let owner2 = SerializablePubkey::new_unique();
    let owner3 = SerializablePubkey::new_unique();
    let owner4 = SerializablePubkey::new_unique();
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
        tlv: None,
    };

    let token_data2 = TokenData {
        mint: mint2,
        owner: owner1,
        amount: UnsignedInteger(2),
        delegate: Some(delegate2),
        state: AccountState::initialized,
        tlv: None,
    };

    let token_data3 = TokenData {
        mint: mint3,
        owner: owner2,
        amount: UnsignedInteger(3),
        delegate: Some(delegate1),
        state: AccountState::frozen,
        tlv: None,
    };
    let token_data4 = TokenData {
        mint: mint1,
        owner: owner2,
        amount: UnsignedInteger(4),
        delegate: Some(delegate1),
        state: AccountState::frozen,
        tlv: None,
    };
    let token_data5 = TokenData {
        mint: mint1,
        owner: owner3,
        amount: UnsignedInteger(4),
        delegate: Some(delegate1),
        state: AccountState::frozen,
        tlv: None,
    };
    let token_data6 = TokenData {
        mint: mint1,
        owner: owner4,
        amount: UnsignedInteger(6),
        delegate: Some(delegate1),
        state: AccountState::frozen,
        tlv: None,
    };
    let token_data7 = TokenData {
        mint: mint1,
        owner: owner2,
        amount: UnsignedInteger(4),
        delegate: Some(delegate1),
        state: AccountState::frozen,
        tlv: None,
    };
    let all_token_data = vec![
        token_data1,
        token_data2,
        token_data3,
        token_data4,
        token_data5,
        token_data6,
        token_data7,
    ];
    let hashes = all_token_data
        .iter()
        .map(|_| Hash::new_unique())
        .collect::<HashSet<Hash>>();

    let all_token_data: Vec<TokenDataWithHash> = all_token_data
        .iter()
        .zip(hashes.iter())
        .map(|(token_data, hash)| TokenDataWithHash {
            token_data: token_data.clone(),
            hash: hash.clone(),
        })
        .collect();

    let mut mint_to_owner_to_balance = HashMap::new();
    for token_data in all_token_data.clone() {
        let token_data = token_data.token_data;
        let mint = token_data.mint;
        let owner = token_data.owner;
        let mint_owner_balances = mint_to_owner_to_balance
            .entry(mint)
            .or_insert(HashMap::new());
        let balance = mint_owner_balances.entry(owner).or_insert(0);
        *balance += token_data.amount.0;
    }

    let txn = sea_orm::TransactionTrait::begin(setup.db_conn.as_ref())
        .await
        .unwrap();

    let mut token_datas = Vec::new();

    for (i, token_data) in all_token_data.iter().enumerate() {
        let slot = 11;
        let hash = token_data.hash.clone();
        let token_data = token_data.token_data.clone();
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
            queue: Set(Pubkey::new_unique().to_bytes().to_vec()),
            tree_type: Set(TreeType::StateV1 as i32),
            seq: Set(Some(0)),
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
        .filter(|x| x.token_data.owner == owner1 && x.token_data.mint == mint1)
        .map(Clone::clone)
        .collect();

    let res = setup
        .api
        .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwner {
            owner: owner1,
            mint: Some(mint1),
            ..Default::default()
        })
        .await
        .unwrap()
        .value;
    verify_response_matches_input_token_data(res.clone(), owner_tlv);

    let res_v2 = setup
        .api
        .get_compressed_token_accounts_by_owner_v2(GetCompressedTokenAccountsByOwner {
            owner: owner1,
            mint: Some(mint1),
            ..Default::default()
        })
        .await
        .unwrap()
        .value;
    for (item, item_v2) in res.items.iter().zip(res_v2.items.iter()) {
        compare_token_account_with_token_account_v2(item, item_v2);
    }

    for owner in [owner2] {
        let owner_tlv = all_token_data
            .iter()
            .filter(|x| x.token_data.owner == owner)
            .map(Clone::clone)
            .collect();
        let res = setup
            .api
            .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwner {
                owner,
                ..Default::default()
            })
            .await
            .unwrap()
            .value;

        let res_v2 = setup
            .api
            .get_compressed_token_accounts_by_owner_v2(GetCompressedTokenAccountsByOwner {
                owner,
                ..Default::default()
            })
            .await
            .unwrap()
            .value;

        for (item, item_v2) in res.items.iter().zip(res_v2.items.iter()) {
            compare_token_account_with_token_account_v2(item, item_v2);
        }

        let mut paginated_res = Vec::new();
        let mut cursor = None;
        loop {
            let res = setup
                .api
                .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwner {
                    owner,
                    cursor: cursor.clone(),
                    limit: Some(Limit::new(1).unwrap()),
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

        for (item, item_v2) in paginated_res.iter().zip(res_v2.items.iter()) {
            compare_token_account_with_token_account_v2(item, item_v2);
        }

        let mut mint_to_balance: HashMap<SerializablePubkey, u64> = HashMap::new();

        for token_account in paginated_res.iter() {
            let balance = mint_to_balance
                .entry(token_account.token_data.mint)
                .or_insert(0);
            *balance += token_account.token_data.amount.0;
        }
        for (mint, balance) in mint_to_balance.iter() {
            let request = GetCompressedTokenBalancesByOwnerRequest {
                owner,
                mint: Some(*mint),
                ..Default::default()
            };
            let res = setup
                .api
                .get_compressed_token_balances_by_owner(request.clone())
                .await
                .unwrap()
                .value;
            assert_eq!(res.token_balances[0].balance.0, *balance);

            let res_v2 = setup
                .api
                .get_compressed_token_balances_by_owner_v2(request)
                .await
                .unwrap()
                .value;
            assert_eq!(res_v2.items, res.token_balances);
        }

        verify_response_matches_input_token_data(res.clone(), owner_tlv);
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
        let delegate_tlv: Vec<TokenDataWithHash> = all_token_data
            .clone()
            .into_iter()
            .filter(|x| x.token_data.delegate == Some(delegate))
            .collect();

        // V1 Endpoint
        let res_v1 = setup
            .api
            .get_compressed_token_accounts_by_delegate(GetCompressedTokenAccountsByDelegate {
                delegate,
                ..Default::default()
            })
            .await
            .unwrap()
            .value;

        let mut paginated_res_v1 = Vec::new();
        let mut cursor = None;
        loop {
            let res = setup
                .api
                .get_compressed_token_accounts_by_delegate(GetCompressedTokenAccountsByDelegate {
                    delegate,
                    cursor: cursor.clone(),
                    limit: Some(Limit::new(1).unwrap()),
                    ..Default::default()
                })
                .await
                .unwrap()
                .value;

            paginated_res_v1.extend(res.items.clone());
            cursor = res.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(paginated_res_v1, res_v1.items);
        verify_response_matches_input_token_data(res_v1.clone(), delegate_tlv.clone());

        // V2 Endpoint
        let res_v2 = setup
            .api
            .get_compressed_token_accounts_by_delegate_v2(GetCompressedTokenAccountsByDelegate {
                delegate,
                ..Default::default()
            })
            .await
            .unwrap()
            .value;

        let mut paginated_res_v2 = Vec::new();
        let mut cursor = None;
        loop {
            let res = setup
                .api
                .get_compressed_token_accounts_by_delegate_v2(
                    GetCompressedTokenAccountsByDelegate {
                        delegate,
                        cursor: cursor.clone(),
                        limit: Some(Limit::new(1).unwrap()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .value;

            paginated_res_v2.extend(res.items.clone());
            cursor = res.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(paginated_res_v2, res_v2.items);
        verify_response_matches_input_token_data_v2(res_v2, delegate_tlv);
    }

    for (mint, owner_to_balance) in mint_to_owner_to_balance.iter() {
        let mut items = Vec::new();

        let mut cursor: Option<Base58String> = None;
        loop {
            let res = setup
                .api
                .get_compressed_mint_token_holders(GetCompressedMintTokenHoldersRequest {
                    mint: *mint,
                    limit: Some(Limit::new(1).unwrap()),
                    cursor,
                })
                .await
                .unwrap()
                .value;
            for item in res.items.clone() {
                items.push(item);
            }
            cursor = res.cursor;
            if cursor.is_none() {
                break;
            }
        }
        // Assert that items are in descending balance order
        for i in 1..items.len() {
            assert!(items[i].balance.0 <= items[i - 1].balance.0);
        }
        assert_eq!(items.len(), owner_to_balance.len());
        for item in items {
            assert_eq!(item.balance.0, *owner_to_balance.get(&item.owner).unwrap());
        }
    }

    let mut owner_to_balances = HashMap::new();
    for (mint, balances) in mint_to_owner_to_balance.into_iter() {
        for (owner, balance) in balances.into_iter() {
            owner_to_balances
                .entry(owner)
                .or_insert(HashMap::new())
                .insert(mint, balance);
        }
    }
    for owner in owner_to_balances.keys() {
        let mut cursor = None;
        let mut mint_to_balance = HashMap::new();

        loop {
            let request = GetCompressedTokenBalancesByOwnerRequest {
                owner: *owner,
                cursor,
                limit: Some(Limit::new(1).unwrap()),
                ..Default::default()
            };
            let res = setup
                .api
                .get_compressed_token_balances_by_owner(request)
                .await
                .unwrap()
                .value;
            let token_balances = res.token_balances;
            for token_balance in token_balances.iter() {
                let balance = mint_to_balance.entry(token_balance.mint).or_insert(0);
                *balance += token_balance.balance.0;
            }
            cursor = res.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(mint_to_balance, *owner_to_balances.get(owner).unwrap());
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
    let num_nodes = 5;

    let leaf_nodes: Vec<LeafNode> = (0..num_nodes)
        .map(|i| LeafNode {
            hash: Hash::new_unique(),
            leaf_index: i,
            tree,
            seq: Some(i),
        })
        .collect();
    let txn = setup.db_conn.as_ref().begin().await.unwrap();
    let tree_height = 32; // prev. 5
    persist_leaf_nodes(&txn, leaf_nodes.clone(), tree_height)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let proofs = get_multiple_compressed_leaf_proofs(
        &setup.db_conn.begin().await.unwrap(),
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
    }

    // Repeat in order to test updates

    let leaf_nodes: Vec<LeafNode> = (0..num_nodes)
        .map(|i| LeafNode {
            hash: Hash::new_unique(),
            leaf_index: i,
            tree,
            seq: Some(i + num_nodes),
        })
        .collect();
    let txn = setup.db_conn.as_ref().begin().await.unwrap();
    persist_leaf_nodes(&txn, leaf_nodes.clone(), tree_height)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let leaves = leaf_nodes
        .iter()
        .map(|x| Hash::try_from(x.hash.clone()).unwrap())
        .collect();

    let proofs = get_multiple_compressed_leaf_proofs(&setup.db_conn.begin().await.unwrap(), leaves)
        .await
        .unwrap();

    let proof_hashes: HashSet<Hash> = proofs.iter().map(|x| x.hash.clone()).collect();
    let leaf_hashes: HashSet<Hash> = leaf_nodes.iter().map(|x| x.hash.clone()).collect();
    assert_eq!(proof_hashes, leaf_hashes);

    for proof in proofs {
        assert_eq!(proof.merkle_tree, tree);
        assert_eq!(num_nodes as u64 - 1 + num_nodes as u64, proof.root_seq);
        assert_eq!(tree_height - 1, proof.proof.len() as u32);
    }
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_indexed_merkle_trees(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use photon_indexer::ingester::persist::persisted_indexed_merkle_tree::validate_tree;

    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;
    let tree = SerializablePubkey::new_unique();
    let num_nodes = 2;

    let txn = sea_orm::TransactionTrait::begin(setup.db_conn.as_ref())
        .await
        .unwrap();

    let values = (0..num_nodes).map(|i| vec![i * 4 + 1]).collect();
    let tree_height = 33; // prev. 4

    multi_append(&txn, values, tree.to_bytes_vec(), tree_height - 1)
        .await
        .unwrap();

    txn.commit().await.unwrap();

    let (model, _) = get_exclusion_range_with_proof_v2(
        &setup.db_conn.begin().await.unwrap(),
        tree.to_bytes_vec(),
        tree_height,
        vec![3],
    )
    .await
    .unwrap();

    let expected_model = indexed_trees::Model {
        tree: tree.to_bytes_vec(),
        leaf_index: 1,
        value: vec![1],
        next_index: 2,
        next_value: vec![5],
        seq: Some(0),
    };

    assert_eq!(model, expected_model);

    let txn = sea_orm::TransactionTrait::begin(setup.db_conn.as_ref())
        .await
        .unwrap();

    let values = vec![vec![3]];

    multi_append(&txn, values, tree.to_bytes_vec(), tree_height - 1)
        .await
        .unwrap();

    txn.commit().await.unwrap();

    validate_tree(setup.db_conn.as_ref(), tree).await;

    let (model, _) = get_exclusion_range_with_proof_v2(
        &setup.db_conn.begin().await.unwrap(),
        tree.to_bytes_vec(),
        tree_height,
        vec![4],
    )
    .await
    .unwrap();

    let expected_model = indexed_trees::Model {
        tree: tree.to_bytes_vec(),
        leaf_index: 3,
        value: vec![3],
        next_index: 2,
        next_value: vec![5],
        seq: Some(0),
    };

    assert_eq!(model, expected_model);
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_get_multiple_new_address_proofs(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use photon_indexer::api::method::get_multiple_new_address_proofs::{
        get_multiple_new_address_proofs, AddressList,
    };

    let name = trim_test_name(function_name!());
    let setup = setup(name.clone(), db_backend).await;

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

    let addresses = vec![
        SerializablePubkey::try_from("Fi6AXBGuGs7DRXP428hwhJJfTpJ4BVZD8DiUcX1cj35W").unwrap(),
        SerializablePubkey::try_from("sH8ux4csv8wxiRejuHjpTCkrfGgeNtg6Y55AJJ9GJSd").unwrap(),
    ];
    let proof = get_multiple_new_address_proofs(&setup.db_conn, AddressList(addresses.clone()))
        .await
        .unwrap();
    insta::assert_json_snapshot!(name, proof);
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_get_multiple_new_address_proofs_interop(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use photon_indexer::api::method::{
        get_multiple_new_address_proofs::{
            get_multiple_new_address_proofs, get_multiple_new_address_proofs_v2, AddressList,
            AddressListWithTrees, AddressWithTree, ADDRESS_TREE_V1,
        },
        get_validity_proof::CompressedProof,
    };

    let name = trim_test_name(function_name!());
    let setup = setup(name.clone(), db_backend).await;

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

    let addresses = vec![SerializablePubkey::try_from(vec![
        0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42, 42, 42, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        26, 27, 28, 29, 30, 31, 32,
    ])
    .unwrap()];
    let proof = get_multiple_new_address_proofs(&setup.db_conn, AddressList(addresses.clone()))
        .await
        .unwrap();
    insta::assert_json_snapshot!(name.clone(), proof);
    let mut validity_proof = get_validity_proof(
        &setup.db_conn,
        &setup.prover_url,
        GetValidityProofRequest {
            new_addresses: addresses.clone(),
            new_addresses_with_trees: vec![],
            hashes: vec![],
        },
    )
    .await
    .unwrap();
    // The Gnark prover has some randomness.
    validity_proof.value.compressed_proof = CompressedProof::default();

    insta::assert_json_snapshot!(format!("{}-validity-proof", name), validity_proof);

    // Repeat with V2
    let addresses_with_trees: Vec<AddressWithTree> = addresses
        .clone()
        .into_iter()
        .map(|address| AddressWithTree {
            address,
            tree: SerializablePubkey::from(ADDRESS_TREE_V1),
        })
        .collect();
    let proof_v2 = get_multiple_new_address_proofs_v2(
        &setup.db_conn,
        AddressListWithTrees(addresses_with_trees.clone()),
    )
    .await
    .unwrap();

    insta::assert_json_snapshot!(name.clone(), proof_v2);
    let mut validity_proof_v2 = get_validity_proof_v2(
        &setup.db_conn,
        &setup.prover_url,
        GetValidityProofRequestV2 {
            new_addresses_with_trees: addresses_with_trees.clone(),
            hashes: vec![],
        },
    )
    .await
    .unwrap();
    // The Gnark prover has some randomness.
    validity_proof_v2.value.compressed_proof = Some(CompressedProof::default());

    insta::assert_json_snapshot!(format!("{}-validity-proof-v2", name), validity_proof_v2);
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
#[ignore]
async fn load_test(#[values(DatabaseBackend::Postgres)] db_backend: DatabaseBackend) {
    let name = trim_test_name(function_name!());
    let setup = setup(name.clone(), db_backend).await;

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

    let mut state_update = StateUpdate::default();

    let tree = SerializablePubkey::new_unique();

    fn generate_mock_account(leaf_index: u64, tree: SerializablePubkey) -> AccountWithContext {
        AccountWithContext {
            account: Account {
                hash: Hash::new_unique(),
                address: Some(SerializablePubkey::new_unique()),
                data: Some(AccountData {
                    discriminator: UnsignedInteger(1),
                    data: Base64String(vec![1; 500]),
                    data_hash: Hash::new_unique(),
                }),
                owner: SerializablePubkey::new_unique(),
                lamports: UnsignedInteger(1000),
                tree,
                leaf_index: UnsignedInteger(leaf_index),
                seq: Some(UnsignedInteger(0)),
                slot_created: UnsignedInteger(0),
            },
            context: AccountContext::default(),
        }
    }

    for i in 0..100000 {
        state_update
            .out_accounts
            .push(generate_mock_account(i, tree));
    }
    persist_state_update_using_connection(setup.db_conn.as_ref(), state_update)
        .await
        .unwrap();
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_persisted_state_trees_bug_with_latter_smaller_seq_values(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use photon_indexer::{
        api::method::get_multiple_new_address_proofs::{
            get_multiple_new_address_proofs, AddressList,
        },
        ingester::persist::persisted_indexed_merkle_tree::validate_tree,
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
    let tree =
        SerializablePubkey::try_from("C83cpRN6oaafjNgMQJvaYgAz592EP5wunKvbokeTKPLn").unwrap();

    let leaf_nodes_1 = vec![
        LeafNode {
            tree,
            leaf_index: 0,
            hash: Hash::try_from("34yinGSAmWKeXw61zZzd8hbE1ySB1pDmgiHzJhRtVwJY").unwrap(),
            seq: Some(4),
        },
        LeafNode {
            tree,
            leaf_index: 1,
            hash: Hash::try_from("34cMT7MjFrs8hLp2zHMrPJHKkUxBDBwBTNck77wLjjcY").unwrap(),
            seq: Some(0),
        },
        LeafNode {
            tree,
            leaf_index: 2,
            hash: Hash::try_from("TTSZiUJsGTcU7sXqYtw53yFY5Ag7DmHXR4GzEjVk7J7").unwrap(),
            seq: Some(5),
        },
    ];
    let leaf_nodes_2 = vec![
        LeafNode {
            tree,
            leaf_index: 0,
            hash: Hash::try_from("3hH3oNVj2bafrqqXLnZjLjkuDaoxKhyyvmxaSs939hws").unwrap(),
            seq: Some(0),
        },
        LeafNode {
            tree,
            leaf_index: 1,
            hash: Hash::try_from("34cMT7MjFrs8hLp2zHMrPJHKkUxBDBwBTNck77wLjjcY").unwrap(),
            seq: Some(0),
        },
        LeafNode {
            tree,
            leaf_index: 2,
            hash: Hash::try_from("25D2cs6h29NZgmDepVqc7bLLSWcNJnMvGoxeTpyZjF3u").unwrap(),
            seq: Some(10),
        },
    ];
    let leaf_node_chunks = vec![leaf_nodes_1, leaf_nodes_2];

    for chunk in leaf_node_chunks {
        let txn = setup.db_conn.as_ref().begin().await.unwrap();
        persist_leaf_nodes(&txn, chunk.clone(), 26).await.unwrap();
        txn.commit().await.unwrap();

        let proof_address = "12prJNGB6sfTMrZM1Udv2Aamv9fLzpm5YfMqssTmGrWy";

        let address_list = AddressList(vec![SerializablePubkey::try_from(proof_address).unwrap()]);

        validate_tree(setup.db_conn.as_ref(), tree).await;
        get_multiple_new_address_proofs(&setup.db_conn, address_list)
            .await
            .unwrap();
    }
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_gpa_filters(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use log::info;

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
    let mut state_update = StateUpdate::default();

    let accounts = vec![AccountWithContext {
        account: Account {
            hash: Hash::new_unique(),
            address: Some(SerializablePubkey::new_unique()),
            data: Some(AccountData {
                discriminator: UnsignedInteger(0),
                data: Base64String(vec![1, 2, 3]),
                data_hash: Hash::new_unique(),
            }),
            owner: owner1,
            lamports: UnsignedInteger(1000),
            tree: SerializablePubkey::new_unique(),
            leaf_index: UnsignedInteger(10),
            seq: Some(UnsignedInteger(1)),
            slot_created: UnsignedInteger(0),
        },
        context: AccountContext::default(),
    }];
    state_update.out_accounts = accounts.clone();
    persist_state_update_using_connection(&setup.db_conn, state_update)
        .await
        .unwrap();

    let res = setup
        .api
        .get_compressed_accounts_by_owner(GetCompressedAccountsByOwnerRequest {
            owner: owner1,
            dataSlice: Some(DataSlice {
                offset: 0,
                length: 2,
            }),
            ..Default::default()
        })
        .await
        .unwrap()
        .value;

    assert!(res.items[0].data.clone().unwrap().data.0 == vec![1, 2]);

    let filters_and_expected_results = vec![
        ((vec![1, 2], 0), 1),
        ((vec![1, 2, 3], 0), 1),
        ((vec![2, 3], 1), 1),
        ((vec![2, 3], 0), 0),
    ];

    for (filter, expected_count) in filters_and_expected_results {
        info!("Filter: {:?}", filter);
        let res = setup
            .api
            .get_compressed_accounts_by_owner(GetCompressedAccountsByOwnerRequest {
                owner: owner1,
                filters: vec![FilterSelector {
                    memcmp: Some(Memcmp {
                        offset: filter.1,
                        bytes: Base58String(filter.0.iter().map(|x| *x as u8).collect()),
                    }),
                }],
                ..Default::default()
            })
            .await
            .unwrap()
            .value;

        assert_eq!(res.items.len(), expected_count);
    }
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_persisted_state_trees_multiple_cases(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let tree = SerializablePubkey::new_unique();
    let tree_height = 32; // prev. 10

    info!("Test case 1: Sequential leaf nodes");
    let leaf_nodes_1 = create_leaf_nodes(tree, 0..5, |i| i);
    test_persist_and_verify(name.clone(), db_backend, tree, leaf_nodes_1, tree_height).await;

    info!("Test case 2: Sequential leaf nodes with random seq order");
    let leaf_nodes_2 = create_leaf_nodes(tree, 0..5, |i| vec![4, 2, 0, 1, 3][i as usize]);
    test_persist_and_verify(name.clone(), db_backend, tree, leaf_nodes_2, tree_height).await;

    info!("Test case 3: Duplicate leaf indices");
    let leaf_nodes_3 = create_leaf_nodes(tree, vec![1, 1, 2, 2, 3, 2].into_iter(), |_| {
        let mut rng = rand::thread_rng();
        rng.gen_range(0..1000)
    });
    test_persist_and_verify(name.clone(), db_backend, tree, leaf_nodes_3, tree_height).await;

    info!("Test case 4: Large gaps in sequence numbers");
    let leaf_nodes_4 = create_leaf_nodes(tree, 0..5, |i| i * 1000);
    test_persist_and_verify(name.clone(), db_backend, tree, leaf_nodes_4, tree_height).await;

    info!("Test case 7: Very large tree");
    let large_tree_height = 32; // prev. 20
    let leaf_nodes_7 = create_leaf_nodes(tree, 0..20, |i| i);
    test_persist_and_verify(
        name.clone(),
        db_backend,
        tree,
        leaf_nodes_7,
        large_tree_height,
    )
    .await;

    info!("Test case 10: Random sequence numbers");
    use log::info;
    use rand::Rng;
    let leaf_nodes_10 = create_leaf_nodes(tree, 0..10, |_| {
        let mut rng = rand::thread_rng();
        rng.gen_range(0..1000)
    });
    test_persist_and_verify(name.clone(), db_backend, tree, leaf_nodes_10, tree_height).await;

    info!("Test case 11: Maximum sequence number (u32::MAX)");
    let leaf_nodes_11 = create_leaf_nodes(tree, 0..5, |i| if i == 2 { u32::MAX as u64 } else { i });
    test_persist_and_verify(name.clone(), db_backend, tree, leaf_nodes_11, tree_height).await;

    info!("Test case 12: Updating all leaves in reverse order");
    let leaf_nodes_13b = create_leaf_nodes(tree, (0..=10).rev(), |i| i + 100);
    test_persist_and_verify(name.clone(), db_backend, tree, leaf_nodes_13b, tree_height).await;

    info!("Test case 13: Inserting leaves with all zero hashes");
    let leaf_nodes_13 = (0..5)
        .map(|i| LeafNode {
            hash: Hash::try_from(vec![0; 32]).unwrap(),
            leaf_index: i,
            tree,
            seq: Some(i),
        })
        .collect::<Vec<_>>();
    test_persist_and_verify(name.clone(), db_backend, tree, leaf_nodes_13, tree_height).await;
}

fn create_leaf_nodes<F>(
    tree: SerializablePubkey,
    range: impl Iterator<Item = u64>,
    seq_fn: F,
) -> Vec<LeafNode>
where
    F: Fn(u64) -> u64,
{
    range
        .map(|i| LeafNode {
            hash: Hash::new_unique(),
            leaf_index: i as u32,
            tree,
            seq: Some(seq_fn(i) as u32),
        })
        .collect()
}

async fn test_persist_and_verify(
    name: String,
    db_backend: DatabaseBackend,
    tree: SerializablePubkey,
    mut leaf_nodes: Vec<LeafNode>,
    tree_height: u32,
) {
    for one_at_a_time in [true, false] {
        let setup = setup(name.clone(), db_backend).await;

        let txn = setup.db_conn.as_ref().begin().await.unwrap();
        if one_at_a_time {
            for leaf_node in leaf_nodes.clone() {
                persist_leaf_nodes(&txn, vec![leaf_node], tree_height)
                    .await
                    .unwrap();
            }
        } else {
            persist_leaf_nodes(&txn, leaf_nodes.clone(), tree_height)
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();

        let mut leaf_node_indexes = vec![];
        let mut de_duplicated_leaf_nodes = vec![];

        leaf_nodes.sort_by_key(|x| x.seq);
        for leaf_node in leaf_nodes.into_iter().rev() {
            if !leaf_node_indexes.contains(&leaf_node.leaf_index) {
                leaf_node_indexes.push(leaf_node.leaf_index);
                de_duplicated_leaf_nodes.push(leaf_node);
            }
        }

        let state_tree_models = state_trees::Entity::find()
            .filter(state_trees::Column::Level.eq(0))
            .all(setup.db_conn.as_ref())
            .await
            .unwrap();

        let hash_index_and_seq_tuples = state_tree_models
            .iter()
            .map(|x| {
                (
                    Hash::try_from(x.hash.clone()).unwrap(),
                    x.leaf_idx.unwrap_or(0) as u64,
                    x.seq,
                )
            })
            .collect::<Vec<_>>();

        println!(
            "hash_index_and_seq_tuples: {:#?}",
            hash_index_and_seq_tuples
        );
        leaf_nodes = de_duplicated_leaf_nodes;
        let proofs = get_multiple_compressed_leaf_proofs(
            &setup.db_conn.begin().await.unwrap(),
            leaf_nodes
                .iter()
                .map(|x| Hash::try_from(x.hash.clone()).unwrap())
                .collect(),
        )
        .await
        .unwrap();

        let proof_hashes: HashSet<Hash> = proofs.iter().map(|x| x.hash.clone()).collect();
        let leaf_hashes: HashSet<Hash> = leaf_nodes.iter().map(|x| x.hash.clone()).collect();
        assert_eq!(
            proof_hashes, leaf_hashes,
            "Proof hashes should match leaf hashes"
        );

        let max_seq = leaf_nodes
            .iter()
            .map(|x| x.seq.unwrap_or_default())
            .max()
            .unwrap_or(0) as u64;

        for proof in proofs {
            assert_eq!(proof.merkle_tree, tree, "Merkle tree should match");
            assert_eq!(
                max_seq, proof.root_seq,
                "Root sequence should be the maximum sequence number"
            );
            assert_eq!(
                tree_height - 1,
                proof.proof.len() as u32,
                "Proof length should be tree height minus one"
            );
        }
        validate_tree(&setup.db_conn, tree).await;
    }
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_update_indexed_merkle_tree(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use itertools::Itertools;
    use photon_indexer::ingester::parser::{
        indexer_events::RawIndexedElement, state_update::IndexedTreeLeafUpdate,
    };
    let name = trim_test_name(function_name!());
    let setup = setup(name.clone(), db_backend).await;
    let tree = Pubkey::new_unique();
    let index = 1;
    let value = [1; 32];
    let index_element_1 = RawIndexedElement {
        value,
        next_index: 3,
        next_value: [2; 32],
        index,
    };
    let index_element_2 = RawIndexedElement {
        value,
        next_index: 4,
        next_value: [7; 32],
        index,
    };
    let parameters = [(index_element_1, 0), (index_element_2, 1)];
    for permutation in parameters.iter().permutations(2) {
        let txn = setup.db_conn.as_ref().begin().await.unwrap();
        for (indexed_element, seq) in permutation {
            let mut indexed_leaf_updates = HashMap::new();
            indexed_leaf_updates.insert(
                (tree, index as u64),
                IndexedTreeLeafUpdate {
                    tree,
                    leaf: *indexed_element,
                    hash: Hash::new_unique().into(), // HACK: We don't care about the hash
                    seq: *seq as u64,
                },
            );
            update_indexed_tree_leaves_v1(&txn, indexed_leaf_updates)
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();
        let tree_model = indexed_trees::Entity::find()
            .filter(
                indexed_trees::Column::Tree
                    .eq(tree.to_bytes().to_vec())
                    .and(indexed_trees::Column::LeafIndex.eq(1)),
            )
            .one(setup.db_conn.as_ref())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tree_model.value, index_element_2.value);
        assert_eq!(tree_model.next_value, index_element_2.next_value);
        assert_eq!(tree_model.next_index, index_element_2.next_index as i64);
        assert_eq!(tree_model.seq, Some(1i64));
    }
}
