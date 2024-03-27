use crate::utils::*;
use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use function_name::named;
use light_merkle_tree_event::{ChangelogEvent, ChangelogEventV1, Changelogs, PathNode};
use photon::api::error::PhotonApiError;
use photon::api::method::get_compressed_program_accounts::GetCompressedProgramAccountsRequest;
use photon::api::method::get_multiple_compressed_accounts::GetMultipleCompressedAccountsRequest;
use photon::api::method::utils::{CompressedAccountRequest, GetCompressedTokenAccountsByAuthority};
use photon::dao::generated::utxos;
use photon::dao::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};
use photon::ingester::index_block;
use photon::ingester::parser::bundle::PublicTransactionEventBundle;
use photon::ingester::persist::state_update::{EnrichedPathNode, UtxoWithSlot};
use photon::ingester::persist::state_update::{EnrichedUtxo, StateUpdate};
use photon::ingester::persist::{persist_state_update, persist_token_datas, EnrichedTokenData};
use photon::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use psp_compressed_pda::{
    tlv::{Tlv, TlvDataElement},
    utxo::Utxo,
};
use psp_compressed_token::AccountState;
use psp_compressed_token::TokenTlvData;
use sea_orm::{EntityTrait, Set};
use serial_test::serial;

use solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::vec;

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug, Clone)]
struct Person {
    name: String,
    age: u64,
}

// TODO:
// - Add tests for duplicate inserts.
// - Add tests for UTXO input spends without existing UTXO.
// - Add test for multi-input/output transitions.

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_persist_state_transitions(
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

    let owner = Pubkey::new_unique();
    let person = Person {
        name: "Alice".to_string(),
        age: 20,
    };
    let person_tlv = Tlv {
        tlv_elements: vec![TlvDataElement {
            discriminator: [0; 8],
            owner,
            data: to_vec(&person).unwrap(),
            data_hash: [0; 32],
        }],
    };
    let tree = Pubkey::new_unique();
    let utxo = Utxo {
        data: Some(person_tlv.clone()),
        owner,
        blinding: [0; 32],
        lamports: 1000,
    };

    let hash = utxo.hash();
    let slot = 123;

    let bundle = PublicTransactionEventBundle {
        in_utxos: vec![],
        out_utxos: vec![utxo.clone()],
        changelogs: Changelogs {
            changelogs: vec![ChangelogEvent::V1(ChangelogEventV1 {
                id: tree.to_bytes(),
                paths: vec![vec![
                    PathNode {
                        node: hash,
                        index: 4,
                    },
                    PathNode {
                        node: mock_str_to_hash("hash_v1_level_1").into(),
                        index: 2,
                    },
                    PathNode {
                        node: mock_str_to_hash("hash_v1_level_2").into(),
                        index: 1,
                    },
                ]],
                seq: 0,
                index: 0,
            })],
        },
        transaction: Signature::new_unique(),
        slot,
    };
    persist_bundle_using_connection(&setup.db_conn, bundle.into())
        .await
        .unwrap();

    let request = CompressedAccountRequest {
        address: None,
        hash: Some(Hash::from(hash)),
    };

    let res = setup
        .api
        .get_compressed_account(request.clone())
        .await
        .unwrap()
        .value;

    #[allow(deprecated)]
    let raw_data = base64::decode(res.data).unwrap();
    assert_eq!(person_tlv, Tlv::try_from_slice(&raw_data).unwrap());
    assert_eq!(res.lamports, utxo.lamports);
    assert_eq!(res.slot_updated, slot);

    let res = setup
        .api
        .get_compressed_balance(request)
        .await
        .unwrap()
        .value;
    assert_eq!(res, utxo.lamports as i64);

    // Assert that we get an error if we input a non-existent UTXO.
    // TODO: Test spent utxos
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

    let owner1 = Pubkey::new_unique();
    let owner2 = Pubkey::new_unique();
    let mut state_update = StateUpdate::default();

    let utxos = vec![
        Utxo {
            data: Some(Tlv {
                tlv_elements: vec![TlvDataElement {
                    discriminator: [0; 8],
                    owner: owner1,
                    data: vec![1; 500],
                    data_hash: [1; 32],
                }],
            }),
            owner: owner1,
            blinding: [0; 32],
            lamports: 5,
        },
        Utxo {
            data: Some(Tlv {
                tlv_elements: vec![TlvDataElement {
                    discriminator: [0; 8],
                    owner: owner1,
                    data: vec![2; 500],
                    data_hash: [2; 32],
                }],
            }),
            owner: owner1,
            blinding: [2; 32],
            lamports: 10,
        },
        Utxo {
            data: Some(Tlv {
                tlv_elements: vec![TlvDataElement {
                    discriminator: [0; 8],
                    owner: owner2,
                    data: vec![3; 500],
                    data_hash: [3; 32],
                }],
            }),
            owner: owner2,
            blinding: [3; 32],
            lamports: 20,
        },
        Utxo {
            data: Some(Tlv {
                tlv_elements: vec![TlvDataElement {
                    discriminator: [0; 8],
                    owner: owner2,
                    data: vec![4; 500],
                    data_hash: [4; 32],
                }],
            }),
            owner: owner2,
            blinding: [4; 32],
            lamports: 30,
        },
    ];

    let arbitrary_multiplier = 10;
    let enriched_utxos: Vec<EnrichedUtxo> = utxos
        .iter()
        .enumerate()
        .map(|(i, utxo)| EnrichedUtxo {
            utxo: UtxoWithSlot {
                utxo: utxo.clone(),
                slot: (i * arbitrary_multiplier) as i64,
            },
            tree: Pubkey::new_unique().to_bytes(),
            seq: i as i64,
        })
        .collect();

    state_update.out_utxos = enriched_utxos.clone();
    let txn = sea_orm::TransactionTrait::begin(setup.db_conn.as_ref())
        .await
        .unwrap();
    persist_state_update(&txn, state_update).await.unwrap();
    txn.commit().await.unwrap();

    for owner in [owner1, owner2] {
        let mut res = setup
            .api
            .get_compressed_program_accounts(GetCompressedProgramAccountsRequest(
                SerializablePubkey::from(owner),
                None,
            ))
            .await
            .unwrap()
            .value;

        let mut utxos = enriched_utxos
            .clone()
            .into_iter()
            .filter(|x| x.utxo.utxo.owner == owner)
            .collect::<Vec<EnrichedUtxo>>();

        assert_utxo_response_list_matches_input(&mut res.items, &mut utxos);
    }

    let mut utxos_of_interest = vec![enriched_utxos[0].clone(), enriched_utxos[2].clone()];
    let mut res = setup
        .api
        .get_multiple_compressed_accounts(GetMultipleCompressedAccountsRequest {
            addresses: None,
            hashes: Some(
                utxos_of_interest
                    .iter()
                    .map(|x| x.utxo.utxo.hash().into())
                    .collect(),
            ),
        })
        .await
        .unwrap()
        .value;

    assert_utxo_response_list_matches_input(&mut res.items, &mut utxos_of_interest);
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

    let token_tlv_data1: TokenTlvData = TokenTlvData {
        mint: mint1,
        owner: owner1,
        amount: 1,
        delegate: Some(delegate1),
        state: AccountState::Frozen,
        is_native: Some(1),
        delegated_amount: 1,
    };

    let token_tlv_data2: TokenTlvData = TokenTlvData {
        mint: mint2,
        owner: owner1,
        amount: 2,
        delegate: Some(delegate2),
        state: AccountState::Initialized,
        is_native: None,
        delegated_amount: 2,
    };

    let token_tlv_data3: TokenTlvData = TokenTlvData {
        mint: mint3,
        owner: owner2,
        amount: 3,
        delegate: Some(delegate1),
        state: AccountState::Frozen,
        is_native: Some(1000),
        delegated_amount: 3,
    };
    let all_token_tlv_data = vec![token_tlv_data1, token_tlv_data2, token_tlv_data3];

    let txn = sea_orm::TransactionTrait::begin(setup.db_conn.as_ref())
        .await
        .unwrap();

    let mut token_datas = Vec::new();

    for token_tlv_data in all_token_tlv_data.iter() {
        let slot = 11;
        let hash = Hash::new_unique();
        let model = utxos::ActiveModel {
            hash: Set(hash.clone().into()),
            account: Set(Some(Pubkey::new_unique().to_bytes().to_vec())),
            spent: Set(false),
            data: Set(to_vec(&token_tlv_data).unwrap()),
            owner: Set(token_tlv_data.owner.to_bytes().to_vec()),
            lamports: Set(10),
            slot_updated: Set(slot),
            ..Default::default()
        };
        utxos::Entity::insert(model).exec(&txn).await.unwrap();
        token_datas.push(EnrichedTokenData {
            hash,
            token_tlv_data: *token_tlv_data,
            slot_updated: slot,
        });
    }

    persist_token_datas(&txn, token_datas).await.unwrap();
    txn.commit().await.unwrap();

    for owner in [owner1, owner2] {
        let owner_tlv = all_token_tlv_data
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
        let delegate_tlv = all_token_tlv_data
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

        verify_responses_match_tlv_data(res, delegate_tlv)
    }
}

#[named]
#[rstest]
#[tokio::test(flavor = "multi_thread", worker_threads = 25)]
#[serial]
#[ignore]
/// Test for testing how fast we can index UTXOs.
async fn test_load_test(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;

    fn generate_random_utxo(tree: Pubkey, seq: i64) -> EnrichedUtxo {
        EnrichedUtxo {
            utxo: UtxoWithSlot {
                utxo: Utxo {
                    data: Some(Tlv {
                        tlv_elements: vec![TlvDataElement {
                            discriminator: [0; 8],
                            owner: Pubkey::new_unique(),
                            data: vec![1; 500],
                            data_hash: [0; 32],
                        }],
                    }),
                    owner: Pubkey::new_unique(),
                    blinding: [0; 32],
                    lamports: 1000,
                },
                slot: 0,
            },
            tree: tree.to_bytes(),
            seq,
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
            seq,
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
            in_utxos: vec![],
            out_utxos: (0..num_elements)
                .map(|i| generate_random_utxo(tree, i))
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
