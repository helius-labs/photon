use crate::utils::*;
use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use function_name::named;
use light_merkle_tree_event::{ChangelogEvent, ChangelogEventV1, Changelogs, PathNode};
use photon::api::method::get_compressed_token_accounts_by_delegate::GetCompressedTokenAccountsByDelegateRequest;
use photon::api::{
    error::PhotonApiError,
    method::{
        get_compressed_token_accounts_by_owner::GetCompressedTokenAccountsByOwnerRequest,
        get_utxo::GetUtxoRequest,
    },
};
use photon::dao::generated::utxos;
use photon::dao::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};
use photon::ingester::parser::bundle::PublicTransactionEventBundle;
use photon::ingester::persist::state_update::{EnrichedPathNode, UtxoWithSlot};
use photon::ingester::persist::state_update::{EnrichedUtxo, StateUpdate};
use photon::ingester::persist::{persist_state_update, persist_token_datas, EnrichedTokenData};
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
// - Replace the test data with transactions generated locally via the new contracts.
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
    let owner = Pubkey::new_unique();
    let person = Person {
        name: "Alice".to_string(),
        age: 20,
    };
    let person_tlv = Tlv {
        tlv_elements: vec![TlvDataElement {
            discriminator: [0; 8],
            owner: owner,
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
                        node: hash.clone().into(),
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
        slot: slot,
    };
    persist_bundle_using_connection(&setup.db_conn, bundle.into())
        .await
        .unwrap();

    // Verify GetUtxo
    let res = setup
        .api
        .get_utxo(GetUtxoRequest {
            hash: Hash::from(hash.clone()),
        })
        .await
        .unwrap();

    #[allow(deprecated)]
    let raw_data = base64::decode(res.data).unwrap();
    assert_eq!(person_tlv, Tlv::try_from_slice(&raw_data).unwrap());
    assert_eq!(res.lamports, utxo.lamports);
    assert_eq!(res.slot_updated, slot as u64);

    // Assert that we get an error if we input a non-existent UTXO.
    // TODO: Test spent utxos
    let err = setup
        .api
        .get_utxo(GetUtxoRequest {
            hash: Hash::from(Pubkey::new_unique().to_bytes()),
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

    let token_tlv_data1: TokenTlvData = TokenTlvData {
        mint: mint1.clone(),
        owner: owner1.clone(),
        amount: 1,
        delegate: Some(delegate1),
        state: AccountState::Frozen,
        is_native: Some(1),
        delegated_amount: 1,
    };

    let token_tlv_data2: TokenTlvData = TokenTlvData {
        mint: mint2.clone(),
        owner: owner1.clone(),
        amount: 2,
        delegate: Some(delegate2),
        state: AccountState::Initialized,
        is_native: None,
        delegated_amount: 2,
    };

    let token_tlv_data3: TokenTlvData = TokenTlvData {
        mint: mint3.clone(),
        owner: owner2.clone(),
        amount: 3,
        delegate: Some(delegate1),
        state: AccountState::Frozen,
        is_native: Some(1000),
        delegated_amount: 3,
    };
    let all_token_tlv_data = vec![
        token_tlv_data1.clone(),
        token_tlv_data2.clone(),
        token_tlv_data3.clone(),
    ];

    let txn = sea_orm::TransactionTrait::begin(setup.db_conn.as_ref())
        .await
        .unwrap();

    let mut token_datas = Vec::new();

    for token_tlv_data in all_token_tlv_data.iter() {
        let slot = 11;
        let hash = Hash::new_unique();
        let model = utxos::ActiveModel {
            hash: Set(hash.clone().into()),
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
            token_tlv_data: token_tlv_data.clone(),
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
            .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwnerRequest {
                owner: SerializablePubkey::from(owner.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        verify_responses_match_tlv_data(res, owner_tlv)
    }
    for delegate in [delegate1, delegate2] {
        let delegate_tlv = all_token_tlv_data
            .clone()
            .into_iter()
            .filter(|x| x.delegate == Some(delegate))
            .collect();
        let res = setup
            .api
            .get_compressed_token_accounts_by_delegate(
                GetCompressedTokenAccountsByDelegateRequest {
                    delegate: SerializablePubkey::from(delegate.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

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
                .map(|i| generate_random_utxo(tree.clone(), i))
                .collect(),
            // We only include the leaf index because we think the most path nodes will be
            // overwritten anyways. So the amortized number of writes will be in each tree
            // will be close to 1.
            path_nodes: (0..num_elements)
                .map(|i| generate_random_leaf_index(tree.clone(), i as u32, i))
                .collect(),
        };
        persist_state_update(&txn, state_update).await.unwrap();
        txn.commit().await.unwrap();
    }
}
