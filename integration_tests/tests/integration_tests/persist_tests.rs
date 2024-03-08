use crate::utils::*;
use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use api::{
    api::ApiContract,
    method::{
        get_compressed_account::GetCompressedAccountRequest,
        get_compressed_token_accounts_by_owner::GetCompressedTokenInfoByOwnerRequest,
    },
};
use dao::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};
use function_name::named;
use ingester::parser::bundle::PublicTransactionEventBundle;
use ingester::persist::persist_bundle;
use ingester::persist::persist_token_data;
use light_merkle_tree_event::{ChangelogEvent, ChangelogEventV1, Changelogs, PathNode};
use psp_compressed_pda::{
    tlv::{Tlv, TlvDataElement},
    utxo::Utxo,
};
use psp_compressed_token::AccountState;
use psp_compressed_token::TokenTlvData;
use serial_test::serial;
use solana_sdk::{pubkey::Pubkey, signature::Signature};

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
// - Replace assertions with API queries instead of direct DB queries.

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
        slot: 123,
    };
    persist_bundle(&setup.db_conn, bundle.into()).await.unwrap();

    // Verify GetCompressedAccount
    let res = setup
        .api
        .get_compressed_account(GetCompressedAccountRequest {
            hash: Some(Hash::from(hash.clone())),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();

    #[allow(deprecated)]
    let raw_data = base64::decode(res.data).unwrap();
    assert_eq!(person_tlv, Tlv::try_from_slice(&raw_data).unwrap());
    assert_eq!(res.lamports, utxo.lamports as i64);
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
    let owner1 = Pubkey::new_unique();
    let owner2 = Pubkey::new_unique();

    let token_tlv_data1: TokenTlvData = TokenTlvData {
        mint: mint1.clone(),
        owner: owner1.clone(),
        amount: 1,
        delegate: Some(Pubkey::new_unique()),
        state: AccountState::Frozen,
        is_native: Some(1),
        delegated_amount: 0,
        close_authority: Some(Pubkey::new_unique()),
    };

    let token_tlv_data2: TokenTlvData = TokenTlvData {
        mint: mint2.clone(),
        owner: owner1.clone(),
        amount: 2,
        delegate: Some(Pubkey::new_unique()),
        state: AccountState::Initialized,
        is_native: None,
        delegated_amount: 1,
        close_authority: None,
    };

    let token_tlv_data3: TokenTlvData = TokenTlvData {
        mint: mint1.clone(),
        owner: owner2.clone(),
        amount: 3,
        delegate: Some(Pubkey::new_unique()),
        state: AccountState::Frozen,
        is_native: Some(1000),
        delegated_amount: 0,
        close_authority: Some(Pubkey::new_unique()),
    };
    for token_tlv_data in vec![
        token_tlv_data1.clone(),
        token_tlv_data2.clone(),
        token_tlv_data3.clone(),
    ] {
        persist_token_data(&setup.db_conn, token_tlv_data.clone())
            .await
            .unwrap();
    }

    let res = setup
        .api
        .get_compressed_account_token_accounts_by_owner(GetCompressedTokenInfoByOwnerRequest {
            owner: SerializablePubkey::from(owner1.clone()),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(res.total, 2);

    let res = setup
        .api
        .get_compressed_account_token_accounts_by_owner(GetCompressedTokenInfoByOwnerRequest {
            owner: SerializablePubkey::from(owner1.clone()),
            mint: Some(SerializablePubkey::from(mint1.clone())),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(res.total, 1);
    let res = res.items[0].clone();
    assert_eq!(res.mint, mint1.into());
    assert_eq!(res.owner, owner1.into());
    assert_eq!(res.amount, token_tlv_data1.amount);
    assert_eq!(
        res.delegate,
        token_tlv_data1.delegate.map(SerializablePubkey::from)
    );
    assert_eq!(res.is_native, true);
    assert_eq!(
        res.close_authority,
        token_tlv_data1
            .close_authority
            .map(SerializablePubkey::from)
    );

    let res = setup
        .api
        .get_compressed_account_token_accounts_by_owner(GetCompressedTokenInfoByOwnerRequest {
            owner: SerializablePubkey::from(owner1.clone()),
            mint: Some(SerializablePubkey::from(mint2.clone())),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(res.total, 1);
    let res = res.items[0].clone();
    assert_eq!(res.mint, mint2.into());
    assert_eq!(res.owner, owner1.into());
    assert_eq!(res.amount, token_tlv_data2.amount);
    assert_eq!(
        res.delegate,
        token_tlv_data2.delegate.map(SerializablePubkey::from)
    );
    assert_eq!(res.is_native, false);
    assert_eq!(res.close_authority, None);
}
