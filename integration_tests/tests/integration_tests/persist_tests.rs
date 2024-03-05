use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use api::{
    api::ApiContract,
    method::{
        get_compressed_account::GetCompressedAccountRequest,
        get_compressed_account_proof::{
            GetCompressedAccountProofRequest, GetCompressedAccountProofResponse,
        },
        get_compressed_token_accounts_by_owner::GetCompressedTokenInfoByOwnerRequest,
    },
};
use dao::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};
use function_name::named;
use ingester::persist::persist_bundle;
use ingester::{
    parser::bundle::{
        AccountState, ChangelogEvent, PathNode, PublicStateTransitionBundle, TokenTlvData,
        UTXOEvent,
    },
    persist::persist_token_data,
};
use serial_test::serial;
use solana_sdk::{pubkey::Pubkey, signature::Signature};

use crate::utils::*;

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

#[tokio::test]
#[serial]
#[named]
async fn test_persist_state_transitions() {
    let name = trim_test_name(function_name!());
    let setup = setup(name).await;
    let owner = Pubkey::new_unique();
    let person = Person {
        name: "Alice".to_string(),
        age: 20,
    };
    let program = Pubkey::new_unique();
    let tree = Pubkey::new_unique();
    let account = Some(
        Pubkey::find_program_address(&["person".as_bytes(), person.name.as_bytes()], &program).0,
    );
    let blinding = mock_str_to_hash("blinding");
    let hash_v1 = mock_str_to_hash("person_v1");
    let bundle = PublicStateTransitionBundle {
        in_utxos: vec![],
        out_utxos: vec![UTXOEvent {
            hash: hash_v1.clone(),
            data: to_vec(&person.clone()).unwrap(),
            owner,
            blinding,
            account,
            tree,
            seq: 0,
            lamports: Some(5000),
        }],
        changelogs: vec![ChangelogEvent {
            tree,
            seq: 0,
            path: vec![
                PathNode {
                    index: 4,
                    hash: hash_v1.clone(),
                },
                PathNode {
                    index: 2,
                    hash: mock_str_to_hash("hash_v1_level_1"),
                },
                PathNode {
                    index: 1,
                    hash: mock_str_to_hash("hash_v1_level_2"),
                },
            ],
        }],
        transaction: Signature::new_unique(),
        slot_updated: 0,
    };
    persist_bundle(&setup.db_conn, bundle.into()).await.unwrap();

    // Verify GetCompressedAccount
    let res = setup
        .api
        .get_compressed_account(GetCompressedAccountRequest {
            hash: Some(Hash::from(hash_v1.clone())),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    let res_clone = res.clone();

    #[allow(deprecated)]
    let raw_data = base64::decode(res.data).unwrap();
    let parsed = Person::try_from_slice(&raw_data).unwrap();
    assert_eq!(parsed, person);
    assert_eq!(res.lamports, Some(5000));

    let account_lookup = setup
        .api
        .get_compressed_account(GetCompressedAccountRequest {
            account_id: account.map(SerializablePubkey::from),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account_lookup, res_clone);

    // Verify GetCompressedAccountProof
    let GetCompressedAccountProofResponse { root, proof, hash } = setup
        .api
        .get_compressed_account_proof(GetCompressedAccountProofRequest {
            hash: Some(hash_v1.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hash, hash_v1);
    assert_eq!(proof.len(), 2);
    assert_eq!(root, mock_str_to_hash("hash_v1_level_2"));
}

#[tokio::test]
#[serial]
#[named]
async fn test_persist_token_data() {
    let name = trim_test_name(function_name!());
    let setup = setup(name).await;
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
