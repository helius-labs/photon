use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use api::{
    api::ApiContract,
    method::{
        get_compressed_account::GetCompressedAccountRequest,
        get_compressed_account_proof::{
            GetCompressedAccountProofRequest, GetCompressedAccountProofResponse,
        },
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

    let token_tlv_data = TokenTlvData {
        mint: Pubkey::new_unique(),
        owner: Pubkey::new_unique(),
        amount: 100,
        delegate: Some(Pubkey::new_unique()),
        state: AccountState::Frozen,
        is_native: Some(1000),
        delegated_amount: 0,
        close_authority: Some(Pubkey::new_unique()),
    };
    persist_token_data(&setup.db_conn, token_tlv_data)
        .await
        .unwrap();
}
