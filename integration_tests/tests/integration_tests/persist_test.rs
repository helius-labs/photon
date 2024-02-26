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
use parser::bundle::{ChangelogEvent, PathNode, PublicStateTransitionBundle, UTXOEvent};
use persist::persist_bundle;
use solana_sdk::{pubkey::Pubkey, signature::Signature};

use crate::utils::{mock_str_to_hash, setup, TestSetup};

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
async fn persist_state_transitions() {
    let TestSetup { db_conn, api } = setup().await;
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
            hash: hash_v1,
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
                    hash: hash_v1,
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
    persist_bundle(&db_conn, bundle.into()).await.unwrap();

    // Verify GetCompressedAccount
    let res = api
        .get_compressed_account(GetCompressedAccountRequest {
            hash: Some(bs58::encode(hash_v1.to_vec()).into_string()),
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

    let account_lookup = api
        .get_compressed_account(GetCompressedAccountRequest {
            account_id: account.map(|a| bs58::encode(a).into_string()),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account_lookup, res_clone);

    // Verify GetCompressedAccountProof
    let GetCompressedAccountProofResponse { root, proof, hash } = api
        .get_compressed_account_proof(GetCompressedAccountProofRequest {
            hash: Some(bs58::encode(hash_v1.to_vec()).into_string()),
            ..Default::default()
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hash, bs58::encode(hash_v1.to_vec()).into_string());
    assert_eq!(proof.len(), 2);
    assert_eq!(
        root,
        bs58::encode(mock_str_to_hash("hash_v1_level_2").to_vec()).into_string()
    );
}
