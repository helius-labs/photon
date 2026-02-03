use std::str::FromStr;
use std::sync::Arc;

use function_name::named;
use insta::assert_json_snapshot;
use photon_indexer::{
    api::{
        api::PhotonApi,
        method::{
            get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest,
            get_multiple_new_address_proofs::{AddressListWithTrees, AddressWithTree},
            get_transaction_with_compression_info::GetTransactionRequest,
        },
    },
    common::{
        get_rpc_client,
        typedefs::{
            serializable_pubkey::SerializablePubkey,
            serializable_signature::SerializableSignature,
        },
    },
};
use solana_signature::Signature;

use crate::utils::*;
use sea_orm::SqlxPostgresConnector;
use serial_test::serial;

#[tokio::test]
#[serial]
#[ignore]
#[named]
async fn test_incorrect_root_bug() {
    let name = trim_test_name(function_name!());

    let readonly_devnet_db_url = std::env::var("READONLY_DEVNET_DB_URL").unwrap();
    let pool = setup_pg_pool(readonly_devnet_db_url.to_string()).await;
    let devnet_db = Arc::new(SqlxPostgresConnector::from_sqlx_postgres_pool(pool));
    let rpc_client = get_rpc_client("https://api.devnet.solana.com");
    let prover_url = "http://localhost:3001";
    let api = PhotonApi::new(devnet_db.clone(), rpc_client, prover_url.to_string());

    let response = api
        .get_compressed_accounts_by_owner(GetCompressedAccountsByOwnerRequest {
            owner: SerializablePubkey::try_from("11111116EPqoQskEM2Pddp8KTL9JdYEBZMGF3aq7V")
                .unwrap(),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[ignore]
#[named]
async fn test_mainnet_fra_invalid_address_tree_bug() {
    let name = trim_test_name(function_name!());

    let readonly_devnet_db_url = std::env::var("READONLY_MAINNET_FRA_DB_URL").unwrap();
    let pool = setup_pg_pool(readonly_devnet_db_url.to_string()).await;
    let devnet_db = Arc::new(SqlxPostgresConnector::from_sqlx_postgres_pool(pool));
    let rpc_client = get_rpc_client("https://api.mainnet-beta.solana.com");
    let prover_url = "http://localhost:3001";
    let api = PhotonApi::new(devnet_db.clone(), rpc_client, prover_url.to_string());

    let response = api
        .get_multiple_new_address_proofs_v2(AddressListWithTrees(vec![AddressWithTree {
            address: SerializablePubkey::try_from("13VVFAQtRomFvVHw3cgcse2BJWtLFpf1gCQDDCD5JjNV")
                .unwrap(),
            tree: SerializablePubkey::try_from("amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2")
                .unwrap(),
        }]))
        .await
        .unwrap();

    assert_json_snapshot!(name, response);
}

/// Test for NULL queue field fix in getTransactionWithCompressionInfoV2
/// This transaction was timing out because an account had a NULL queue field
/// which caused ParsePubkeyError when converting empty Vec<u8> to SerializablePubkey
#[tokio::test]
#[serial]
#[ignore]
#[named]
async fn test_mainnet_pitt_null_queue_transaction() {
    let name = trim_test_name(function_name!());

    let readonly_mainnet_pitt_db_url = std::env::var("READONLY_MAINNET_PITT_DB_URL").unwrap();
    let pool = setup_pg_pool(readonly_mainnet_pitt_db_url.to_string()).await;
    let mainnet_db = Arc::new(SqlxPostgresConnector::from_sqlx_postgres_pool(pool));
    let rpc_url = std::env::var("MAINNET_RPC_URL")
        .unwrap_or_else(|_| "https://mainnet.helius-rpc.com?api-key=987e3556-4ed4-4e5d-813d-3234bcd97d05".to_string());
    let rpc_client = get_rpc_client(&rpc_url);
    let prover_url = "http://localhost:3001";
    let api = PhotonApi::new(mainnet_db.clone(), rpc_client, prover_url.to_string());

    // This transaction was causing "Invalid public key in database" error
    // due to NULL queue field being converted to empty Vec<u8>
    let signature = SerializableSignature(
        Signature::from_str(
            "55Any7LBL6bGwWJRBGD7EFYs9nJkjgzWPwf6hWwDE8LLvbXjNDiQiK7yev9igwHjGUugiXG67BGmfgtWSR5pBJKP",
        )
        .unwrap(),
    );

    let response = api
        .get_transaction_with_compression_info_v2(GetTransactionRequest { signature })
        .await;

    // With the fix, this should not return an "Invalid public key in database" error
    // It may return a different error (e.g., if transaction doesn't have compression info)
    // but it should NOT fail with ParsePubkeyError
    match &response {
        Ok(_) => {
            // Success - the fix worked
            assert_json_snapshot!(name, response.unwrap());
        }
        Err(e) => {
            let error_msg = format!("{:?}", e);
            assert!(
                !error_msg.contains("Invalid public key in database"),
                "Fix failed: still getting 'Invalid public key in database' error: {}",
                error_msg
            );
            // Other errors are acceptable (e.g., transaction not found, no compression info)
            println!("Got expected non-pubkey error: {}", error_msg);
        }
    }
}

/// Test for NULL queue field fix in getTransactionWithCompressionInfo (v1)
/// Same fix as v2 - both use AccountWithContext which had the bug
#[tokio::test]
#[serial]
#[ignore]
#[named]
async fn test_mainnet_pitt_null_queue_transaction_v1() {
    let name = trim_test_name(function_name!());

    let readonly_mainnet_pitt_db_url = std::env::var("READONLY_MAINNET_PITT_DB_URL").unwrap();
    let pool = setup_pg_pool(readonly_mainnet_pitt_db_url.to_string()).await;
    let mainnet_db = Arc::new(SqlxPostgresConnector::from_sqlx_postgres_pool(pool));
    let rpc_url = std::env::var("MAINNET_RPC_URL")
        .unwrap_or_else(|_| "https://mainnet.helius-rpc.com?api-key=987e3556-4ed4-4e5d-813d-3234bcd97d05".to_string());
    let rpc_client = get_rpc_client(&rpc_url);
    let prover_url = "http://localhost:3001";
    let api = PhotonApi::new(mainnet_db.clone(), rpc_client, prover_url.to_string());

    // Same transaction as v2 test
    let signature = SerializableSignature(
        Signature::from_str(
            "55Any7LBL6bGwWJRBGD7EFYs9nJkjgzWPwf6hWwDE8LLvbXjNDiQiK7yev9igwHjGUugiXG67BGmfgtWSR5pBJKP",
        )
        .unwrap(),
    );

    let response = api
        .get_transaction_with_compression_info(GetTransactionRequest { signature })
        .await;

    match &response {
        Ok(_) => {
            assert_json_snapshot!(name, response.unwrap());
        }
        Err(e) => {
            let error_msg = format!("{:?}", e);
            assert!(
                !error_msg.contains("Invalid public key in database"),
                "Fix failed: still getting 'Invalid public key in database' error: {}",
                error_msg
            );
            println!("Got expected non-pubkey error: {}", error_msg);
        }
    }
}
