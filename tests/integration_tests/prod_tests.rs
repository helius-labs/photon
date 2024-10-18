use std::{str::FromStr, sync::Arc};

use function_name::named;
use insta::assert_json_snapshot;
use itertools::Itertools;
use photon_indexer::{
    api::{
        api::PhotonApi,
        method::{
            get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest,
            get_multiple_new_address_proofs::{AddressListWithTrees, AddressWithTree},
        },
    },
    common::typedefs::{
        rpc_client_with_uri::RpcClientWithUri, serializable_pubkey::SerializablePubkey,
    },
    dao::generated::state_tree_histories,
    ingester::{
        parser::{parse_transaction, state_update::StateUpdate},
        typedefs::block_info::TransactionInfo,
    },
    monitor::start_latest_slot_updater,
};
use solana_client::{
    rpc_client::GetConfirmedSignaturesForAddress2Config, rpc_config::RpcTransactionConfig,
};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use solana_transaction_status::UiTransactionEncoding;

use crate::utils::*;
use sea_orm::{EntityTrait, QueryFilter, SqlxPostgresConnector};
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
    let rpc_client = Arc::new(RpcClientWithUri::new(
        "https://api.devnet.solana.com".to_string(),
    ));
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
    let rpc_client = Arc::new(RpcClientWithUri::new(
        "https://api.mainnet-beta.solana.com".to_string(),
    ));
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

#[tokio::test]
#[serial]
#[ignore]
#[named]
async fn test_reindex_messed_up_tree() {
    use futures::StreamExt;

    let db_url = std::env::var("READONLY_MAINNET_FRA_DB_URL").unwrap();
    let pool = setup_pg_pool(db_url.to_string()).await;
    let db = Arc::new(SqlxPostgresConnector::from_sqlx_postgres_pool(pool));
    let rpc_client = Arc::new(RpcClientWithUri::new("".to_string()));
    let tree = Pubkey::try_from("amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2").unwrap();
    let mut signatures = vec![];
    let mut before = None;

    loop {
        let txns = rpc_client
            .client
            .get_signatures_for_address_with_config(
                &tree,
                GetConfirmedSignaturesForAddress2Config {
                    before,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        if txns.is_empty() {
            break;
        }
        println!("Number of signatures: {}", signatures.len());
        let txns = txns
            .into_iter()
            .map(|x| Signature::from_str(&x.signature).unwrap())
            .collect_vec();
        let last_signature = txns.last().unwrap();
        before = Some(last_signature.clone());
        signatures.extend(txns);
        break;
    }
    let txn_stream = async_stream::stream! {
        for signature in signatures {
            yield signature;
        }
    };
    println!("Finished fetching signatures");

    let txns: Vec<(TransactionInfo, u64)> = txn_stream
        .map(|x| {
            let rpc_client = rpc_client.clone();
            async move {
                let txn = rpc_client
                    .client
                    .get_transaction_with_config(
                        &x,
                        RpcTransactionConfig {
                            encoding: Some(UiTransactionEncoding::Base58),
                            max_supported_transaction_version: Some(0),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                let slot = txn.slot;
                let txn: TransactionInfo = txn.try_into().unwrap();
                (txn, slot)
            }
        })
        .buffer_unordered(200)
        .collect()
        .await;
    let txns = txns.into_iter().rev().collect_vec();
    println!("Finished fetching transactions");
    let state_update = StateUpdate::merge_updates(
        txns.into_iter()
            .map(|(txn, slot)| parse_transaction(&txn, slot).unwrap())
            .collect_vec(),
    );
    persist_state_update_using_connection(&db, state_update)
        .await
        .unwrap();
}
