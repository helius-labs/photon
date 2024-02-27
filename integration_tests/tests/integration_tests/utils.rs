use std::{
    env,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Mutex,
    time::Duration,
};

use api::api::{PhotonApi, PhotonApiConfig};
use ingester::{
    parser::bundle::Hash, VersionedConfirmedTransactionWithUiStatusMeta,
    VersionedTransactionWithUiStatusMeta,
};
use log::{error, info};
use migration::{Migrator, MigratorTrait};
use once_cell::sync::Lazy;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, DbErr, ExecResult, SqlxPostgresConnector,
    Statement,
};
use serde::Serialize;
use solana_client::{
    client_error::{reqwest::Version, ClientError},
    nonblocking::rpc_client::RpcClient,
    rpc_config::RpcTransactionConfig,
    rpc_request::RpcRequest,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionStatusMeta, UiTransactionEncoding,
    UiTransactionStatusMeta, VersionedConfirmedTransactionWithStatusMeta,
    VersionedTransactionWithStatusMeta,
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use tokio::time::sleep;

static INIT: Lazy<Mutex<Option<()>>> = Lazy::new(|| Mutex::new(None));

fn setup_logging() {
    // let env_filter = env::var("RUST_LOG").unwrap_or("debug,sqlx::off".to_string());
    // tracing_subscriber::fmt()
    //     .with_test_writer()
    //     .with_env_filter(env_filter)
    //     .init();
}

async fn run_migrations_from_fresh(db: &DatabaseConnection) {
    std::env::set_var("INIT_FILE_PATH", "../init.sql");
    Migrator::fresh(db).await.unwrap();
}

async fn run_one_time_setup(db: &DatabaseConnection) {
    let mut init = INIT.lock().unwrap();
    if init.is_none() {
        setup_logging();
        run_migrations_from_fresh(db).await;
        *init = Some(());
        return;
    }
}

pub struct TestSetup {
    pub db_conn: DatabaseConnection,
    pub api: PhotonApi,
    pub name: String,
    pub client: RpcClient,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum Network {
    #[default]
    Mainnet,
    Devnet,
    // Localnet is not a great test option since transactions are not persisted but we don't know
    // how to deploy everything into devnet yet.
    Localnet,
}

#[derive(Clone, Copy, Default)]
pub struct TestSetupOptions {
    pub network: Network,
}

pub async fn setup_with_options(name: String, opts: TestSetupOptions) -> TestSetup {
    let local_db = "postgres://postgres@localhost/postgres";
    let pool = setup_pg_pool(local_db.to_string()).await;
    let db_conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool);
    run_one_time_setup(&db_conn).await;
    reset_tables(&db_conn).await.unwrap();

    let api = PhotonApi::new(PhotonApiConfig {
        max_conn: 1,
        timeout_seconds: 15,
        db_url: local_db.to_string(),
    })
    .await
    .map_err(|e| panic!("Failed to setup Photon API: {}", e))
    .unwrap();

    let rpc_url = match opts.network {
        Network::Mainnet => std::env::var("MAINNET_RPC_URL").unwrap(),
        Network::Devnet => std::env::var("DEVNET_RPC_URL").unwrap(),
        Network::Localnet => "http://127.0.0.1:8899".to_string(),
    };
    let client = RpcClient::new(rpc_url.to_string());

    TestSetup {
        name,
        db_conn,
        api,
        client,
    }
}

pub async fn setup(name: String) -> TestSetup {
    setup_with_options(name, TestSetupOptions::default()).await
}

pub async fn setup_pg_pool(database_url: String) -> PgPool {
    let options: PgConnectOptions = database_url.parse().unwrap();
    PgPoolOptions::new()
        .min_connections(1)
        .connect_with(options)
        .await
        .unwrap()
}

pub async fn reset_tables(conn: &DatabaseConnection) -> Result<(), DbErr> {
    for table in vec!["state_trees", "utxos"] {
        truncate_table(conn, table.to_string()).await?;
    }
    Ok(())
}

pub async fn truncate_table(conn: &DatabaseConnection, table: String) -> Result<ExecResult, DbErr> {
    conn.execute(Statement::from_string(
        DbBackend::Postgres,
        format!("TRUNCATE TABLE {} CASCADE", table),
    ))
    .await
}

pub fn mock_str_to_hash(input: &str) -> Hash {
    let mut array = [0u8; 32];
    let bytes = input.as_bytes();

    for (i, &byte) in bytes.iter().enumerate().take(32) {
        array[i] = byte;
    }

    Hash::new(array)
}

fn get_relative_project_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

pub async fn fetch_transaction(
    client: &RpcClient,
    sig: Signature,
) -> EncodedConfirmedTransactionWithStatusMeta {
    const CONFIG: RpcTransactionConfig = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        commitment: Some(CommitmentConfig {
            commitment: CommitmentLevel::Confirmed,
        }),
        max_supported_transaction_version: Some(0),
    };

    // We do not immediately deserialize because VersionedTransactionWithStatusMeta does not
    // implement Deserialize
    let txn: EncodedConfirmedTransactionWithStatusMeta = client
        .send(
            RpcRequest::GetTransaction,
            serde_json::json!([sig.to_string(), CONFIG,]),
        )
        .await
        .unwrap();

    // Ignore if tx failed or meta is missed
    let meta = txn.transaction.meta.as_ref();
    if meta.map(|meta| meta.status.is_err()).unwrap_or(true) {
        panic!("Trying to index failed transaction: {}", sig);
    }
    txn
}

async fn cached_fetch_transaction(
    setup: &TestSetup,
    sig: Signature,
) -> VersionedConfirmedTransactionWithUiStatusMeta {
    let dir = get_relative_project_path(&format!("tests/data/transactions/{}", setup.name));
    if !Path::new(&dir).exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    let file_path = dir.join(sig.to_string());

    let txn = if file_path.exists() {
        let txn_string = std::fs::read(file_path).unwrap();
        serde_json::from_slice(&txn_string).unwrap()
    } else {
        let txn = fetch_transaction(&setup.client, sig).await;
        std::fs::write(file_path, serde_json::to_string(&txn).unwrap()).unwrap();
        txn
    };

    VersionedConfirmedTransactionWithUiStatusMeta {
        tx_with_meta: VersionedTransactionWithUiStatusMeta {
            transaction: txn.transaction.transaction.decode().unwrap(),
            meta: txn.transaction.meta.unwrap(),
        },
        block_time: txn.block_time,
        slot: txn.slot,
    }
}

pub async fn index_transaction(setup: &TestSetup, txn: &str) {
    let sig = Signature::from_str(txn).unwrap();
    let txn = cached_fetch_transaction(setup, sig).await;
    ingester::index_transaction(&setup.db_conn, txn).await
}

pub async fn index_transactions(setup: &TestSetup, txns: &[&str]) {
    for txn in txns {
        index_transaction(setup, txn).await;
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Order {
    Forward,
    AllPermutations,
}

pub fn trim_test_name(name: &str) -> String {
    name.replace("test_", "")
}
