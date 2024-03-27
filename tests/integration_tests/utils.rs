use std::{
    env,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Mutex,
};

use borsh::to_vec;
use photon::{
    api::{
        api::PhotonApi,
        method::{
            utils::{TokenAccountList, Utxo},
        },
    },
    dao::typedefs::serializable_pubkey::SerializablePubkey,
    ingester::{
        parser::bundle::EventBundle,
        persist::{
            persist_bundle,
            state_update::{EnrichedUtxo, UtxoWithSlot},
        },
        typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo},
    },
};

use once_cell::sync::Lazy;
use photon::migration::{Migrator, MigratorTrait};
use psp_compressed_token::{AccountState, TokenTlvData};
pub use sea_orm::DatabaseBackend;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, DbErr, ExecResult, SqlxPostgresConnector,
    SqlxSqliteConnector, Statement, TransactionTrait,
};

use photon::dao::typedefs::hash::Hash;
use photon::ingester::typedefs::block_info::TransactionInfo;
pub use rstest::rstest;
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_config::RpcTransactionConfig, rpc_request::RpcRequest,
};
use solana_sdk::{
    clock::Slot,
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiConfirmedBlock, UiTransactionEncoding,
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions},
    PgPool,
};
use std::sync::Arc;

const RPC_CONFIG: RpcTransactionConfig = RpcTransactionConfig {
    encoding: Some(UiTransactionEncoding::Base64),
    commitment: Some(CommitmentConfig {
        commitment: CommitmentLevel::Confirmed,
    }),
    max_supported_transaction_version: Some(0),
};

static INIT: Lazy<Mutex<Option<()>>> = Lazy::new(|| Mutex::new(None));

fn setup_logging() {
    let env_filter =
        env::var("RUST_LOG").unwrap_or("info,sqlx=error,sea_orm_migration=error".to_string());
    tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter(env_filter)
        .init();
}

async fn run_migrations_from_fresh(db: &DatabaseConnection) {
    std::env::set_var("INIT_FILE_PATH", "../init.sql");
    Migrator::fresh(db).await.unwrap();
}

async fn run_one_time_setup(db: &DatabaseConnection) {
    let mut init = INIT.lock().unwrap();
    if init.is_none() {
        setup_logging();
        if db.get_database_backend() != DbBackend::Sqlite {
            // We run migrations from fresh everytime for SQLite
            run_migrations_from_fresh(db).await;
        }
        *init = Some(())
    }
}

pub struct TestSetup {
    pub db_conn: Arc<DatabaseConnection>,
    pub api: PhotonApi,
    pub name: String,
    pub client: Arc<RpcClient>,
}

#[derive(Clone, Copy, Debug)]
pub enum Network {
    #[allow(unused)]
    Mainnet,
    #[allow(unused)]
    Devnet,
    // Localnet is not a great test option since transactions are not persisted but we don't know
    // how to deploy everything into devnet yet.
    Localnet,
}

#[derive(Clone, Copy)]
pub struct TestSetupOptions {
    pub network: Network,
    pub db_backend: DatabaseBackend,
}

pub async fn setup_with_options(name: String, opts: TestSetupOptions) -> TestSetup {
    let db_conn = Arc::new(match opts.db_backend {
        DatabaseBackend::Postgres => {
            let local_db = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
            if !(local_db.contains("127.0.0.1") || local_db.contains("localhost")) {
                panic!("Refusing to run tests on non-local database out of caution");
            }
            let pool = setup_pg_pool(local_db.to_string()).await;
            SqlxPostgresConnector::from_sqlx_postgres_pool(pool)
        }
        DatabaseBackend::Sqlite => {
            SqlxSqliteConnector::from_sqlx_sqlite_pool(setup_sqllite_pool().await)
        }
        _ => unimplemented!(),
    });
    run_one_time_setup(&db_conn).await;
    match opts.db_backend {
        DatabaseBackend::Postgres => {
            reset_tables(&db_conn).await.unwrap();
        }
        DatabaseBackend::Sqlite => {
            // We need to run migrations from fresh for SQLite every time since we are using an
            // in memory database that gets dropped every after test.
            run_migrations_from_fresh(&db_conn).await;
        }
        _ => unimplemented!(),
    }

    let rpc_url = match opts.network {
        Network::Mainnet => std::env::var("MAINNET_RPC_URL").unwrap(),
        Network::Devnet => std::env::var("DEVNET_RPC_URL").unwrap(),
        Network::Localnet => "http://127.0.0.1:8899".to_string(),
    };
    let client = Arc::new(RpcClient::new(rpc_url.to_string()));
    let api = PhotonApi::new(db_conn.clone(), client.clone());
    TestSetup {
        name,
        db_conn,
        api,
        client,
    }
}

pub async fn setup(name: String, database_backend: DatabaseBackend) -> TestSetup {
    setup_with_options(
        name,
        TestSetupOptions {
            network: Network::Mainnet,
            db_backend: database_backend,
        },
    )
    .await
}

pub async fn setup_pg_pool(database_url: String) -> PgPool {
    let options: PgConnectOptions = database_url.parse().unwrap();
    PgPoolOptions::new()
        .min_connections(1)
        .connect_with(options)
        .await
        .unwrap()
}

pub async fn setup_sqllite_pool() -> SqlitePool {
    let options: SqliteConnectOptions = "sqlite::memory:".parse().unwrap();
    SqlitePoolOptions::new()
        .min_connections(1)
        .connect_with(options)
        .await
        .unwrap()
}

pub async fn reset_tables(conn: &DatabaseConnection) -> Result<(), DbErr> {
    let query = match conn.get_database_backend() {
        sea_orm::DatabaseBackend::Postgres => "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' AND tablename != 'seaql_migrations'",
        sea_orm::DatabaseBackend::Sqlite => "SELECT name as tablename FROM sqlite_master WHERE type='table'",
        _ => unimplemented!()
        // Add other database backends here if needed
    };

    let tables: Vec<String> = conn
        .query_all(Statement::from_string(
            conn.get_database_backend(),
            query.to_string(),
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get("", "tablename").unwrap())
        .collect::<Vec<String>>();

    for table in tables {
        truncate_table(conn, table.to_string()).await?;
    }
    Ok(())
}

pub async fn truncate_table(conn: &DatabaseConnection, table: String) -> Result<ExecResult, DbErr> {
    match conn.get_database_backend() {
        DbBackend::Postgres => {
            conn.execute(Statement::from_string(
                conn.get_database_backend(),
                format!("TRUNCATE TABLE {} CASCADE", table),
            ))
            .await
        }
        DbBackend::Sqlite => {
            conn.execute(Statement::from_string(
                conn.get_database_backend(),
                // SQLite does not support the TRUNCATE operation. Typically, using DELETE FROM could
                // result in errors due to foreign key constraints. However, SQLite does not enforce
                // foreign key constraints by default.
                format!("DELETE FROM {}", table),
            ))
            .await
        }
        _ => unimplemented!(),
    }
}

pub fn mock_str_to_hash(input: &str) -> Hash {
    let mut array = [0u8; 32];
    let bytes = input.as_bytes();

    for (i, &byte) in bytes.iter().enumerate().take(32) {
        array[i] = byte;
    }

    Hash::try_from(array.to_vec()).unwrap()
}

fn get_relative_project_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

pub async fn fetch_transaction(
    client: &RpcClient,
    sig: Signature,
) -> EncodedConfirmedTransactionWithStatusMeta {
    // We do not immediately deserialize because VersionedTransactionWithStatusMeta does not
    // implement Deserialize
    let txn: EncodedConfirmedTransactionWithStatusMeta = client
        .send(
            RpcRequest::GetTransaction,
            serde_json::json!([sig.to_string(), RPC_CONFIG,]),
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

pub async fn cached_fetch_transaction(setup: &TestSetup, tx: &str) -> TransactionInfo {
    let sig = Signature::from_str(tx).unwrap();
    let dir = get_relative_project_path(&format!("tests/data/transactions/{}", setup.name));
    if !Path::new(&dir).exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    let file_path = dir.join(sig.to_string());

    let tx: EncodedConfirmedTransactionWithStatusMeta = if file_path.exists() {
        let txn_string = std::fs::read(file_path).unwrap();
        serde_json::from_slice(&txn_string).unwrap()
    } else {
        let tx = fetch_transaction(&setup.client, sig).await;
        std::fs::write(file_path, serde_json::to_string(&tx).unwrap()).unwrap();
        tx
    };
    tx.try_into().unwrap()
}

async fn fetch_block(client: &RpcClient, slot: Slot) -> UiConfirmedBlock {
    client
        .send(RpcRequest::GetBlock, serde_json::json!([slot, RPC_CONFIG,]))
        .await
        .unwrap()
}

pub async fn cached_fetch_block(setup: &TestSetup, slot: Slot) -> BlockInfo {
    let dir = get_relative_project_path(&format!("tests/data/blocks/{}", setup.name));
    if !Path::new(&dir).exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    let file_path = dir.join(format!("{}", slot));

    let block: UiConfirmedBlock = if file_path.exists() {
        let txn_string = std::fs::read(file_path).unwrap();
        serde_json::from_slice(&txn_string).unwrap()
    } else {
        let block = fetch_block(&setup.client, slot).await;
        std::fs::write(file_path, serde_json::to_string(&block).unwrap()).unwrap();
        block
    };
    parse_ui_confirmed_blocked(block, slot).unwrap()
}

pub fn trim_test_name(name: &str) -> String {
    // Remove the test_ prefix and the case suffix
    name.replace("test_", "")
        .split("::case")
        .next()
        .unwrap()
        .to_string()
}

fn order_tlvs(mut tlvs: Vec<TokenTlvData>) -> Vec<TokenTlvData> {
    let mut without_duplicates = tlvs.clone();
    without_duplicates.dedup_by(|a, b| a.mint == b.mint);
    if without_duplicates.len() != tlvs.len() {
        panic!(
            "Duplicate mint in tlvs: {:?}. Need hashes to further order token tlv data.",
            tlvs
        );
    }
    tlvs.sort_by(|a, b| a.mint.cmp(&b.mint));
    tlvs
}

pub fn verify_responses_match_tlv_data(response: TokenAccountList, tlvs: Vec<TokenTlvData>) {
    if response.items.len() != tlvs.len() {
        panic!(
            "Mismatch in number of accounts. Expected: {}, Actual: {}",
            tlvs.len(),
            response.items.len()
        );
    }

    let token_accounts = response.items;
    for (account, tlv) in token_accounts.iter().zip(order_tlvs(tlvs).iter()) {
        let account = account.clone();
        assert_eq!(account.mint, tlv.mint.into());
        assert_eq!(account.owner, tlv.owner.into());
        assert_eq!(account.amount, tlv.amount);
        assert_eq!(account.delegate, tlv.delegate.map(Into::into));
        assert_eq!(account.is_native, tlv.is_native.is_some());
        assert_eq!(account.frozen, tlv.state == AccountState::Frozen);
        assert_eq!(account.is_native, tlv.is_native.is_some());
    }
}

pub fn assert_utxo_response_list_matches_input(
    utxo_response: &mut Vec<Utxo>,
    input_utxos: &mut Vec<EnrichedUtxo>,
) {
    utxo_response.sort_by(|a, b| a.hash.to_vec().cmp(&b.hash.to_vec()));
    input_utxos.sort_by(|a, b| a.utxo.utxo.hash().cmp(&b.utxo.utxo.hash()));

    for (res, utxo) in utxo_response.iter().zip(input_utxos.iter()) {
        let EnrichedUtxo { utxo, tree, seq } = utxo;
        let UtxoWithSlot { utxo, slot } = utxo;
        assert_eq!(res.hash, utxo.hash().into());
        assert_eq!(res.owner, SerializablePubkey::from(utxo.owner));
        assert_eq!(res.tree, Some(SerializablePubkey::from(tree.clone())));
        assert_eq!(res.seq, Some(*seq as u64));
        assert_eq!(res.lamports, utxo.lamports);
        assert_eq!(res.slot_updated, *slot as u64);
        assert_eq!(
            res.data,
            base64::encode(to_vec(&utxo.data.clone().unwrap()).unwrap())
        );
    }
}

/// Persist using a database connection instead of a transaction. Should only be use for tests.
pub async fn persist_bundle_using_connection(
    db: &DatabaseConnection,
    bundle: EventBundle,
) -> Result<(), sea_orm::DbErr> {
    let txn = db.begin().await.unwrap();
    persist_bundle(&txn, bundle).await.unwrap();
    txn.commit().await.unwrap();
    Ok(())
}
