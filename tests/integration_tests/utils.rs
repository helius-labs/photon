use std::{env, path::Path, str::FromStr, sync::Mutex};

use once_cell::sync::Lazy;

// Predefined test tree pubkeys that have metadata populated in populate_test_tree_metadata
pub const TEST_STATE_TREE_V1_1: &str = "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT";
use photon_indexer::api::method::utils::{TokenAccount, TokenAccountListV2, TokenAccountV2};
use photon_indexer::common::typedefs::account::AccountV2;
use photon_indexer::common::typedefs::hash::Hash;
use photon_indexer::migration::{MigractorWithCustomMigrations, MigratorTrait};
use photon_indexer::{
    api::{api::PhotonApi, method::utils::TokenAccountList},
    common::{
        get_rpc_client, relative_project_path,
        typedefs::{account::Account, token_data::TokenData},
    },
    ingester::{
        parser::{parse_transaction, state_update::StateUpdate, EXPECTED_TREE_OWNER},
        persist::persist_state_update,
        typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo, TransactionInfo},
    },
};
pub use sea_orm::DatabaseBackend;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, DbErr, ExecResult, SqlxPostgresConnector,
    SqlxSqliteConnector, Statement, TransactionTrait,
};

use light_compressed_account::TreeType;
use photon_indexer::ingester::index_block;
use photon_indexer::ingester::typedefs::block_info::BlockMetadata;
use photon_indexer::monitor::tree_metadata_sync::{upsert_tree_metadata, TreeAccountData};
pub use rstest::rstest;
use solana_account::Account as SolanaAccount;
use solana_client::{
    nonblocking::rpc_client::RpcClient, rpc_config::RpcTransactionConfig, rpc_request::RpcRequest,
};
use solana_clock::Slot;
use solana_commitment_config::CommitmentConfig;
use solana_commitment_config::CommitmentLevel;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
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
    MigractorWithCustomMigrations::fresh(db).await.unwrap();
}

/// Populate common test tree metadata for tests to work
pub async fn populate_test_tree_metadata(db: &DatabaseConnection) {
    // Common test trees from the old static configuration
    let test_trees = vec![
        // V1 State Trees
        (
            "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT",
            "nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148",
            TreeType::StateV1,
            26,
            2400,
        ),
        (
            "smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho",
            "nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X",
            TreeType::StateV1,
            26,
            2400,
        ),
        // V1 Address Trees
        (
            "amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2",
            "aq1S9z4reTSQAdgWHGD2zDaS39sjGrAxbR31vxJ2F4F",
            TreeType::AddressV1,
            26,
            2400,
        ),
        // V2 State Trees
        (
            "HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu",
            "6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU",
            TreeType::StateV2,
            32,
            2400,
        ),
        // V2 Address Tree
        (
            "EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK",
            "EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK",
            TreeType::AddressV2,
            40,
            2400,
        ),
    ];

    let txn = db.begin().await.unwrap();

    for (tree_str, queue_str, tree_type, height, root_history_capacity) in test_trees {
        let tree_pubkey = tree_str.parse::<Pubkey>().unwrap();
        let queue_pubkey = queue_str.parse::<Pubkey>().unwrap();

        // Only insert if it doesn't already exist
        let owner = EXPECTED_TREE_OWNER.expect("EXPECTED_TREE_OWNER must be set for tests");
        let data = TreeAccountData {
            queue_pubkey,
            root_history_capacity: root_history_capacity as usize,
            height: height as u32,
            sequence_number: 0,
            next_index: 0,
            owner: Pubkey::from(owner.to_bytes()),
        };
        let _ = upsert_tree_metadata(&txn, tree_pubkey, tree_type, &data, 0).await;
    }

    txn.commit().await.unwrap();
}

async fn run_one_time_setup(db: &DatabaseConnection) {
    let mut init = INIT.lock().unwrap();
    if init.is_none() {
        setup_logging();
        if db.get_database_backend() == DbBackend::Postgres {
            // We run migrations from fresh everytime for SQLite
            MigractorWithCustomMigrations::fresh(db).await.unwrap();
        }
        *init = Some(())
    }
}

pub struct TestSetup {
    pub db_conn: Arc<DatabaseConnection>,
    pub api: PhotonApi,
    pub name: String,
    pub client: Arc<RpcClient>,
    pub prover_url: String,
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
    // Skip transaction isolation level settings in tests to avoid read-after-write visibility issues
    env::set_var("PHOTON_SKIP_ISOLATION_LEVEL", "true");

    let db_conn: Arc<DatabaseConnection> = Arc::new(match opts.db_backend {
        DatabaseBackend::Postgres => {
            let local_db = env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL must be set");
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
            populate_test_tree_metadata(&db_conn).await;
        }
        DatabaseBackend::Sqlite => {
            // We need to run migrations from fresh for SQLite every time since we are using an
            // in memory database that gets dropped every after test.
            run_migrations_from_fresh(&db_conn).await;
            populate_test_tree_metadata(&db_conn).await;
        }
        _ => unimplemented!(),
    }

    let rpc_url = match opts.network {
        Network::Mainnet => env::var("MAINNET_RPC_URL").unwrap(),
        Network::Devnet => env::var("DEVNET_RPC_URL").unwrap(),
        Network::Localnet => "http://127.0.0.1:8899".to_string(),
    };
    let client = get_rpc_client(&rpc_url);
    let prover_url = "http://127.0.0.1:3001".to_string();
    let api = PhotonApi::new(db_conn.clone(), client.clone(), prover_url.clone());
    TestSetup {
        name,
        db_conn,
        api,
        client: client.clone(),
        prover_url,
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
    let pool = PgPoolOptions::new()
        .min_connections(1)
        .connect_with(options)
        .await
        .unwrap();

    // Set default isolation level to READ COMMITTED for all connections in the pool
    // This ensures each statement sees the latest committed data
    sqlx::query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&pool)
        .await
        .unwrap();

    pool
}

pub async fn setup_sqllite_pool() -> SqlitePool {
    let db_name = format!(
        "test_{}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        rand::random::<u64>()
    );

    let options: SqliteConnectOptions = format!("sqlite:file:{}?mode=memory&cache=shared", db_name)
        .parse::<SqliteConnectOptions>()
        .unwrap()
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);

    SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(1)
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

    // Re-populate tree metadata after resetting tables
    populate_test_tree_metadata(conn).await;

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
        .unwrap_or_else(|_| panic!("Failed to fetch transaction: {sig}"));

    // Ignore if tx failed or meta is missed
    let meta = txn.transaction.meta.as_ref();
    if meta.map(|meta| meta.status.is_err()).unwrap_or(true) {
        panic!("Trying to index failed transaction: {}", sig);
    }
    txn
}

pub async fn cached_fetch_transaction(
    test_name: &str,
    rpc_client: Arc<RpcClient>,
    tx: &str,
) -> EncodedConfirmedTransactionWithStatusMeta {
    let sig = Signature::from_str(tx).unwrap();
    let dir = relative_project_path(&format!("tests/data/transactions/{}", test_name));
    if !Path::new(&dir).exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    let file_path = dir.join(sig.to_string());

    if file_path.exists() {
        let txn_string = std::fs::read(file_path).unwrap();
        serde_json::from_slice(&txn_string).unwrap()
    } else {
        let tx = fetch_transaction(&rpc_client, sig).await;
        std::fs::write(file_path, serde_json::to_string(&tx).unwrap()).unwrap();
        tx
    }
}

async fn fetch_block(client: &RpcClient, slot: Slot) -> UiConfirmedBlock {
    client
        .send(RpcRequest::GetBlock, serde_json::json!([slot, RPC_CONFIG,]))
        .await
        .unwrap()
}

pub async fn cached_fetch_block(
    test_name: &str,
    rpc_client: Arc<RpcClient>,
    slot: Slot,
) -> BlockInfo {
    let dir = relative_project_path(&format!("tests/data/blocks/{}", test_name));
    if !Path::new(&dir).exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    let file_path = dir.join(format!("{}", slot));

    let block: UiConfirmedBlock = if file_path.exists() {
        let txn_string = std::fs::read(file_path).unwrap();
        serde_json::from_slice(&txn_string).unwrap()
    } else {
        let block = fetch_block(&rpc_client, slot).await;
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

#[derive(Clone, Debug)]
pub struct TokenDataWithHash {
    pub token_data: TokenData,
    pub hash: Hash,
}

fn order_token_datas(mut token_datas: Vec<TokenDataWithHash>) -> Vec<TokenDataWithHash> {
    token_datas.sort_by(|a, b| {
        a.token_data
            .mint
            .0
            .cmp(&b.token_data.mint.0)
            .then_with(|| a.hash.to_vec().cmp(&b.hash.to_vec()))
    });
    token_datas
}

pub fn verify_response_matches_input_token_data(
    response: TokenAccountList,
    tlvs: Vec<TokenDataWithHash>,
) {
    if response.items.len() != tlvs.len() {
        panic!(
            "Mismatch in number of accounts. Expected: {}, Actual: {}",
            tlvs.len(),
            response.items.len()
        );
    }

    let token_accounts = response.items;
    for (account, tlv) in token_accounts.iter().zip(order_token_datas(tlvs).iter()) {
        let account = account.clone();
        assert_eq!(account.token_data.mint, tlv.token_data.mint);
        assert_eq!(account.token_data.owner, tlv.token_data.owner);
        assert_eq!(account.token_data.amount, tlv.token_data.amount);
        assert_eq!(
            account.token_data.delegate,
            tlv.token_data.delegate.map(Into::into)
        );
    }
}

pub fn verify_response_matches_input_token_data_v2(
    response: TokenAccountListV2,
    tlvs: Vec<TokenDataWithHash>,
) {
    if response.items.len() != tlvs.len() {
        panic!(
            "Mismatch in number of accounts. Expected: {}, Actual: {}",
            tlvs.len(),
            response.items.len()
        );
    }
    let token_accounts = response.items;
    for (account, tlv) in token_accounts.iter().zip(order_token_datas(tlvs).iter()) {
        let account = account.clone();
        assert_eq!(account.token_data.mint, tlv.token_data.mint);
        assert_eq!(account.token_data.owner, tlv.token_data.owner);
        assert_eq!(account.token_data.amount, tlv.token_data.amount);
        assert_eq!(
            account.token_data.delegate,
            tlv.token_data.delegate.map(Into::into)
        );
        assert_eq!(account.token_data.state, tlv.token_data.state);
    }
}

pub fn assert_account_response_list_matches_input(
    account_response: &mut Vec<Account>,
    input_accounts: &mut Vec<Account>,
) {
    assert_eq!(account_response.len(), input_accounts.len());
    account_response.sort_by(|a, b| a.hash.to_vec().cmp(&b.hash.to_vec()));
    input_accounts.sort_by(|a, b| a.hash.to_vec().cmp(&b.hash.to_vec()));
    assert_eq!(account_response, input_accounts);
}
pub fn assert_account_response_list_matches_input_v2(
    account_response_v2: &mut Vec<AccountV2>,
    input_accounts: &mut Vec<Account>,
) {
    assert_eq!(account_response_v2.len(), input_accounts.len());
    account_response_v2.sort_by(|a, b| a.hash.to_vec().cmp(&b.hash.to_vec()));
    input_accounts.sort_by(|a, b| a.hash.to_vec().cmp(&b.hash.to_vec()));

    for (account_v2, account) in account_response_v2.iter().zip(input_accounts.iter()) {
        compare_account_with_account_v2(account, account_v2);
    }
}

pub fn compare_account_with_account_v2(account: &Account, account_v2: &AccountV2) {
    assert_eq!(account.hash, account_v2.hash);
    assert_eq!(account.address, account_v2.address);
    assert_eq!(account.data, account_v2.data);
    assert_eq!(account.owner, account_v2.owner);
    assert_eq!(account.lamports, account_v2.lamports);
    assert_eq!(account.tree, account_v2.merkle_context.tree);
    assert_eq!(account.leaf_index, account_v2.leaf_index);
    assert_eq!(account.seq, account_v2.seq);
    assert_eq!(account.slot_created, account_v2.slot_created);
}

pub fn compare_token_account_with_token_account_v2(
    token_acc: &TokenAccount,
    token_acc_v2: &TokenAccountV2,
) {
    compare_account_with_account_v2(&token_acc.account, &token_acc_v2.account);
    assert_eq!(token_acc.token_data.mint, token_acc_v2.token_data.mint);
    assert_eq!(token_acc.token_data.owner, token_acc_v2.token_data.owner);
    assert_eq!(token_acc.token_data.amount, token_acc_v2.token_data.amount);
    assert_eq!(
        token_acc.token_data.delegate,
        token_acc_v2.token_data.delegate
    );
    assert_eq!(token_acc.token_data.state, token_acc_v2.token_data.state);
}

/// Persist using a database connection instead of a transaction. Should only be use for tests.
pub async fn persist_state_update_using_connection(
    db: &DatabaseConnection,
    state_update: StateUpdate,
) -> Result<(), sea_orm::DbErr> {
    let txn = db.begin().await.unwrap();
    persist_state_update(&txn, state_update).await.unwrap();
    txn.commit().await.unwrap();
    Ok(())
}

pub async fn index_transaction(
    test_name: &str,
    db_conn: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    tx: &str,
) {
    let tx = cached_fetch_transaction(test_name, rpc_client, tx).await;
    let tx_info: TransactionInfo = tx.try_into().unwrap();
    let state_update = parse_transaction(db_conn.as_ref(), &tx_info, 0)
        .await
        .unwrap();
    persist_state_update_using_connection(db_conn.as_ref(), state_update)
        .await
        .unwrap();
}

pub async fn index_multiple_transactions(
    test_name: &str,
    db_conn: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    txs: Vec<&str>,
) {
    let mut transactions_infos = Vec::<TransactionInfo>::new();
    for tx in txs {
        let tx = cached_fetch_transaction(test_name, rpc_client.clone(), tx).await;
        transactions_infos.push(tx.try_into().unwrap());
    }
    let mut state_updates = Vec::new();
    for transaction_info in transactions_infos {
        let tx_state_update = parse_transaction(db_conn.as_ref(), &transaction_info, 0)
            .await
            .unwrap();
        state_updates.push(tx_state_update);
    }
    let state_update = StateUpdate::merge_updates(state_updates);
    persist_state_update_using_connection(db_conn.as_ref(), state_update)
        .await
        .unwrap();
}

#[allow(dead_code)]
pub async fn cached_fetch_account(setup: &TestSetup, account: Pubkey) -> SolanaAccount {
    let dir = relative_project_path(&format!("tests/data/accounts/{}", setup.name));
    if !Path::new(&dir).exists() {
        std::fs::create_dir(&dir).unwrap();
    }
    let file_path = dir.join(account.to_string());
    if file_path.exists() {
        let account_string = std::fs::read(file_path).unwrap();
        serde_json::from_slice(&account_string).unwrap()
    } else {
        let account = fetch_account(&setup.client, account).await;
        std::fs::write(file_path, serde_json::to_string(&account).unwrap()).unwrap();
        account
    }
}

#[allow(dead_code)]
async fn fetch_account(client: &RpcClient, account: Pubkey) -> SolanaAccount {
    client.get_account(&account).await.unwrap()
}

/// Reads file names from tests/data/transactions/<name>
/// returns a vector of file names sorted by slot
pub fn read_file_names(name: &str, sort_by_slot: bool) -> Vec<String> {
    let signatures = std::fs::read_dir(format!("tests/data/transactions/{}", name))
        .unwrap()
        .filter_map(|entry| {
            entry
                .ok()
                .and_then(|e| e.file_name().to_str().map(|s| s.to_string()))
        })
        .collect::<Vec<String>>();
    if sort_by_slot {
        let mut sorted_files: Vec<(String, u64)> = Vec::new();
        for filename in signatures {
            let json_str =
                std::fs::read_to_string(format!("tests/data/transactions/{}/{}", name, filename))
                    .unwrap();
            let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();
            let slot = json["slot"].as_u64().unwrap_or(0);
            sorted_files.push((filename, slot));
        }
        sorted_files.sort_by_key(|k| k.1);
        sorted_files.into_iter().map(|(name, _)| name).collect()
    } else {
        signatures
    }
}

/// Reset table
/// Index transactions individually or in one batch
pub async fn index(
    test_name: &str,
    db_conn: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    txns: &[String],
    index_transactions_individually: bool,
) {
    let txs_permutations = txns
        .iter()
        .map(|x| vec![x.to_string()])
        .collect::<Vec<Vec<_>>>();

    for index_transactions_individually in [index_transactions_individually] {
        for (i, txs) in txs_permutations.clone().iter().enumerate() {
            println!(
                "indexing tx {} {}/{}",
                index_transactions_individually,
                i + 1,
                txs_permutations.len()
            );
            println!("tx {:?}", txs);

            // HACK: We index a block so that API methods can fetch the current slot.
            index_block(
                db_conn.as_ref(),
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

            if index_transactions_individually {
                for tx in txs {
                    index_transaction(test_name, db_conn.clone(), rpc_client.clone(), tx).await;
                }
            } else {
                index_multiple_transactions(
                    test_name,
                    db_conn.clone(),
                    rpc_client.clone(),
                    txs.iter().map(|x| x.as_str()).collect(),
                )
                .await;
            }
        }
    }
}
