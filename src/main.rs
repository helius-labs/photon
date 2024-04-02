use std::{fmt, thread::sleep, time::Duration};

use clap::{Parser, ValueEnum};
use jsonrpsee::server::ServerHandle;
use log::{error, info};
use photon_indexer::api::{self, api::PhotonApi};
use photon_indexer::ingester::fetchers::poller::{
    fetch_current_slot_with_infinite_retry, Options, TransactionPoller,
};
use photon_indexer::ingester::index_block_batch_with_infinite_retries;
use photon_indexer::migration::{
    sea_orm::{DatabaseBackend, DatabaseConnection, SqlxPostgresConnector, SqlxSqliteConnector},
    Migrator, MigratorTrait,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    PgPool, SqlitePool,
};
use std::env;
use std::sync::Arc;

#[derive(Parser, Debug, Clone, ValueEnum)]
enum LoggingFormat {
    Standard,
    Json,
}

impl fmt::Display for LoggingFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoggingFormat::Standard => write!(f, "standard"),
            LoggingFormat::Json => write!(f, "json"),
        }
    }
}

/// Photon: a compressed transaction Solana indexer
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Port to expose the local Photon API
    // We use a random default port to avoid conflicts with other services
    #[arg(short, long, default_value_t = 8784)]
    port: u16,

    /// URL of the RPC server
    #[arg(short, long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    /// DB URL to store indexing data. By default we use an in-memory SQLite database.
    #[arg(short, long)]
    db_url: Option<String>,

    /// The start slot to begin indexing from. Defaults to the current slot for testnet/mainnet/devnet
    /// and 0 for localnet.
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// Max database connections to use in database pool
    #[arg(long, default_value_t = 10)]
    max_db_conn: u32,

    /// Logging format
    #[arg(short, long, default_value_t = LoggingFormat::Standard)]
    logging_format: LoggingFormat,

    /// Max number of blocks to fetch concurrently. Generally, this should be set to be as high
    /// as possible without reaching RPC rate limits.
    #[arg(long)]
    max_concurrent_block_fetches: Option<usize>,
}

pub async fn setup_pg_pool(database_url: &str, max_connections: u32) -> PgPool {
    let options: PgConnectOptions = database_url.parse().unwrap();
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
        .unwrap()
}

async fn start_transaction_indexer(
    db: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    is_localnet: bool,
    start_slot: Option<u64>,
    max_batch_size: usize,
) -> tokio::task::JoinHandle<()> {
    // Spawn the task
    let handle = tokio::spawn(async move {
        let current_slot = fetch_current_slot_with_infinite_retry(rpc_client.as_ref()).await;
        let start_slot = match (start_slot, is_localnet) {
            (Some(start_slot), _) => start_slot,
            // Start indexing from the first slot for localnet.
            (None, true) => 0,
            (None, false) => current_slot,
        };
        let mut poller = TransactionPoller::new(rpc_client, Options { start_slot }).await;
        let number_of_blocks_to_backfill = current_slot - start_slot;
        info!(
            "Backfilling historical blocks. Number of blocks to backfill: {}",
            number_of_blocks_to_backfill
        );
        if number_of_blocks_to_backfill > 10_000 && is_localnet {
            info!("Backfilling a large number of blocks. This may take a while. Considering restarting local validator or specifying a start slot.");
        }
        let mut finished_backfill = false;

        loop {
            let blocks = poller.fetch_new_block_batch(max_batch_size).await;
            if blocks.is_empty() {
                sleep(Duration::from_millis(20));
                if !finished_backfill {
                    info!("Finished backfilling historical blocks...");
                    info!("Streaming live blocks...");
                }
                finished_backfill = true;
                continue;
            }
            index_block_batch_with_infinite_retries(db.as_ref(), blocks).await;
        }
    });
    handle
}

async fn start_api_server(
    db: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    api_port: u16,
) -> ServerHandle {
    let api = PhotonApi::new(db, rpc_client);
    api::rpc_server::run_server(api, api_port).await.unwrap()
}

fn setup_logging(logging_format: LoggingFormat) {
    let env_filter =
        env::var("RUST_LOG").unwrap_or("info,sqlx=error,sea_orm_migration=error".to_string());
    let subscriber = tracing_subscriber::fmt().with_env_filter(env_filter);
    match logging_format {
        LoggingFormat::Standard => subscriber.init(),
        LoggingFormat::Json => subscriber.json().init(),
    }
}

async fn setup_sqllite_in_memory_pool(max_connections: u32) -> SqlitePool {
    setup_sqllite_pool("sqlite::memory:", max_connections).await
}

async fn setup_sqllite_pool(db_url: &str, max_connections: u32) -> SqlitePool {
    let options: SqliteConnectOptions = db_url.parse().unwrap();
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
        .unwrap()
}

pub fn parse_db_type(db_url: &str) -> DatabaseBackend {
    if db_url.starts_with("postgres://") {
        DatabaseBackend::Postgres
    } else if db_url.starts_with("sqlite://") {
        DatabaseBackend::Sqlite
    } else {
        unimplemented!("Unsupported database type: {}", db_url)
    }
}

async fn setup_database_connection(
    db_url: Option<String>,
    max_connections: u32,
) -> Arc<DatabaseConnection> {
    Arc::new(match db_url {
        Some(db_url) => {
            let db_type = parse_db_type(&db_url);
            match db_type {
                DatabaseBackend::Postgres => SqlxPostgresConnector::from_sqlx_postgres_pool(
                    setup_pg_pool(&db_url, max_connections).await,
                ),
                DatabaseBackend::Sqlite => SqlxSqliteConnector::from_sqlx_sqlite_pool(
                    setup_sqllite_pool(&db_url, max_connections).await,
                ),
                _ => unimplemented!("Unsupported database type: {}", db_url),
            }
        }
        None => SqlxSqliteConnector::from_sqlx_sqlite_pool(
            setup_sqllite_in_memory_pool(max_connections).await,
        ),
    })
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_logging(args.logging_format);

    let db_conn = setup_database_connection(args.db_url.clone(), args.max_db_conn).await;
    if args.db_url.is_none() {
        info!("Running migrations...");
        Migrator::up(db_conn.as_ref(), None).await.unwrap();
    }
    let rpc_client = Arc::new(RpcClient::new(args.rpc_url.clone()));

    info!("Starting indexer...");
    let is_localnet = args.rpc_url.contains("127.0.0.1");
    // For localnet we can safely use a large batch size to speed up indexing.
    let max_batch_size = if is_localnet { 20 } else { 5 };
    let indexer_handle = start_transaction_indexer(
        db_conn.clone(),
        rpc_client.clone(),
        is_localnet,
        args.start_slot,
        max_batch_size,
    )
    .await;

    info!("Starting API server with port {}...", args.port);
    let api_handler = start_api_server(db_conn, rpc_client, args.port).await;
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            info!("Shutting down indexer...");
            indexer_handle.abort();
            info!("Shutting down API server...");
            api_handler.stop().unwrap();
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }
    // We need to wait for the API server to stop to ensure that all clean up is done
    tokio::spawn(api_handler.stopped());
}
