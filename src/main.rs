use std::{fmt, thread::sleep, time::Duration};

use clap::{Parser, ValueEnum};
use jsonrpsee::server::ServerHandle;
use log::{error, info};
use photon::api::{self, api::PhotonApi};
use photon::ingester::{
    fetchers::poller::{fetch_current_slot_with_infinite_retry, Options, TransactionPoller},
    index_block_stream,
};
use photon::migration::{
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
    #[arg(short, long, default_value_t = 8781)]
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
    #[arg(short, long, default_value_t = 10)]
    max_db_conn: u32,

    /// Logging format
    #[arg(short, long, default_value_t = LoggingFormat::Standard)]
    logging_format: LoggingFormat,
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
    rpc_client: RpcClient,
    is_localnet: bool,
    start_slot: Option<u64>,
    max_indexing_concurrency: u32,
) -> tokio::task::JoinHandle<()> {
    let rpc_client = Arc::new(rpc_client);
    tokio::spawn(async move {
        let start_slot = match (start_slot, is_localnet) {
            (Some(start_slot), _) => start_slot,
            // Start indexing from the first slot for localnet.
            (None, true) => 0,
            (None, false) => fetch_current_slot_with_infinite_retry(rpc_client.as_ref()).await,
        };
        let mut poller = TransactionPoller::new(
            rpc_client,
            Options {
                start_slot,
                // Indexing throughput is more influenced by concurrent transaction indexing than
                // RPC block fetching. So just use a reasonable default here.
                max_block_fetching_concurrency: 5,
            },
        )
        .await;

        info!("Backfilling historical blocks...");
        let mut finished_backfill = false;
        loop {
            let blocks = poller.fetch_new_block_batch(5).await;
            if blocks.is_empty() {
                sleep(Duration::from_millis(20));
                if !finished_backfill {
                    info!("Finished backfilling historical blocks...");
                    info!("Streaming live blocks...");
                }
                finished_backfill = true;
                continue;
            }
            let transactions = futures::stream::iter(transactions);

            index_transaction_stream(
                db.clone(),
                Box::pin(transactions),
                max_indexing_concurrency as usize,
            )
            .await;
        }
    })
}

async fn start_api_server(db: Arc<DatabaseConnection>, api_port: u16) -> ServerHandle {
    let api = PhotonApi::from(db);
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

    info!("Starting indexer...");
    let is_localnet = args.rpc_url.contains("127.0.0.1");
    start_transaction_indexer(
        db_conn.clone(),
        RpcClient::new(args.rpc_url),
        is_localnet,
        args.start_slot,
        // Set the max number of concurrent transactions to the index to the number of db connections
        // as we can concurrently index at most one transaction per db connection.
        args.max_db_conn,
    )
    .await;

    info!("Starting API server with port {}...", args.port);
    let api_handler = start_api_server(db_conn, args.port).await;
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
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
