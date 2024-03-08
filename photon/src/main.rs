use std::{fmt, thread::sleep, time::Duration};

use api::api::PhotonApi;
use clap::{Parser, ValueEnum};
use ingester::{
    fetchers::poller::{fetch_current_slot_with_infinite_retry, Options, TransactionPoller},
    index_transaction_stream,
};
use jsonrpsee::server::ServerHandle;
use log::{error, info};
use migration::{
    sea_orm::{DatabaseConnection, SqlxPostgresConnector, SqlxSqliteConnector},
    Migrator, MigratorTrait,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use sqlx::{
    migrate::Migration,
    postgres::{PgConnectOptions, PgPoolOptions},
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    PgPool, SqlitePool,
};
use std::env;
use std::sync::Arc;

#[derive(Parser, Debug, Clone, ValueEnum)]
enum LoggingFormat {
    STANDARD,
    JSON,
}

impl fmt::Display for LoggingFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoggingFormat::STANDARD => write!(f, "standard"),
            LoggingFormat::JSON => write!(f, "json"),
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

    /// DB URL to store indexing data
    #[arg(short, long, default_value = "postgres://postgres@localhost/postgres")]
    db_url: String,

    /// The start slot to begin indexing from. Defaults to the current slot for testnet/mainnet/devnet
    /// and 0 for localnet.
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// Max database connections to use in database pool
    #[arg(short, long, default_value_t = 10)]
    max_db_conn: u32,

    /// Logging format
    #[arg(short, long, default_value_t = LoggingFormat::STANDARD)]
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
            let transactions = poller.fetch_new_transactions().await;
            if transactions.is_empty() {
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
        LoggingFormat::STANDARD => subscriber.init(),
        LoggingFormat::JSON => subscriber.json().init(),
    }
}

pub async fn setup_sqllite_pool() -> SqlitePool {
    let options: SqliteConnectOptions = "sqlite::memory:".parse().unwrap();
    SqlitePoolOptions::new()
        .min_connections(1)
        .connect_with(options)
        .await
        .unwrap()
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_logging(args.logging_format);

    // let db_conn = Arc::new(SqlxPostgresConnector::from_sqlx_postgres_pool(
    //     setup_pg_pool(&args.db_url, args.max_db_conn).await,
    // ));
    let db_conn = Arc::new(SqlxSqliteConnector::from_sqlx_sqlite_pool(
        setup_sqllite_pool().await,
    ));
    Migrator::fresh(db_conn.as_ref()).await.unwrap();

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

    info!("Starting API server...");
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
