use std::fmt;
use std::fs::File;
use std::net::UdpSocket;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use cadence::{BufferedUdpMetricSink, QueuingMetricSink, StatsdClient};
use cadence_macros::set_global_default;
use clap::{Parser, ValueEnum};
use futures::{pin_mut, stream, StreamExt};
use jsonrpsee::server::ServerHandle;
use log::{error, info};
use photon_indexer::api::{self, api::PhotonApi};

use photon_indexer::ingester::fetchers::BlockStreamConfig;
use photon_indexer::ingester::indexer::{
    fetch_last_indexed_slot_with_infinite_retry, index_block_stream,
};
use photon_indexer::ingester::snapshotter::{
    get_snapshot_files_with_slots, load_block_stream_from_snapshot_directory,
};
use photon_indexer::migration::{
    sea_orm::{DatabaseBackend, DatabaseConnection, SqlxPostgresConnector, SqlxSqliteConnector},
    Migrator, MigratorTrait,
};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcBlockConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    PgPool, SqlitePool,
};
use std::env;
use std::env::temp_dir;
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

    /// The start slot to begin indexing from. Defaults to the last indexed slot in the database plus
    /// one.  
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
    #[arg(short, long)]
    max_concurrent_block_fetches: Option<usize>,

    /// Light Prover url to use for verifying proofs
    #[arg(long, default_value = "http://127.0.0.1:3001")]
    prover_url: String,

    /// Snasphot directory
    #[arg(long, default_value = None)]
    snapshot_dir: Option<String>,

    /// Incremental snapshot slots
    #[arg(long, default_value = None)]
    incremental_snapshot_internval_slots: Option<u64>,

    /// Fullsnapshot slots
    #[arg(long, default_value = None)]
    snapshot_interval_slots: Option<u64>,

    #[arg(short, long, default_value = None)]
    /// Yellowstone gRPC URL. If it's inputed, then the indexer will use gRPC to fetch new blocks
    /// instead of polling. It will still use RPC to fetch blocks if
    grpc_url: Option<String>,

    /// Disable indexing
    #[arg(long, action = clap::ArgAction::SetTrue)]
    disable_indexing: bool,

    /// Disable API
    #[arg(long, action = clap::ArgAction::SetTrue)]
    disable_api: bool,

    /// Metrics endpoint in the format `host:port`
    /// If provided, metrics will be sent to the specified statsd server.
    #[arg(long, default_value = None)]
    metrics_endpoint: Option<String>,
}

pub async fn setup_pg_pool(database_url: &str, max_connections: u32) -> PgPool {
    let options: PgConnectOptions = database_url.parse().unwrap();
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
        .unwrap()
}

async fn start_api_server(
    db: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    prover_url: String,
    api_port: u16,
) -> ServerHandle {
    let api = PhotonApi::new(db, rpc_client, prover_url);
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

pub fn setup_metrics(metrics_endpoint: Option<String>) {
    if let Some(metrics_endpoint) = metrics_endpoint {
        let env = env::var("ENV").unwrap_or("dev".to_string());
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let (host, port) = {
            let mut iter = metrics_endpoint.split(":");
            (iter.next().unwrap(), iter.next().unwrap())
        };
        let port = port.parse::<u16>().unwrap();
        let udp_sink = BufferedUdpMetricSink::from((host, port), socket).unwrap();
        let queuing_sink = QueuingMetricSink::from(udp_sink);
        let builder = StatsdClient::builder("photon", queuing_sink);
        let client = builder
            .with_tag("env", env)
            .with_tag("version", env!("CARGO_PKG_VERSION"))
            .build();
        set_global_default(client);
    }
}

async fn setup_temporary_sqlite_database_pool(max_connections: u32) -> SqlitePool {
    let dir = temp_dir();
    if !dir.exists() {
        std::fs::create_dir_all(&dir).unwrap();
    }
    let db_name = "photon_indexer.db";
    let path = dir.join(db_name);
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    info!("Creating temporary SQLite database at: {:?}", path);
    File::create(&path).unwrap();
    let db_path = format!("sqlite:////{}", path.to_str().unwrap());
    setup_sqlite_pool(&db_path, max_connections).await
}

async fn setup_sqlite_pool(db_url: &str, max_connections: u32) -> SqlitePool {
    let options: SqliteConnectOptions = db_url.parse().unwrap();
    SqlitePoolOptions::new()
        .max_connections(max_connections)
        .min_connections(1)
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
                    setup_sqlite_pool(&db_url, max_connections).await,
                ),
                _ => unimplemented!("Unsupported database type: {}", db_url),
            }
        }
        None => SqlxSqliteConnector::from_sqlx_sqlite_pool(
            setup_temporary_sqlite_database_pool(max_connections).await,
        ),
    })
}

async fn get_genesis_hash_with_infinite_retry(rpc_client: &RpcClient) -> String {
    loop {
        match rpc_client.get_genesis_hash().await {
            Ok(genesis_hash) => return genesis_hash.to_string(),
            Err(e) => {
                log::error!("Failed to fetch genesis hash: {}", e);
                sleep(Duration::from_secs(5));
            }
        }
    }
}

async fn fetch_block_parent_slot(rpc_client: Arc<RpcClient>, slot: u64) -> u64 {
    rpc_client
        .get_block_with_config(
            slot,
            RpcBlockConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                transaction_details: Some(TransactionDetails::None),
                rewards: None,
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            },
        )
        .await
        .unwrap()
        .parent_slot
}

fn continously_index_new_blocks(
    block_stream_config: BlockStreamConfig,
    db: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    last_indexed_slot: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let block_stream = block_stream_config.load_block_stream();
        index_block_stream(block_stream, db, rpc_client, last_indexed_slot).await;
    })
}

async fn continously_run_snapshotter(
    block_stream_config: BlockStreamConfig,
    incremental_snapshot_interval_slots: u64,
    full_snapshot_interval_slots: u64,
    snapshot_dir: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        photon_indexer::ingester::snapshotter::update_snapshot(
            block_stream_config,
            incremental_snapshot_interval_slots,
            full_snapshot_interval_slots,
            Path::new(&snapshot_dir),
        )
        .await;
    })
}

async fn get_network_start_slot(rpc_client: Arc<RpcClient>) -> u64 {
    let genesis_hash = get_genesis_hash_with_infinite_retry(rpc_client.as_ref()).await;
    match genesis_hash.as_str() {
        // Devnet
        "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG" => 310276132 - 1,
        // Mainnet
        "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d" => 277957074 - 1,
        _ => 0,
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_logging(args.logging_format);
    setup_metrics(args.metrics_endpoint);

    let db_conn = setup_database_connection(args.db_url.clone(), args.max_db_conn).await;
    if args.db_url.is_none() {
        info!("Running migrations...");
        Migrator::up(db_conn.as_ref(), None).await.unwrap();
    }
    let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
        args.rpc_url.clone(),
        Duration::from_secs(10),
        CommitmentConfig::confirmed(),
    ));
    let mut snapshotter_handle = None;

    if let Some(snapshot_dir) = args.snapshot_dir {
        let snapshot_dir_path = Path::new(&snapshot_dir);
        if !get_snapshot_files_with_slots(snapshot_dir_path).is_empty() {
            info!("Detected snapshot files. Loading snapshot...");
            let block_stream = load_block_stream_from_snapshot_directory(snapshot_dir_path);
            pin_mut!(block_stream);
            let first_block = block_stream.next().await.unwrap();
            let slot = first_block.metadata.slot;
            let last_indexed_slot = first_block.metadata.parent_slot;
            index_block_stream(
                stream::iter(vec![first_block].into_iter()),
                db_conn.clone(),
                rpc_client.clone(),
                last_indexed_slot,
            )
            .await;
            index_block_stream(block_stream, db_conn.clone(), rpc_client.clone(), slot).await;
        }

        match (
            args.incremental_snapshot_internval_slots,
            args.snapshot_interval_slots,
        ) {
            (Some(incremental_snapshot_interval_slots), Some(snapshot_interval_slots)) => {
                info!("Starting snapshotter...");

                snapshotter_handle = Some(
                    continously_run_snapshotter(
                        BlockStreamConfig {
                            rpc_client: rpc_client.clone(),
                            max_concurrent_block_fetches: 1,
                            last_indexed_slot: 0,
                            geyser_url: args.grpc_url.clone(),
                        },
                        incremental_snapshot_interval_slots,
                        snapshot_interval_slots,
                        snapshot_dir,
                    )
                    .await,
                );
            }
            (None, None) => {
                info!("Snapshotting is disabled");
            }
            _ => {
                panic!("Both incremental_snapshot_interval_slots and snapshot_interval_slots must be provided");
            }
        }
    }

    let is_rpc_node_local = args.rpc_url.contains("127.0.0.1");

    let indexer_handle = match args.disable_indexing {
        true => {
            info!("Indexing is disabled");
            None
        }
        false => {
            info!("Starting indexer...");
            // For localnet we can safely use a large batch size to speed up indexing.
            let max_concurrent_block_fetches = match args.max_concurrent_block_fetches {
                Some(max_concurrent_block_fetches) => max_concurrent_block_fetches,
                None => {
                    if is_rpc_node_local {
                        200
                    } else {
                        20
                    }
                }
            };

            let last_indexed_slot = match args.start_slot {
                Some(start_slot) => fetch_block_parent_slot(rpc_client.clone(), start_slot).await,
                None => {
                    (fetch_last_indexed_slot_with_infinite_retry(db_conn.as_ref())
                        .await
                        .unwrap_or(get_network_start_slot(rpc_client.clone()).await as i64))
                        as u64
                }
            };

            let block_stream_config = BlockStreamConfig {
                rpc_client: rpc_client.clone(),
                max_concurrent_block_fetches,
                last_indexed_slot,
                geyser_url: args.grpc_url,
            };

            Some(continously_index_new_blocks(
                block_stream_config,
                db_conn.clone(),
                rpc_client.clone(),
                last_indexed_slot,
            ))
        }
    };

    info!("Starting API server with port {}...", args.port);
    let api_handler = if args.disable_api {
        None
    } else {
        Some(
            start_api_server(
                db_conn.clone(),
                rpc_client.clone(),
                args.prover_url,
                args.port,
            )
            .await,
        )
    };

    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            if let Some(indexer_handle) = indexer_handle {
                info!("Shutting down indexer...");
                indexer_handle.abort();
                indexer_handle
                    .await
                    .expect_err("Indexer should have been aborted");
            }
            if let Some(api_handler) = &api_handler {
                info!("Shutting down API server...");
                api_handler.stop().unwrap();
            }
            if let Some(snapshotter_handle) = snapshotter_handle {
                info!("Shutting down snapshotter...");
                snapshotter_handle.abort();
                snapshotter_handle
                    .await
                    .expect_err("Snapshotter should have been aborted");
            }
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }
    // We need to wait for the API server to stop to ensure that all clean up is done
    if let Some(api_handler) = api_handler {
        tokio::spawn(api_handler.stopped());
    }
}
