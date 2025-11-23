use std::fs::File;

use async_std::stream::StreamExt;
use async_stream::stream;
use clap::Parser;
use futures::pin_mut;
use jsonrpsee::server::ServerHandle;
use log::{error, info};
use photon_indexer::api::{self, api::PhotonApi};

use photon_indexer::common::{
    fetch_block_parent_slot, fetch_current_slot_with_infinite_retry, get_network_start_slot,
    get_rpc_client, setup_logging, setup_metrics, setup_pg_pool, LoggingFormat,
};

use photon_indexer::ingester::fetchers::BlockStreamConfig;
use photon_indexer::ingester::indexer::{
    fetch_last_indexed_slot_with_infinite_retry, index_block_stream,
};
use photon_indexer::migration::{
    sea_orm::{DatabaseBackend, DatabaseConnection, SqlxPostgresConnector, SqlxSqliteConnector},
    Migrator, MigratorTrait,
};

use photon_indexer::monitor::continously_monitor_photon;
use photon_indexer::snapshot::{
    get_snapshot_files_with_metadata, load_block_stream_from_directory_adapter, DirectoryAdapter,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};
use std::env::temp_dir;
use std::sync::Arc;

/// Photon: a compressed transaction Solana indexer
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Port to expose the local Photon API
    // We use a random default port to avoid conflicts with other services
    #[arg(short, long, default_value_t = 8784)]
    port: u16,

    /// Port for the gRPC API server (optional, if not provided gRPC server won't start)
    #[arg(long)]
    grpc_port: Option<u16>,

    /// URL of the RPC server
    #[arg(short, long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    /// DB URL to store indexing data. By default we use an in-memory SQLite database.
    #[arg(short, long)]
    db_url: Option<String>,

    /// The start slot to begin indexing from. Defaults to the last indexed slot in the database plus
    /// one.
    #[arg(short, long)]
    start_slot: Option<String>,

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

    /// GCS bucket name for loading snapshots. The bucket must already exist.
    /// Credentials must be provided via Application Default Credentials (ADC) or environment variables.
    #[arg(long)]
    gcs_bucket: Option<String>,

    /// GCS prefix for snapshot files. All snapshots will be loaded from this prefix in the GCS bucket.
    #[arg(long, default_value = "")]
    gcs_prefix: String,

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

    /// Custom account compression program ID (optional)
    #[arg(long, default_value = "compr6CUsB5m2jS4Y3831ztGSTnDpnKJTKS95d64XVq")]
    compression_program_id: String,

    /// Metrics endpoint in the format `host:port`
    /// If provided, metrics will be sent to the specified statsd server.
    #[arg(long, default_value = None)]
    metrics_endpoint: Option<String>,
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

fn continously_index_new_blocks(
    block_stream_config: BlockStreamConfig,
    db: Arc<DatabaseConnection>,
    rpc_client: Arc<RpcClient>,
    last_indexed_slot: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let block_stream = block_stream_config.load_block_stream();
        index_block_stream(
            block_stream,
            db,
            rpc_client.clone(),
            last_indexed_slot,
            None,
        )
        .await;
    })
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_logging(args.logging_format);
    setup_metrics(args.metrics_endpoint);

    if let Err(err) =
        photon_indexer::ingester::parser::set_compression_program_id(&args.compression_program_id)
    {
        error!("Failed to set compression program ID: {}", err);
        std::process::exit(1);
    }

    if let Some(expected_owner) = photon_indexer::ingester::parser::EXPECTED_TREE_OWNER {
        info!("Filtering trees by owner: {}", expected_owner);
    }

    let db_conn = setup_database_connection(args.db_url.clone(), args.max_db_conn).await;
    if args.db_url.is_none() {
        info!("Running migrations...");
        Migrator::up(db_conn.as_ref(), None).await.unwrap();
    }
    let is_rpc_node_local = args.rpc_url.contains("127.0.0.1");
    let rpc_client = get_rpc_client(&args.rpc_url);

    // Load snapshot if provided (either from local directory or GCS)
    let directory_adapter = match (args.snapshot_dir.clone(), args.gcs_bucket.clone()) {
        (Some(snapshot_dir), None) => Some(Arc::new(DirectoryAdapter::from_local_directory(
            snapshot_dir,
        ))),
        (None, Some(gcs_bucket)) => Some(Arc::new(
            DirectoryAdapter::from_gcs_bucket_and_prefix_and_env(
                gcs_bucket,
                args.gcs_prefix.clone(),
            )
            .await,
        )),
        (None, None) => None,
        (Some(_), Some(_)) => {
            error!("Cannot specify both snapshot_dir and gcs_bucket");
            std::process::exit(1);
        }
    };

    if let Some(directory_adapter) = directory_adapter {
        let snapshot_files = get_snapshot_files_with_metadata(&directory_adapter)
            .await
            .unwrap();
        if !snapshot_files.is_empty() {
            // Sync tree metadata from on-chain before processing snapshot
            // This is REQUIRED so the indexer knows about all existing trees
            info!("Syncing tree metadata from on-chain before loading snapshot...");
            if let Err(e) = photon_indexer::monitor::tree_metadata_sync::sync_tree_metadata(
                rpc_client.as_ref(),
                db_conn.as_ref(),
            )
            .await
            {
                error!(
                    "Failed to sync tree metadata: {}. Cannot proceed with snapshot loading.",
                    e
                );
                error!("Tree metadata must be synced before loading snapshots to avoid skipping transactions.");
                std::process::exit(1);
            }
            info!("Tree metadata sync completed successfully");

            info!("Detected snapshot files. Loading snapshot...");
            let last_slot = snapshot_files.last().unwrap().end_slot;
            let block_stream =
                load_block_stream_from_directory_adapter(directory_adapter.clone()).await;
            pin_mut!(block_stream);
            let first_blocks = block_stream.next().await.unwrap();
            let last_indexed_slot = first_blocks.first().unwrap().metadata.parent_slot;
            let block_stream = stream! {
                yield first_blocks;
                while let Some(blocks) = block_stream.next().await {
                    yield blocks;
                }
            };
            index_block_stream(
                block_stream,
                db_conn.clone(),
                rpc_client.clone(),
                last_indexed_slot,
                Some(last_slot),
            )
            .await;
        }
    }

    let (indexer_handle, monitor_handle) = match args.disable_indexing {
        true => {
            info!("Indexing is disabled");
            (None, None)
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
                Some(start_slot) => match start_slot.as_str() {
                    "latest" => fetch_current_slot_with_infinite_retry(&rpc_client).await,
                    _ => {
                        fetch_block_parent_slot(&rpc_client, start_slot.parse::<u64>().unwrap())
                            .await
                    }
                },
                None => fetch_last_indexed_slot_with_infinite_retry(db_conn.as_ref())
                    .await
                    .unwrap_or(
                        get_network_start_slot(&rpc_client)
                            .await
                            .try_into()
                            .unwrap(),
                    )
                    .try_into()
                    .unwrap(),
            };

            let block_stream_config = BlockStreamConfig {
                rpc_client: rpc_client.clone(),
                max_concurrent_block_fetches,
                last_indexed_slot,
                geyser_url: args.grpc_url,
            };

            (
                Some(continously_index_new_blocks(
                    block_stream_config,
                    db_conn.clone(),
                    rpc_client.clone(),
                    last_indexed_slot,
                )),
                Some(continously_monitor_photon(
                    db_conn.clone(),
                    rpc_client.clone(),
                )),
            )
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

            if let Some(monitor_handle) = monitor_handle {
                info!("Shutting down monitor...");
                monitor_handle.abort();
                monitor_handle
                    .await
                    .expect_err("Monitor should have been aborted");
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
