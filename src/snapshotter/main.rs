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
use photon_indexer::common::{
    fetch_block_parent_slot, get_network_start_slot, setup_logging, setup_metrics, LoggingFormat,
};

use photon_indexer::ingester::fetchers::BlockStreamConfig;
use photon_indexer::ingester::indexer::{
    fetch_last_indexed_slot_with_infinite_retry, index_block_stream,
};
use photon_indexer::migration::{
    sea_orm::{DatabaseBackend, DatabaseConnection, SqlxPostgresConnector, SqlxSqliteConnector},
    Migrator, MigratorTrait,
};
use photon_indexer::openapi::update_docs;

use photon_indexer::snapshotter::get_snapshot_files_with_slots;
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

/// Photon: a compressed transaction Solana indexer
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Port to expose the local snapshotter API
    // We use a random default port to avoid conflicts with other services
    #[arg(short, long, default_value_t = 8825)]
    port: u16,

    /// URL of the RPC server
    #[arg(short, long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    /// The start slot to begin indexing from. Defaults to the last indexed slot in the snapshots
    /// plus one.
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// Logging format
    #[arg(short, long, default_value_t = LoggingFormat::Standard)]
    logging_format: LoggingFormat,

    /// Max number of blocks to fetch concurrently. Generally, this should be set to be as high
    /// as possible without reaching RPC rate limits.
    #[arg(short, long)]
    max_concurrent_block_fetches: Option<usize>,

    /// Snasphot directory
    #[arg(long)]
    snapshot_dir: String,

    /// Incremental snapshot slots
    #[arg(long, default_value_t = 1000)]
    incremental_snapshot_interval_slots: u64,

    /// Fullsnapshot slots
    #[arg(long, default_value_t = 100_000)]
    snapshot_interval_slots: u64,

    #[arg(short, long, default_value = None)]
    /// Yellowstone gRPC URL. If it's inputed, then the indexer will use gRPC to fetch new blocks
    /// instead of polling. It will still use RPC to fetch blocks if
    grpc_url: Option<String>,

    /// Metrics endpoint in the format `host:port`
    /// If provided, metrics will be sent to the specified statsd server.
    #[arg(long, default_value = None)]
    metrics_endpoint: Option<String>,
}

async fn continously_run_snapshotter(
    block_stream_config: BlockStreamConfig,
    full_snapshot_interval_slots: u64,
    incremental_snapshot_interval_slots: u64,
    snapshot_dir: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        photon_indexer::snapshotter::update_snapshot(
            block_stream_config,
            incremental_snapshot_interval_slots,
            full_snapshot_interval_slots,
            Path::new(&snapshot_dir),
        )
        .await;
    })
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_logging(args.logging_format);
    setup_metrics(args.metrics_endpoint);

    let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
        args.rpc_url.clone(),
        Duration::from_secs(10),
        CommitmentConfig::confirmed(),
    ));

    info!("Starting snapshotter...");

    let last_indexed_slot = match args.start_slot {
        Some(start_slot) => fetch_block_parent_slot(rpc_client.clone(), start_slot).await,
        None => {
            let snapshot_files =
                get_snapshot_files_with_slots(Path::new(&args.snapshot_dir)).unwrap();
            if snapshot_files.is_empty() {
                get_network_start_slot(rpc_client.clone()).await
            } else {
                snapshot_files.last().unwrap().end_slot
            }
        }
    };

    let snapshotter_handle = continously_run_snapshotter(
        BlockStreamConfig {
            rpc_client: rpc_client.clone(),
            max_concurrent_block_fetches: args.max_concurrent_block_fetches.unwrap_or(20),
            last_indexed_slot: last_indexed_slot,
            geyser_url: args.grpc_url.clone(),
        },
        args.incremental_snapshot_interval_slots,
        args.snapshot_interval_slots,
        args.snapshot_dir,
    )
    .await;

    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            snapshotter_handle.abort();
            snapshotter_handle
                .await
                .expect_err("Snapshotter should have been aborted");
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }
}
