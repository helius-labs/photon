use std::fs::File;

use async_std::stream::StreamExt;
use async_stream::stream;
use clap::Parser;
use futures::pin_mut;
use jsonrpsee::server::ServerHandle;
use log::{error, info};

use photon_indexer::common::{
    fetch_block_parent_slot, fetch_current_slot_with_infinite_retry, get_network_start_slot,
    get_rpc_client, setup_logging, setup_metrics, setup_pg_pool, LoggingFormat,
};

use photon_indexer::ingester::fetchers::BlockStreamConfig;
use photon_indexer::ingester::indexer::index_block_stream;

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
    /// URL of the RPC server
    #[arg(short, long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    /// The start slot to begin indexing from. Defaults to the last indexed slot in the database plus
    /// one.  
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// Logging format
    #[arg(short, long, default_value_t = LoggingFormat::Standard)]
    logging_format: LoggingFormat,

    #[arg(short, long, default_value = None)]
    /// Yellowstone gRPC URL. If it's inputed, then the indexer will use gRPC to fetch new blocks
    /// instead of polling. It will still use RPC to fetch blocks if
    grpc_url: Option<String>,

    /// Metrics endpoint in the format `host:port`
    /// If provided, metrics will be sent to the specified statsd server.
    #[arg(long, default_value = None)]
    metrics_endpoint: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    setup_logging(args.logging_format);
    setup_metrics(args.metrics_endpoint);

    let rpc_client = get_rpc_client(&args.rpc_url);
    let max_concurrent_block_fetches = 20;
    let start_slot = args
        .start_slot
        .unwrap_or(fetch_current_slot_with_infinite_retry(&rpc_client).await);
    let block_stream = BlockStreamConfig {
        rpc_client: rpc_client.clone(),
        geyser_url: args.grpc_url,
        max_concurrent_block_fetches,
        last_indexed_slot: start_slot,
    }
    .load_block_stream();
    index_block_stream(block_stream, rpc_client.clone(), start_slot).await;
}
