use clap::Parser;
use futures::StreamExt;
use log::{error, info};
use photon_indexer::common::{
    fetch_block_parent_slot, get_network_start_slot, setup_logging, setup_metrics, LoggingFormat,
};
use photon_indexer::ingester::fetchers::BlockStreamConfig;
use photon_indexer::snapshot::{
    get_snapshot_files_with_metadata, load_byte_stream_from_directory_adapter, DirectoryAdapter,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use std::convert::Infallible;
use tower::ServiceBuilder;

/// Photon Snapshotter: a utility to create snapshots of Photon's state at regular intervals.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Port to expose the local snapshotter API
    #[arg(short, long, default_value_t = 8825)]
    port: u16,

    /// URL of the RPC server
    #[arg(short, long, default_value = "http://127.0.0.1:8899")]
    rpc_url: String,

    /// The start slot to begin indexing from
    #[arg(short, long)]
    start_slot: Option<u64>,

    /// Logging format
    #[arg(short, long, default_value_t = LoggingFormat::Standard)]
    logging_format: LoggingFormat,

    /// Max number of blocks to fetch concurrently
    #[arg(short, long)]
    max_concurrent_block_fetches: Option<usize>,

    /// Snapshot directory
    #[arg(long)]
    snapshot_dir: Option<String>,

    /// R2 bucket name. The bucket must already exist. The endpoint url, region, access keys, and
    /// secret keys must be provided in the environment variables.
    #[arg(long)]
    r2_bucket: Option<String>,

    /// R2 prefix. All snapshots will be stored under this prefix in the R2 bucket.
    #[arg(long, default_value = "")]
    r2_prefix: String,

    /// Incremental snapshot slots
    #[arg(long, default_value_t = 1000)]
    incremental_snapshot_interval_slots: u64,

    /// Full snapshot slots
    #[arg(long, default_value_t = 100_000)]
    snapshot_interval_slots: u64,

    /// Yellowstone gRPC URL
    #[arg(short, long, default_value = None)]
    grpc_url: Option<String>,

    /// Metrics endpoint in the format `host:port`
    #[arg(long, default_value = None)]
    metrics_endpoint: Option<String>,

    /// Disable snapshot generation and only serve snapshots
    #[arg(long, default_value_t = false)]
    disable_snapshot_generation: bool,

    /// Disable api server
    #[arg(long, default_value_t = false)]
    disable_api: bool,
}

async fn continously_run_snapshotter(
    directory_adapter: Arc<DirectoryAdapter>,
    block_stream_config: BlockStreamConfig,
    full_snapshot_interval_slots: u64,
    incremental_snapshot_interval_slots: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        photon_indexer::snapshot::update_snapshot(
            directory_adapter,
            block_stream_config,
            incremental_snapshot_interval_slots,
            full_snapshot_interval_slots,
        )
        .await;
    })
}

async fn stream_bytes(
    directory_adapter: Arc<DirectoryAdapter>,
) -> Result<Response<Body>, hyper::http::Error> {
    let byte_stream = load_byte_stream_from_directory_adapter(directory_adapter).await;
    info!("Finished loading byte stream");
    let byte_stream = byte_stream.map(|bytes| {
        bytes.map_err(|e| {
            error!("Error reading byte: {:?}", e);
            io::Error::new(io::ErrorKind::Other, "Stream Error")
        })
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .body(Body::wrap_stream(byte_stream))
}

async fn fetch_slot(
    directory_adapter: Arc<DirectoryAdapter>,
) -> Result<Response<hyper::Body>, hyper::http::Error> {
    let snapshot_files = get_snapshot_files_with_metadata(directory_adapter.as_ref()).await;

    match snapshot_files {
        Ok(snapshot_files) => {
            let last_snapshot = snapshot_files.last();
            match last_snapshot {
                Some(snapshot) => Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(snapshot.end_slot.to_string())),
                None => Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::from("No snapshots found")),
            }
        }
        Err(e) => {
            error!("Error fetching snapshot files: {:?}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("Internal Server Error"))
        }
    }
}

async fn handle_request(
    req: Request<Body>,
    directory_adapter: Arc<DirectoryAdapter>,
) -> Result<Response<Body>, hyper::http::Error> {
    match req.uri().path() {
        "/download" => match stream_bytes(directory_adapter).await {
            Ok(response) => Ok(response),
            Err(e) => {
                error!("Error creating stream: {:?}", e);
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("Internal Server Error"))
            }
        },
        "/health" | "/readiness" | "/healthz" => Response::builder()
            .status(StatusCode::OK)
            .body(Body::from("OK")),
        "/slot" => fetch_slot(directory_adapter).await,
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("404 Not Found")),
    }
    .map_err(|e| {
        error!("Error building response: {:?}", e);
        e
    })
}
async fn create_server(
    port: u16,
    directory_adapter: Arc<DirectoryAdapter>,
) -> tokio::task::JoinHandle<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    // Spawn the server task
    tokio::spawn(async move {
        let make_svc = make_service_fn(move |_conn| {
            let layer = ServiceBuilder::new();
            let directory_adapter = directory_adapter.clone();
            async move {
                Ok::<_, Infallible>(layer.service(service_fn(move |req| {
                    handle_request(req, directory_adapter.clone())
                })))
            }
        });

        let server = Server::bind(&addr).serve(make_svc);
        info!("Listening on http://{}", addr);

        if let Err(e) = server.await {
            error!("Server error: {}", e);
        }
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

    let directory_adapter = match (args.snapshot_dir.clone(), args.r2_bucket.clone()) {
        (Some(snapshot_dir), None) => {
            Arc::new(DirectoryAdapter::from_local_directory(snapshot_dir))
        }
        (None, Some(r2_bucket)) => Arc::new(
            DirectoryAdapter::from_r2_bucket_and_prefix_and_env(r2_bucket, args.r2_prefix.clone())
                .await,
        ),
        _ => {
            error!("Either snapshot_dir or r2_bucket must be provided");
            return;
        }
    };
    let snapshotter_handle = if args.disable_snapshot_generation {
        None
    } else {
        info!("Starting snapshotter...");
        let snapshot_files = get_snapshot_files_with_metadata(directory_adapter.as_ref())
            .await
            .unwrap();
        let last_indexed_slot = match args.start_slot {
            Some(start_slot) => {
                if !snapshot_files.is_empty() {
                    panic!("Cannot specify start_slot when snapshot files are present");
                }
                fetch_block_parent_slot(rpc_client.clone(), start_slot).await
            }
            None => {
                if snapshot_files.is_empty() {
                    get_network_start_slot(rpc_client.clone()).await
                } else {
                    snapshot_files.last().unwrap().end_slot
                }
            }
        };
        info!("Starting from slot: {}", last_indexed_slot + 1);
        Some(
            continously_run_snapshotter(
                directory_adapter.clone(),
                BlockStreamConfig {
                    rpc_client: rpc_client.clone(),
                    max_concurrent_block_fetches: args.max_concurrent_block_fetches.unwrap_or(20),
                    last_indexed_slot,
                    geyser_url: args.grpc_url.clone(),
                },
                args.incremental_snapshot_interval_slots,
                args.snapshot_interval_slots,
            )
            .await,
        )
    };
    let server_handle = if args.disable_api {
        None
    } else {
        Some(create_server(args.port, directory_adapter.clone()).await)
    };

    // Handle shutdown signal
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            if let Some(snapshotter_handle) = snapshotter_handle {
                snapshotter_handle.abort();
                snapshotter_handle
                    .await
                    .expect_err("Snapshotter should have been aborted");
            }
            if let Some(server_handle) = server_handle {
                server_handle.abort();
                server_handle
                    .await
                    .expect_err("Server should have been aborted");
            }
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }
}
