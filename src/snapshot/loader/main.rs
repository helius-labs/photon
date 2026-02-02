use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use log::error;
use photon_indexer::common::{setup_logging, LoggingFormat};
use photon_indexer::snapshot::{create_snapshot_from_byte_stream, DirectoryAdapter};
use std::path::Path;

/// Photon Loader: a utility to load snapshots from a snapshot server
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Snapshot url
    #[arg(short, long)]
    snapshot_server_url: String,

    /// Snapshot directory
    #[arg(long)]
    snapshot_dir: String,

    /// Logging format
    #[arg(short, long, default_value_t = LoggingFormat::Standard)]
    logging_format: LoggingFormat,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    setup_logging(args.logging_format);

    // Create snapshot directory if it doesn't exist
    if !Path::new(&args.snapshot_dir).exists() {
        std::fs::create_dir_all(&args.snapshot_dir).unwrap();
    }

    let http_client = reqwest::Client::new();
    // Call the download snapshot endpoint
    let response = http_client
        .get(format!("{}/download", args.snapshot_server_url))
        .send()
        .await
        .unwrap();
    // Check if the response status is OK
    if !response.status().is_success() {
        error!("Failed to download snapshot: HTTP {}", response.status());
        return Err(anyhow::anyhow!(
            "HTTP request failed with status {}",
            response.status()
        ));
    }

    // Stream the response body to the file
    let stream = response
        .bytes_stream()
        .map(|byte| byte.with_context(|| "Failed to read byte stream from response body"));
    let directory_adapter = DirectoryAdapter::from_local_directory(args.snapshot_dir.clone());
    create_snapshot_from_byte_stream(stream, &directory_adapter).await?;

    Ok(())
}
