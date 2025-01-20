use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use log::{error, info};
use photon_indexer::common::{setup_logging, LoggingFormat};
use photon_indexer::snapshot::{
    create_snapshot_from_byte_stream,get_snapshot_files_with_metadata, DirectoryAdapter,
};
use std::path::Path;
use std::sync::Arc;

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

    /// Minmum snapshot age to fetching from
    #[arg(long, default_value = "1000")]
    min_snapshot_age: Option<u64>,

    /// Logging format
    #[arg(short, long, default_value_t = LoggingFormat::Standard)]
    logging_format: LoggingFormat,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    setup_logging(args.logging_format);

    let snapshot_dir = &args.snapshot_dir;

    // Create snapshot directory if it doesn't exist
    if !Path::new(snapshot_dir).exists() {
        std::fs::create_dir_all(snapshot_dir).unwrap();
    }

    let http_client = reqwest::Client::new();

    // Get the snapshots from the local directory
    let directory_adapter = Arc::new(DirectoryAdapter::from_local_directory(snapshot_dir.clone()));
    let snapshot_files = get_snapshot_files_with_metadata(&directory_adapter)
        .await
        .unwrap();
    if !snapshot_files.is_empty() {
        info!("Detected snapshot files. Loading snapshot...");
        // Fetch the maximum end_slot from snapshot_files
        let latest_snapshot = snapshot_files
            .iter()
            .max_by_key(|file| file.end_slot)
            .unwrap();

        // Get the remote snapshot
        let response = http_client
            .get(format!("{}/slot", args.snapshot_server_url))
            .send()
            .await
            .unwrap();

        if response.status().is_success() {
            // REad response body and return it as an integre
            let remote_end_slot = response
                .text()
                .await
                .unwrap()
                .parse::<u64>()
                .context("Failed to parse remote end slot")?;


            if remote_end_slot <= latest_snapshot.end_slot+args.min_snapshot_age.unwrap_or(0) {
                info!("Local snapshot is up to date");
                return Ok(());
            }

        } else {
            error!("Failed to query snapshot status")
        }
    }

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
