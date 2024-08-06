use anyhow::Context;
use async_std::stream::StreamExt;
use clap::Parser;
use log::{error, info};
use photon_indexer::common::{
    setup_logging, LoggingFormat,
};
use std::fs::OpenOptions;
use std::io::Write;
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
        .get(&format!("{}/download", args.snapshot_server_url))
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

    // File path for the snapshot
    let snapshot_file_path = Path::new(&args.snapshot_dir).join("snapshot.dat");

    // Open the file for writing
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&snapshot_file_path)
        .context("Failed to open snapshot file")
        .unwrap();

    // Stream the response body to the file
    let mut stream = response.bytes_stream();

    // Skip snapshot version byte
    stream.next().await;

    // Process the byte stream
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                // Write the chunk to the file
                if let Err(e) = file.write_all(&bytes) {
                    error!("Error writing to file: {:?}", e);
                    return Err(anyhow::anyhow!("Failed to write to file"));
                }
            }
            Err(e) => {
                error!("Error receiving chunk: {:?}", e);
                return Err(anyhow::anyhow!("Failed to receive data chunk"));
            }
        }
    }

    info!(
        "Snapshot downloaded successfully to {:?}",
        snapshot_file_path
    );

    Ok(())
}
