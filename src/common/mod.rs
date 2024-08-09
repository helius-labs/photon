use core::fmt;
use std::{env, net::UdpSocket, path::PathBuf, sync::Arc, thread::sleep, time::Duration};

use cadence::{BufferedUdpMetricSink, QueuingMetricSink, StatsdClient};
use cadence_macros::set_global_default;
use clap::{Parser, ValueEnum};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
pub mod typedefs;

pub fn relative_project_path(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

#[macro_export]
macro_rules! metric {
    {$($block:stmt;)*} => {
        use cadence_macros::is_global_default_set;
        if is_global_default_set() {
            $(
                $block
            )*
        }
    };
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

pub async fn get_genesis_hash_with_infinite_retry(rpc_client: &RpcClient) -> String {
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

pub async fn fetch_block_parent_slot(rpc_client: Arc<RpcClient>, slot: u64) -> u64 {
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

pub async fn get_network_start_slot(rpc_client: Arc<RpcClient>) -> u64 {
    let genesis_hash = get_genesis_hash_with_infinite_retry(rpc_client.as_ref()).await;
    match genesis_hash.as_str() {
        // Devnet
        "EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG" => 310809099 - 1,
        // Mainnet
        "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d" => 277957074 - 1,
        _ => 0,
    }
}

#[derive(Parser, Debug, Clone, ValueEnum)]
pub enum LoggingFormat {
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

pub fn setup_logging(logging_format: LoggingFormat) {
    let env_filter =
        env::var("RUST_LOG").unwrap_or("info,sqlx=error,sea_orm_migration=error".to_string());
    let subscriber = tracing_subscriber::fmt().with_env_filter(env_filter);
    match logging_format {
        LoggingFormat::Standard => subscriber.init(),
        LoggingFormat::Json => subscriber.json().init(),
    }
}
