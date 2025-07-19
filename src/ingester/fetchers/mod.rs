use std::sync::Arc;

use async_stream::stream;
use futures::{pin_mut, Stream, StreamExt};
use solana_client::nonblocking::rpc_client::RpcClient;

use super::typedefs::block_info::BlockInfo;

pub mod grpc;
pub mod poller;

use grpc::get_grpc_stream_with_rpc_fallback;
use poller::get_block_poller_stream;

pub struct BlockStreamConfig {
    pub rpc_client: Arc<RpcClient>,
    pub geyser_url: Option<String>,
    pub max_concurrent_block_fetches: usize,
    pub last_indexed_slot: u64,
    pub canonical_rpc_client: Option<Arc<RpcClient>>,
    pub enable_canonical_validation: bool,
}

impl BlockStreamConfig {
    pub fn detect_canonical_url(primary_rpc_url: &str) -> Option<String> {
        if primary_rpc_url.contains("devnet") || primary_rpc_url.contains("api.devnet.solana.com") {
            Some("https://api.devnet.solana.com".to_string())
        } else if primary_rpc_url.contains("mainnet") || primary_rpc_url.contains("api.mainnet-beta.solana.com") {
            Some("https://api.mainnet-beta.solana.com".to_string())
        } else if primary_rpc_url.contains("testnet") {
            Some("https://api.testnet.solana.com".to_string())
        } else {
            None // Custom RPC, no auto-detection
        }
    }

    pub fn new(
        rpc_client: Arc<RpcClient>,
        geyser_url: Option<String>,
        max_concurrent_block_fetches: usize,
        last_indexed_slot: u64,
        canonical_rpc_url: Option<String>,
        enable_canonical_validation: bool,
    ) -> Self {
        let canonical_rpc_client = if enable_canonical_validation {
            let canonical_url = canonical_rpc_url.or_else(|| {
                Self::detect_canonical_url(&rpc_client.url())
            });
            
            canonical_url.map(|url| {
                Arc::new(RpcClient::new(url))
            })
        } else {
            None
        };

        Self {
            rpc_client,
            geyser_url,
            max_concurrent_block_fetches,
            last_indexed_slot,
            canonical_rpc_client,
            enable_canonical_validation,
        }
    }
    pub fn load_block_stream(&self) -> impl Stream<Item = Vec<BlockInfo>> {
        let grpc_stream = self.geyser_url.as_ref().map(|geyser_url| {
            let auth_header = std::env::var("GRPC_X_TOKEN").unwrap();
            get_grpc_stream_with_rpc_fallback(
                geyser_url.clone(),
                auth_header,
                self.rpc_client.clone(),
                self.last_indexed_slot,
                self.max_concurrent_block_fetches,
            )
        });

        let poller_stream = if self.geyser_url.is_none() {
            Some(get_block_poller_stream(
                self.rpc_client.clone(),
                self.last_indexed_slot,
                self.max_concurrent_block_fetches,
                self.canonical_rpc_client.clone(),
            ))
        } else {
            None
        };

        stream! {
            if let Some(grpc_stream) = grpc_stream {
                pin_mut!(grpc_stream);
                loop {
                    match grpc_stream.next().await {
                        Some(blocks) => yield blocks,
                        None => break,
                    }
                }
            }

            if let Some(poller_stream) = poller_stream {
                pin_mut!(poller_stream);
                loop {
                    match poller_stream.next().await {
                        Some(blocks) => yield blocks,
                        None => break,
                    }
                }
            }
        }
    }
}
