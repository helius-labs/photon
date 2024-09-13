use std::sync::Arc;

use async_stream::stream;
use futures::{pin_mut, Stream, StreamExt};

use crate::common::typedefs::rpc_client_with_uri::RpcClientWithUri;

use super::typedefs::block_info::BlockInfo;

pub mod grpc;
pub mod poller;

use grpc::get_grpc_stream_with_rpc_fallback;
use poller::get_poller_block_stream;

pub struct BlockStreamConfig {
    pub rpc_client: Arc<RpcClientWithUri>,
    pub geyser_url: Option<String>,
    pub max_concurrent_block_fetches: usize,
    pub last_indexed_slot: u64,
}

impl BlockStreamConfig {
    pub fn load_block_stream(&self) -> impl Stream<Item = Vec<BlockInfo>> {
        let grpc_stream = self.geyser_url.as_ref().map(|geyser_url| {
            get_grpc_stream_with_rpc_fallback(
                geyser_url.clone(),
                self.rpc_client.clone(),
                self.last_indexed_slot,
                self.max_concurrent_block_fetches,
            )
        });

        let poller_stream = if self.geyser_url.is_none() {
            Some(get_poller_block_stream(
                self.rpc_client.clone(),
                self.last_indexed_slot,
                self.max_concurrent_block_fetches,
                None,
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
