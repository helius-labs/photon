//! Optional endpoint failover; HTTP, JSON-RPC and 429 retries stay in the SDK.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use serde_json::Value;
use solana_client::{
    client_error::{ClientError, ClientErrorKind, Result as ClientResult},
    nonblocking::rpc_client::RpcClient,
    rpc_client::RpcClientConfig,
    rpc_request::{RpcError, RpcRequest},
    rpc_sender::{RpcSender, RpcTransportStats},
};

pub fn with_fallbacks(primary: Arc<RpcClient>, urls: &[String]) -> Arc<RpcClient> {
    if urls.is_empty() {
        return primary;
    }
    let config = RpcClientConfig::with_commitment(primary.commitment());
    let clients = std::iter::once(primary)
        .chain(urls.iter().map(|url| super::get_rpc_client(url)))
        .collect();
    Arc::new(RpcClient::new_sender(
        FailoverSender {
            clients,
            preferred: AtomicUsize::new(0),
        },
        config,
    ))
}

struct FailoverSender {
    clients: Vec<Arc<RpcClient>>,
    preferred: AtomicUsize,
}

#[async_trait]
impl RpcSender for FailoverSender {
    async fn send(&self, request: RpcRequest, params: Value) -> ClientResult<Value> {
        let start = self.preferred.load(Ordering::Relaxed);
        let full_block = request == RpcRequest::GetBlock
            && params
                .get(1)
                .and_then(|config| config.get("transactionDetails"))
                .and_then(Value::as_str)
                .unwrap_or("full")
                == "full";
        let mut last_error = None;
        // Do not replay mutating requests after an ambiguous transport failure.
        let attempts = if request.to_string().starts_with("get") {
            self.clients.len()
        } else {
            1
        };
        for offset in 0..attempts {
            let index = (start + offset) % self.clients.len();
            let result = self.clients[index]
                .send::<Value>(request, params.clone())
                .await
                .and_then(|value| {
                    if full_block && !value["transactions"].is_array() {
                        Err(
                            ClientErrorKind::Custom("Incomplete full-block RPC response".into())
                                .into(),
                        )
                    } else {
                        Ok(value)
                    }
                });
            match result {
                Ok(value) => {
                    self.preferred.store(index, Ordering::Relaxed);
                    return Ok(value);
                }
                Err(error) => {
                    // Preserve SDK error types without credential-bearing transport URLs.
                    let kind = match *error.kind {
                        ClientErrorKind::Reqwest(error) => {
                            ClientErrorKind::Reqwest(error.without_url())
                        }
                        kind => kind,
                    };
                    let error = ClientError {
                        request: error.request,
                        kind: Box::new(kind),
                    };
                    log::warn!(
                        "RPC method {} failed on endpoint {}: {}",
                        request,
                        index,
                        error
                    );
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            RpcError::RpcRequestError("No RPC endpoint available".into()).into()
        }))
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        let mut total = RpcTransportStats::default();
        for client in &self.clients {
            let stats = client.get_transport_stats();
            total.request_count += stats.request_count;
            total.elapsed_time += stats.elapsed_time;
            total.rate_limited_time += stats.rate_limited_time;
        }
        total
    }

    fn url(&self) -> String {
        self.clients[self.preferred.load(Ordering::Relaxed)].url()
    }
}
