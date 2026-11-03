use std::time::Duration;

use rand::Rng;
use solana_client::{client_error::ClientErrorKind, rpc_request::RpcError};

/// Retry delays grow to 30 seconds, with jitter to avoid synchronized workers.
#[derive(Default)]
pub struct Backoff {
    attempts: u32,
}

impl Backoff {
    pub fn next_delay(&mut self) -> Duration {
        let ceiling = 500u64.saturating_mul(1 << self.attempts.min(6)).min(30_000);
        self.attempts = self.attempts.saturating_add(1);
        Duration::from_millis(rand::thread_rng().gen_range(ceiling / 2..=ceiling))
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }
}

/// Missing history and skipped-slot responses are provider-specific; another
/// endpoint may have the block. They must never advance a checkpoint by themselves.
pub fn rpc_error_is_retryable(error: &solana_client::client_error::ClientError) -> bool {
    match error.kind() {
        ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. }) => {
            !matches!(code, -32602..=-32600 | -32700 | -32015)
        }
        ClientErrorKind::Reqwest(error) => error
            .status()
            .is_none_or(|status| status.is_server_error() || matches!(status.as_u16(), 408 | 429)),
        ClientErrorKind::SerdeJson(_) | ClientErrorKind::Custom(_) => false,
        _ => true,
    }
}
