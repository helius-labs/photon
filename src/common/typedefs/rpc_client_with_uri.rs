use std::time::Duration;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

// We use this because for some reason Arc<RpcClient> fetches blocks slowly when doing concurrent indexing
pub struct RpcClientWithUri {
    pub client: RpcClient,
    pub uri: String,
}

impl RpcClientWithUri {
    pub fn new(uri: String) -> Self {
        let client = RpcClient::new_with_timeout_and_commitment(
            uri.clone(),
            Duration::from_secs(10),
            CommitmentConfig::confirmed(),
        );
        Self { client, uri }
    }
}
