use schemars::JsonSchema;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;

use super::super::error::PhotonApiError;
use super::utils::Context;

// TODO: Make this an environment variable.
const HEALTH_CHECK_SLOT_DISTANCE: u64 = 5;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountBalance {
    value: i64,
    context: Context,
}

// TODO: Make sure that get_health formatting matches the Solana RPC formatting.
pub async fn get_health(conn: &DatabaseConnection, rpc: &RpcClient) -> Result<(), PhotonApiError> {
    let context = Context::extract(conn).await?;
    let slot = rpc
        .get_slot()
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("RPC error: {}", e)))?;

    let slots_behind = slot - context.slot;
    if slots_behind > HEALTH_CHECK_SLOT_DISTANCE {
        return Err(PhotonApiError::StaleSlot(slots_behind));
    }
    Ok(())
}
