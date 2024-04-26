use sea_orm::DatabaseConnection;

use solana_client::nonblocking::rpc_client::RpcClient;

use super::super::error::PhotonApiError;
use super::utils::Context;

// TODO: Make this an environment variable.
const HEALTH_CHECK_SLOT_DISTANCE: i64 = 5;

// TODO: Make sure that get_indexer_health formatting matches the Solana RPC formatting.
pub async fn get_indexer_health(
    conn: &DatabaseConnection,
    rpc: &RpcClient,
) -> Result<String, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let slot = rpc
        .get_slot()
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("RPC error: {}", e)))?;

    let slots_behind = slot as i64 - context.slot as i64;
    if slots_behind > HEALTH_CHECK_SLOT_DISTANCE {
        return Err(PhotonApiError::StaleSlot(slots_behind as u64));
    }
    Ok("ok".to_string())
}
