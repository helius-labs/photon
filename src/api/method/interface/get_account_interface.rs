use sea_orm::DatabaseConnection;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;

use super::racing::race_hot_cold;
use super::types::{GetAccountInterfaceRequest, GetAccountInterfaceResponse};

/// Get account data from either on-chain or compressed sources.
/// Races both lookups and returns the result with the higher slot.
pub async fn get_account_interface(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    request: GetAccountInterfaceRequest,
) -> Result<GetAccountInterfaceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let value = race_hot_cold(rpc_client, conn, &request.address).await?;

    Ok(GetAccountInterfaceResponse { context, value })
}
