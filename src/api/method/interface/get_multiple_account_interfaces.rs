use sea_orm::DatabaseConnection;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;

use super::racing::race_hot_cold;
use super::types::{
    GetMultipleAccountInterfacesRequest, GetMultipleAccountInterfacesResponse, MAX_BATCH_SIZE,
};

/// Get multiple account data from either on-chain or compressed sources.
/// Returns one unified AccountInterface shape for every input pubkey.
pub async fn get_multiple_account_interfaces(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    request: GetMultipleAccountInterfacesRequest,
) -> Result<GetMultipleAccountInterfacesResponse, PhotonApiError> {
    if request.addresses.len() > MAX_BATCH_SIZE {
        return Err(PhotonApiError::ValidationError(format!(
            "Batch size {} exceeds maximum of {}",
            request.addresses.len(),
            MAX_BATCH_SIZE
        )));
    }

    if request.addresses.is_empty() {
        return Err(PhotonApiError::ValidationError(
            "At least one address must be provided".to_string(),
        ));
    }

    let context = Context::extract(conn).await?;

    let futures: Vec<_> = request
        .addresses
        .iter()
        .map(|address| race_hot_cold(rpc_client, conn, address))
        .collect();

    let results = futures::future::join_all(futures).await;

    let value = results
        .into_iter()
        .enumerate()
        .map(|(i, r)| match r {
            Ok(v) => v,
            Err(e) => {
                log::warn!(
                    "Failed to fetch interface for address {:?} (index {}): {:?}",
                    request.addresses.get(i),
                    i,
                    e
                );
                None
            }
        })
        .collect();

    Ok(GetMultipleAccountInterfacesResponse { context, value })
}
