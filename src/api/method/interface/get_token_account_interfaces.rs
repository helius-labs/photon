use sea_orm::DatabaseConnection;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;

use super::get_token_account_interface::get_token_account_interface;
use super::types::{
    GetTokenAccountInterfaceRequest, GetTokenAccountInterfacesRequest,
    GetTokenAccountInterfacesResponse, MAX_BATCH_SIZE,
};

/// Batch fetch token account data from either on-chain or compressed sources.
pub async fn get_token_account_interfaces(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    request: GetTokenAccountInterfacesRequest,
) -> Result<GetTokenAccountInterfacesResponse, PhotonApiError> {
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
        .map(|address| {
            get_token_account_interface(
                conn,
                rpc_client,
                GetTokenAccountInterfaceRequest { address: *address },
            )
        })
        .collect();

    let results = futures::future::join_all(futures).await;

    let value = results
        .into_iter()
        .enumerate()
        .map(|(i, r)| match r {
            Ok(resp) => resp.value,
            Err(e) => {
                log::warn!(
                    "Failed to fetch token account interface for {:?} (index {}): {:?}",
                    request.addresses.get(i),
                    i,
                    e
                );
                None
            }
        })
        .collect();

    Ok(GetTokenAccountInterfacesResponse { context, value })
}
