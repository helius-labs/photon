use sea_orm::DatabaseConnection;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;

use super::get_mint_interface::get_mint_interface;
use super::types::{
    GetMintAccountInterfacesRequest, GetMintAccountInterfacesResponse, GetMintInterfaceRequest,
    MAX_BATCH_SIZE,
};

/// Batch fetch mint account data from either on-chain or compressed sources.
pub async fn get_mint_account_interfaces(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    request: GetMintAccountInterfacesRequest,
) -> Result<GetMintAccountInterfacesResponse, PhotonApiError> {
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
            get_mint_interface(
                conn,
                rpc_client,
                GetMintInterfaceRequest { address: *address },
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
                    "Failed to fetch mint account interface for {:?} (index {}): {:?}",
                    request.addresses.get(i),
                    i,
                    e
                );
                None
            }
        })
        .collect();

    Ok(GetMintAccountInterfacesResponse { context, value })
}
