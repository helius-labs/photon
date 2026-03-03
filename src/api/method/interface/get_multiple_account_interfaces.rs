use sea_orm::DatabaseConnection;
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::Semaphore;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;

use super::racing::{get_distinct_owners_with_addresses, race_hot_cold};
use super::types::{
    AccountInterface, GetMultipleAccountInterfacesRequest, GetMultipleAccountInterfacesResponse,
    MAX_BATCH_SIZE,
};

/// Maximum concurrent hot+cold lookups per batch request.
const MAX_CONCURRENT_LOOKUPS: usize = 20;

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

    let distinct_owners = get_distinct_owners_with_addresses(conn)
        .await
        .map_err(PhotonApiError::DatabaseError)?;

    let semaphore = Semaphore::new(MAX_CONCURRENT_LOOKUPS);
    let futures: Vec<_> = request
        .addresses
        .iter()
        .map(|address| async {
            let _permit = semaphore.acquire().await.unwrap();
            race_hot_cold(rpc_client, conn, address, Some(&distinct_owners)).await
        })
        .collect();

    let results = futures::future::join_all(futures).await;

    let value = collect_batch_results(&request.addresses, results)?;

    Ok(GetMultipleAccountInterfacesResponse { context, value })
}

fn collect_batch_results(
    addresses: &[SerializablePubkey],
    results: Vec<Result<Option<AccountInterface>, PhotonApiError>>,
) -> Result<Vec<Option<AccountInterface>>, PhotonApiError> {
    let mut value = Vec::with_capacity(results.len());
    for (i, result) in results.into_iter().enumerate() {
        match result {
            // Includes Ok(None): account not found is returned as None.
            Ok(account) => value.push(account),
            // Only actual lookup failures abort the entire batch call.
            Err(e) => {
                log::error!(
                    "Failed to fetch interface for address {:?} (index {}): {:?}",
                    addresses.get(i),
                    i,
                    e
                );
                return Err(e);
            }
        }
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_batch_results_keeps_none_for_not_found_accounts() {
        let addresses = vec![SerializablePubkey::default(), SerializablePubkey::default()];
        let results = vec![Ok(None), Ok(None)];

        let value = collect_batch_results(&addresses, results).expect("expected success");
        assert_eq!(value, vec![None, None]);
    }

    #[test]
    fn collect_batch_results_returns_error_for_actual_failure() {
        let addresses = vec![SerializablePubkey::default()];
        let results = vec![Err(PhotonApiError::UnexpectedError("boom".to_string()))];

        let err = collect_batch_results(&addresses, results).expect_err("expected error");
        assert_eq!(err, PhotonApiError::UnexpectedError("boom".to_string()));
    }
}
