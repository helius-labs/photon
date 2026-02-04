use std::time::Duration;

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::time::timeout;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::token_accounts;

use super::get_account_interface::get_account_interface;
use super::get_token_account_interface::get_token_account_interface;
use super::types::{
    GetAccountInterfaceRequest, GetMultipleAccountInterfacesRequest,
    GetMultipleAccountInterfacesResponse, GetTokenAccountInterfaceRequest, InterfaceResult,
    DB_TIMEOUT_MS, MAX_BATCH_SIZE,
};

/// Get multiple account data from either on-chain or compressed sources.
/// Server auto-detects account type (account, token) and returns heterogeneous results.
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
        .map(|address| detect_and_fetch(conn, rpc_client, *address))
        .collect();

    let results = futures::future::join_all(futures).await;

    let value: Vec<Option<InterfaceResult>> = results
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetectedType {
    Token,
    Account,
}

/// Auto-detect account type and fetch with appropriate parsing.
/// Detection order:
/// 1. Check token_accounts table (by address) → Token (compressed)
/// 2. Default → Account
async fn detect_and_fetch(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    address: SerializablePubkey,
) -> Result<Option<InterfaceResult>, PhotonApiError> {
    let detected_type = detect_type(conn, &address).await?;

    match detected_type {
        DetectedType::Token => {
            let token_response = get_token_account_interface(
                conn,
                rpc_client,
                GetTokenAccountInterfaceRequest { address },
            )
            .await?;
            Ok(token_response.value.map(InterfaceResult::Token))
        }
        DetectedType::Account => {
            let account_response =
                get_account_interface(conn, rpc_client, GetAccountInterfaceRequest { address })
                    .await?;
            Ok(account_response.value.map(InterfaceResult::Account))
        }
    }
}

/// Detect account type by checking database tables.
/// Detection order:
/// 1. Check token_accounts table (by address) → Token (compressed)
/// 2. Default → Account
async fn detect_type(
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<DetectedType, PhotonApiError> {
    let address_bytes: Vec<u8> = (*address).into();

    // Check token_accounts table by joining with accounts on address OR onchain_pubkey
    // This supports both direct address lookups and generic linking via onchain_pubkey
    use crate::dao::generated::accounts;
    use sea_orm::Condition;
    let token_exists = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        token_accounts::Entity::find()
            .inner_join(accounts::Entity)
            .filter(
                Condition::any()
                    .add(accounts::Column::Address.eq(address_bytes.clone()))
                    .add(accounts::Column::OnchainPubkey.eq(address_bytes.clone())),
            )
            .filter(token_accounts::Column::Spent.eq(false))
            .one(conn),
    )
    .await;

    if let Ok(Ok(Some(_))) = token_exists {
        return Ok(DetectedType::Token);
    }

    // Default to generic account
    Ok(DetectedType::Account)
}
