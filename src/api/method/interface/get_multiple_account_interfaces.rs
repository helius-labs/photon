use std::time::Duration;

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use tokio::time::timeout;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::mint_data::MintData;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::{mints, token_accounts};
use crate::ingester::persist::LIGHT_TOKEN_PROGRAM_ID;

use super::get_account_interface::get_account_interface;
use super::get_mint_interface::get_mint_interface;
use super::get_token_account_interface::get_token_account_interface;
use super::types::{
    GetAccountInterfaceRequest, GetMintInterfaceRequest, GetMultipleAccountInterfacesRequest,
    GetMultipleAccountInterfacesResponse, GetTokenAccountInterfaceRequest, InterfaceResult,
    DB_TIMEOUT_MS, MAX_BATCH_SIZE, RPC_TIMEOUT_MS,
};

/// Get multiple account data from either on-chain or compressed sources.
/// Server auto-detects account type (account, token, mint) and returns heterogeneous results.
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
    Mint,
    Token,
    Account,
}

/// Auto-detect account type and fetch with appropriate parsing.
/// Detection order:
/// 1. Check mints table (by mint_pda) → Mint (compressed)
/// 2. Check token_accounts table (by address) → Token (compressed)
/// 3. Check on-chain if owned by LIGHT_TOKEN_PROGRAM_ID → Mint or Token (decompressed)
/// 4. Default → Account
async fn detect_and_fetch(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    address: SerializablePubkey,
) -> Result<Option<InterfaceResult>, PhotonApiError> {
    let detected_type = detect_type(conn, rpc_client, &address).await?;

    match detected_type {
        DetectedType::Mint => {
            let mint_response =
                get_mint_interface(conn, rpc_client, GetMintInterfaceRequest { address }).await?;
            Ok(mint_response.value.map(InterfaceResult::Mint))
        }
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

/// Detect account type by checking database tables and on-chain data.
/// Detection order:
/// 1. Check mints table (by mint_pda) → Mint (compressed)
/// 2. Check token_accounts table (by address) → Token (compressed)
/// 3. Check on-chain if owned by LIGHT_TOKEN_PROGRAM_ID → Mint or Token (decompressed)
/// 4. Default → Account
async fn detect_type(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    address: &SerializablePubkey,
) -> Result<DetectedType, PhotonApiError> {
    let address_bytes: Vec<u8> = (*address).into();

    // Check mints table first (by mint_pda - the external address users query)
    let mint_exists = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        mints::Entity::find()
            .filter(mints::Column::MintPda.eq(address_bytes.clone()))
            .filter(mints::Column::Spent.eq(false))
            .one(conn),
    )
    .await;

    if let Ok(Ok(Some(_))) = mint_exists {
        return Ok(DetectedType::Mint);
    }

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

    // Check on-chain for decompressed Light Protocol accounts
    let onchain_type = detect_type_onchain(rpc_client, address).await;
    if let Ok(Some(detected)) = onchain_type {
        return Ok(detected);
    }

    // Default to generic account
    Ok(DetectedType::Account)
}

/// Detect account type from on-chain data.
/// Only detects Light Protocol accounts (CMint/CToken) owned by LIGHT_TOKEN_PROGRAM_ID.
async fn detect_type_onchain(
    rpc_client: &RpcClient,
    address: &SerializablePubkey,
) -> Result<Option<DetectedType>, PhotonApiError> {
    let pubkey = Pubkey::from(address.0.to_bytes());

    let result = timeout(
        Duration::from_millis(RPC_TIMEOUT_MS),
        rpc_client.get_account_with_commitment(&pubkey, CommitmentConfig::confirmed()),
    )
    .await;

    let account = match result {
        Ok(Ok(response)) => response.value,
        _ => return Ok(None),
    };

    let Some(account) = account else {
        return Ok(None);
    };

    // Only handle Light Protocol accounts
    if account.owner != LIGHT_TOKEN_PROGRAM_ID {
        return Ok(None);
    }

    // SPL Token/Mint accounts store an account type discriminator at byte offset 165.
    // 1 = Mint, 2 = Token Account.
    const ACCOUNT_TYPE_OFFSET: usize = 165;
    const ACCOUNT_TYPE_MINT: u8 = 1;
    const ACCOUNT_TYPE_TOKEN: u8 = 2;

    if account.data.len() > ACCOUNT_TYPE_OFFSET {
        match account.data[ACCOUNT_TYPE_OFFSET] {
            ACCOUNT_TYPE_MINT => return Ok(Some(DetectedType::Mint)),
            ACCOUNT_TYPE_TOKEN => return Ok(Some(DetectedType::Token)),
            _ => {}
        }
    }

    // Fallback: try structural parsing
    if MintData::parse(&account.data).is_ok() {
        return Ok(Some(DetectedType::Mint));
    }

    // If not a mint, assume it's a token (CToken)
    Ok(Some(DetectedType::Token))
}
