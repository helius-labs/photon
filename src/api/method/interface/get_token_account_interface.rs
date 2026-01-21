use std::time::Duration;

use light_token_interface::state::Token;
use light_zero_copy::traits::ZeroCopyAt;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use solana_account::Account as SolanaAccount;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use tokio::time::timeout;

use crate::api::error::PhotonApiError;
use crate::api::method::utils::parse_decimal;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::token_data::{AccountState, TokenData};
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::accounts;

use super::types::{
    AccountInterface, CompressedContext, GetTokenAccountInterfaceRequest,
    GetTokenAccountInterfaceResponse, ResolvedFrom, TokenAccountInterface, DB_TIMEOUT_MS,
    RPC_TIMEOUT_MS,
};

/// Light Token Program ID (for CToken accounts / decompressed token accounts)
const LIGHT_TOKEN_PROGRAM_ID: Pubkey =
    solana_pubkey::pubkey!("cTokenmWW8bLPjZEBAUgYy3zKxQZW6VKi7bqNFEVv3m");

/// Get token account data from either on-chain (decompressed CToken) or compressed sources.
/// Races both lookups and returns the result with the higher slot.
pub async fn get_token_account_interface(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    request: GetTokenAccountInterfaceRequest,
) -> Result<GetTokenAccountInterfaceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let pubkey = Pubkey::from(request.address.0.to_bytes());

    let (hot_result, cold_result) = tokio::join!(
        hot_token_lookup(rpc_client, &pubkey),
        cold_token_lookup(conn, &request.address)
    );

    let value = resolve_token_race(hot_result, cold_result, request.address)?;

    Ok(GetTokenAccountInterfaceResponse { context, value })
}

/// Hot lookup for on-chain CToken account
/// Only looks for accounts owned by LIGHT_TOKEN_PROGRAM_ID.
async fn hot_token_lookup(
    rpc_client: &RpcClient,
    address: &Pubkey,
) -> Result<Option<(SolanaAccount, u64)>, PhotonApiError> {
    let result = timeout(
        Duration::from_millis(RPC_TIMEOUT_MS),
        rpc_client.get_account_with_commitment(address, CommitmentConfig::confirmed()),
    )
    .await;

    match result {
        Ok(Ok(response)) => {
            if let Some(account) = response.value {
                let slot = response.context.slot;
                if account.owner == LIGHT_TOKEN_PROGRAM_ID {
                    Ok(Some((account, slot)))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
        Ok(Err(e)) => {
            log::debug!("RPC error during hot token lookup: {:?}", e);
            Ok(None)
        }
        Err(_) => {
            log::debug!("Timeout during hot token lookup");
            Ok(None)
        }
    }
}

/// Cold lookup for compressed token account
async fn cold_token_lookup(
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<Option<ColdTokenData>, PhotonApiError> {
    let address_bytes: Vec<u8> = (*address).into();

    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        accounts::Entity::find()
            .filter(accounts::Column::Address.eq(address_bytes))
            .filter(accounts::Column::Spent.eq(false))
            .one(conn),
    )
    .await;

    match result {
        Ok(Ok(Some(account))) => {
            let token_data = match parse_compressed_token_data(account.data.as_deref()) {
                Ok(Some(td)) => td,
                Ok(None) => {
                    log::debug!(
                        "Account exists but is not a valid compressed token account: {:?}",
                        address
                    );
                    return Ok(None);
                }
                Err(e) => {
                    log::warn!(
                        "Failed to parse compressed token data for {:?}: {:?}",
                        address,
                        e
                    );
                    return Ok(None);
                }
            };

            let cold_data = ColdTokenData {
                hash: account.hash.clone().try_into()?,
                address: account
                    .address
                    .clone()
                    .map(SerializablePubkey::try_from)
                    .transpose()?,
                data: account.data.clone(),
                owner: account.owner.clone().try_into()?,
                lamports: parse_decimal(account.lamports)?,
                tree: account.tree.clone().try_into()?,
                leaf_index: crate::api::method::utils::parse_leaf_index(account.leaf_index)?,
                seq: account.seq.map(|s| s as u64),
                slot_created: account.slot_created as u64,
                prove_by_index: account.in_output_queue,
                token_data,
            };
            Ok(Some(cold_data))
        }
        Ok(Ok(None)) => Ok(None),
        Ok(Err(e)) => Err(PhotonApiError::DatabaseError(e)),
        Err(_) => {
            log::debug!("Timeout during cold token lookup");
            Ok(None)
        }
    }
}

/// Compressed token data with parsed token info
#[derive(Debug)]
struct ColdTokenData {
    hash: Hash,
    address: Option<SerializablePubkey>,
    data: Option<Vec<u8>>,
    owner: SerializablePubkey,
    lamports: u64,
    tree: SerializablePubkey,
    leaf_index: u64,
    seq: Option<u64>,
    slot_created: u64,
    prove_by_index: bool,
    token_data: TokenData,
}

/// Resolve the token race result between hot (on-chain CToken) and cold (compressed) lookups
fn resolve_token_race(
    hot_result: Result<Option<(SolanaAccount, u64)>, PhotonApiError>,
    cold_result: Result<Option<ColdTokenData>, PhotonApiError>,
    address: SerializablePubkey,
) -> Result<Option<TokenAccountInterface>, PhotonApiError> {
    let hot = hot_result.ok().flatten();
    let cold = cold_result.ok().flatten();

    match (hot, cold) {
        // Both have data - return higher slot
        (Some((account, hot_slot)), Some(cold_data)) => {
            if hot_slot >= cold_data.slot_created {
                // Use on-chain CToken data
                parse_light_token(&account, hot_slot, address)
            } else {
                // Use compressed data (returns None if no address)
                Ok(cold_to_token_interface(&cold_data))
            }
        }
        // Only hot has data (on-chain CToken)
        (Some((account, slot)), None) => parse_light_token(&account, slot, address),
        // Only cold has data (returns None if no address)
        (None, Some(cold_data)) => Ok(cold_to_token_interface(&cold_data)),
        // Neither has data
        (None, None) => Ok(None),
    }
}

/// Parse Light Protocol CToken (decompressed token account) from on-chain account
fn parse_light_token(
    account: &SolanaAccount,
    slot: u64,
    address: SerializablePubkey,
) -> Result<Option<TokenAccountInterface>, PhotonApiError> {
    let token_data = parse_light_token_data(&account.data)?;
    match token_data {
        Some(td) => Ok(Some(hot_to_token_interface(account, address, slot, td))),
        None => Ok(None),
    }
}

/// Convert on-chain CToken to TokenAccountInterface
fn hot_to_token_interface(
    account: &SolanaAccount,
    address: SerializablePubkey,
    slot: u64,
    token_data: TokenData,
) -> TokenAccountInterface {
    TokenAccountInterface {
        account: AccountInterface {
            address,
            lamports: UnsignedInteger(account.lamports),
            owner: SerializablePubkey::from(account.owner.to_bytes()),
            data: Base64String(account.data.clone()),
            executable: account.executable,
            rent_epoch: UnsignedInteger(account.rent_epoch),
            resolved_from: ResolvedFrom::Onchain,
            resolved_slot: UnsignedInteger(slot),
            compressed_context: None,
        },
        token_data,
    }
}

/// Convert compressed token to TokenAccountInterface.
/// Returns None if the account is missing its address (common for fungible token accounts).
fn cold_to_token_interface(cold: &ColdTokenData) -> Option<TokenAccountInterface> {
    let address = match cold.address {
        Some(addr) => addr,
        None => {
            log::debug!(
                "Compressed token account has no address field for hash: {:?}. \
                This is expected for fungible token accounts created via mint_to.",
                cold.hash
            );
            return None;
        }
    };

    Some(TokenAccountInterface {
        account: AccountInterface {
            address,
            lamports: UnsignedInteger(cold.lamports),
            owner: cold.owner,
            data: Base64String(cold.data.clone().unwrap_or_default()),
            executable: false,
            rent_epoch: UnsignedInteger(0),
            resolved_from: ResolvedFrom::Compressed,
            resolved_slot: UnsignedInteger(cold.slot_created),
            compressed_context: Some(CompressedContext {
                hash: cold.hash.clone(),
                tree: cold.tree,
                leaf_index: UnsignedInteger(cold.leaf_index),
                seq: cold.seq.map(UnsignedInteger),
                prove_by_index: cold.prove_by_index,
            }),
        },
        token_data: cold.token_data.clone(),
    })
}

/// Parse compressed token data using the existing TokenData parser
/// Compressed token accounts have an 8-byte discriminator prefix.
fn parse_compressed_token_data(data: Option<&[u8]>) -> Result<Option<TokenData>, PhotonApiError> {
    let data = match data {
        Some(d) if !d.is_empty() => d,
        _ => return Ok(None),
    };

    // Compressed token accounts have an 8-byte discriminator prefix
    if data.len() < 8 {
        return Ok(None);
    }

    match TokenData::parse(&data[8..]) {
        Ok(token_data) => Ok(Some(token_data)),
        Err(e) => {
            log::debug!("Failed to parse compressed token data: {:?}", e);
            Ok(None)
        }
    }
}

/// Parse Light Protocol CToken data (on-chain decompressed token account)
fn parse_light_token_data(data: &[u8]) -> Result<Option<TokenData>, PhotonApiError> {
    // CToken accounts need at least 165 bytes (SPL Token base size)
    if data.len() < 165 {
        return Ok(None);
    }

    // Use light-token-interface's zero-copy parser for SPL-compatible layout
    let (token, _remaining) = match Token::zero_copy_at(data) {
        Ok(result) => result,
        Err(e) => {
            log::debug!("Failed to parse CToken with zero_copy_at: {:?}", e);
            return Ok(None);
        }
    };

    // Check if token is initialized
    if token.base.is_uninitialized() {
        log::debug!("CToken account is uninitialized");
        return Ok(None);
    }

    // Convert ZToken to our TokenData struct
    let state = if token.base.is_frozen() {
        AccountState::frozen
    } else {
        AccountState::initialized
    };

    let delegate = token
        .base
        .delegate()
        .map(|d| SerializablePubkey::from(d.to_bytes()));

    // We don't return TLV extension data for on-chain CToken accounts.
    // On-chain accounts use raw SPL Token2022 TLV format, while compressed accounts
    // use Borsh-serialized Vec<ExtensionStruct>. These formats are incompatible,
    // so we set TLV to None for on-chain accounts to avoid deserialization errors.
    // The raw account data is still available in the `data` field if needed.
    let tlv = None;

    Ok(Some(TokenData {
        mint: SerializablePubkey::from(token.base.mint.to_bytes()),
        owner: SerializablePubkey::from(token.base.owner.to_bytes()),
        amount: UnsignedInteger(token.base.amount.into()),
        delegate,
        state,
        tlv,
    }))
}