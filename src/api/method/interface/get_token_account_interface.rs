use std::time::Duration;

use light_compressed_account::address::derive_address;
use light_sdk_types::constants::ADDRESS_TREE_V2;
use light_token_interface::state::Token;
use light_zero_copy::traits::ZeroCopyAt;
use sea_orm::{sea_query::Condition, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
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
use crate::ingester::persist::LIGHT_TOKEN_PROGRAM_ID;

use super::racing::split_discriminator;
use super::types::{
    AccountInterface, ColdContext, ColdData, GetTokenAccountInterfaceRequest,
    GetTokenAccountInterfaceResponse, SolanaAccountData, TokenAccountInterface, TreeInfo,
    DB_TIMEOUT_MS, RPC_TIMEOUT_MS,
};

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
        Ok(Err(e)) => Err(PhotonApiError::UnexpectedError(format!(
            "RPC error during hot token lookup: {}",
            e
        ))),
        Err(_) => Err(PhotonApiError::UnexpectedError(
            "Timeout during hot token lookup".to_string(),
        )),
    }
}

/// Cold lookup for compressed token account.
/// Derives the compressed address from the on-chain pubkey and queries both
/// the Address column (compressed address) and OnchainPubkey column.
async fn cold_token_lookup(
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<Option<ColdTokenData>, PhotonApiError> {
    let onchain_pubkey_bytes: Vec<u8> = (*address).into();
    let compressed_address = derive_address(
        &address.0.to_bytes(),
        &ADDRESS_TREE_V2,
        &LIGHT_TOKEN_PROGRAM_ID.to_bytes(),
    );

    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        accounts::Entity::find()
            .filter(
                Condition::all().add(accounts::Column::Spent.eq(false)).add(
                    Condition::any()
                        .add(accounts::Column::Address.eq(compressed_address.to_vec()))
                        .add(accounts::Column::OnchainPubkey.eq(onchain_pubkey_bytes)),
                ),
            )
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
                data: account.data.clone(),
                owner: account.owner.clone().try_into()?,
                lamports: parse_decimal(account.lamports)?,
                tree: account.tree.clone().try_into()?,
                leaf_index: crate::api::method::utils::parse_leaf_index(account.leaf_index)?,
                seq: account.seq.map(|s| s as u64),
                token_data,
            };
            Ok(Some(cold_data))
        }
        Ok(Ok(None)) => Ok(None),
        Ok(Err(e)) => Err(PhotonApiError::DatabaseError(e)),
        Err(_) => Err(PhotonApiError::UnexpectedError(
            "Timeout during cold token lookup".to_string(),
        )),
    }
}

/// Compressed token data with parsed token info
#[derive(Debug)]
struct ColdTokenData {
    hash: Hash,
    data: Option<Vec<u8>>,
    owner: SerializablePubkey,
    lamports: u64,
    tree: SerializablePubkey,
    leaf_index: u64,
    seq: Option<u64>,
    token_data: TokenData,
}

/// Resolve the token race result between hot (on-chain CToken) and cold (compressed) lookups.
/// Prefers hot if account exists on-chain with lamports > 0 and is owned by LIGHT_TOKEN_PROGRAM_ID.
fn resolve_token_race(
    hot_result: Result<Option<(SolanaAccount, u64)>, PhotonApiError>,
    cold_result: Result<Option<ColdTokenData>, PhotonApiError>,
    address: SerializablePubkey,
) -> Result<Option<TokenAccountInterface>, PhotonApiError> {
    match (hot_result, cold_result) {
        // Both succeeded — prefer hot
        (Ok(hot), Ok(cold)) => {
            if let Some((account, slot)) = hot {
                if account.lamports > 0 {
                    if let Some(interface) = parse_light_token(&account, slot, address)? {
                        return Ok(Some(interface));
                    }
                    // Fall through to cold if hot parse returned None
                }
            }
            if let Some(cold_data) = cold {
                return Ok(cold_to_token_interface(&cold_data, address));
            }
            Ok(None)
        }
        // Only hot succeeded
        (Ok(hot), Err(e)) => {
            log::warn!("Cold token lookup failed: {:?}", e);
            match hot {
                Some((account, slot)) => parse_light_token(&account, slot, address),
                None => Ok(None),
            }
        }
        // Only cold succeeded
        (Err(e), Ok(cold)) => {
            log::warn!("Hot token lookup failed: {:?}", e);
            match cold {
                Some(cold_data) => Ok(cold_to_token_interface(&cold_data, address)),
                None => Ok(None),
            }
        }
        // Both failed - return the hot error
        (Err(hot_err), Err(cold_err)) => {
            log::warn!(
                "Both hot and cold token lookups failed. Hot: {:?}, Cold: {:?}",
                hot_err,
                cold_err
            );
            Err(hot_err)
        }
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
    _slot: u64,
    token_data: TokenData,
) -> TokenAccountInterface {
    TokenAccountInterface {
        account: AccountInterface {
            key: address,
            account: SolanaAccountData {
                lamports: UnsignedInteger(account.lamports),
                data: Base64String(account.data.clone()),
                owner: SerializablePubkey::from(account.owner.to_bytes()),
                executable: account.executable,
                rent_epoch: UnsignedInteger(account.rent_epoch),
            },
            cold: None,
        },
        token_data,
    }
}

/// Convert compressed token to TokenAccountInterface.
/// Always uses `query_address` (the on-chain Solana pubkey) as the key,
/// since `cold.address` is the compressed address (Poseidon hash), not the on-chain pubkey.
fn cold_to_token_interface(
    cold: &ColdTokenData,
    query_address: SerializablePubkey,
) -> Option<TokenAccountInterface> {
    let address = query_address;
    let raw_data = cold.data.clone().unwrap_or_default();
    let (discriminator, data_payload) = split_discriminator(&raw_data);

    Some(TokenAccountInterface {
        account: AccountInterface {
            key: address,
            account: SolanaAccountData {
                lamports: UnsignedInteger(cold.lamports),
                data: Base64String(raw_data),
                owner: cold.owner,
                executable: false,
                rent_epoch: UnsignedInteger(0),
            },
            cold: Some(ColdContext::Token {
                hash: cold.hash.clone(),
                leaf_index: UnsignedInteger(cold.leaf_index),
                tree_info: TreeInfo {
                    tree: cold.tree,
                    seq: cold.seq.map(UnsignedInteger),
                },
                data: ColdData {
                    discriminator,
                    data: Base64String(data_payload),
                },
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
