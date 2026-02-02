use std::time::Duration;

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use solana_account::Account as SolanaAccount;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use tokio::time::timeout;

use crate::api::error::PhotonApiError;
use crate::api::method::get_compressed_mint::mint_model_to_mint_data;
use crate::api::method::utils::parse_decimal;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::mint_data::MintData;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::{accounts, mints};
use crate::ingester::persist::LIGHT_TOKEN_PROGRAM_ID;

use super::racing::split_discriminator;
use super::types::{
    AccountInterface, ColdContext, ColdData, GetMintInterfaceRequest, GetMintInterfaceResponse,
    MintInterface, SolanaAccountData, TreeInfo, TreeType, DB_TIMEOUT_MS, RPC_TIMEOUT_MS,
};

/// Get mint account data from either on-chain or compressed sources.
pub async fn get_mint_interface(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    request: GetMintInterfaceRequest,
) -> Result<GetMintInterfaceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let pubkey = Pubkey::from(request.address.0.to_bytes());

    // Run both lookups concurrently
    let (hot_result, cold_result) = tokio::join!(
        hot_mint_lookup(rpc_client, &pubkey),
        cold_mint_lookup(conn, &request.address)
    );

    let value = resolve_mint_race(hot_result, cold_result, request.address)?;

    Ok(GetMintInterfaceResponse { context, value })
}

/// Hot lookup for on-chain CMint account
/// Only looks for accounts owned by LIGHT_TOKEN_PROGRAM_ID.
async fn hot_mint_lookup(
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
            "RPC error during hot mint lookup: {}",
            e
        ))),
        Err(_) => Err(PhotonApiError::UnexpectedError(
            "Timeout during hot mint lookup".to_string(),
        )),
    }
}

async fn cold_mint_lookup(
    conn: &DatabaseConnection,
    mint_pda: &SerializablePubkey,
) -> Result<Option<ColdMintData>, PhotonApiError> {
    let mint_pda_bytes: Vec<u8> = (*mint_pda).into();

    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        mints::Entity::find()
            .find_also_related(accounts::Entity)
            .filter(mints::Column::MintPda.eq(mint_pda_bytes))
            .filter(mints::Column::Spent.eq(false))
            .filter(accounts::Column::Spent.eq(false))
            .one(conn),
    )
    .await;

    match result {
        Ok(Ok(Some((mint, Some(account))))) => {
            let mint_data = mint_model_to_mint_data(&mint)?;

            let tree: SerializablePubkey = account.tree.clone().try_into()?;
            let queue: SerializablePubkey = account
                .queue
                .clone()
                .map(SerializablePubkey::try_from)
                .transpose()?
                .unwrap_or(tree);
            let tree_type = account
                .tree_type
                .map(TreeType::from)
                .unwrap_or(TreeType::StateV1);

            let cold_data = ColdMintData {
                hash: account.hash.clone().try_into()?,
                data: account.data.clone(),
                owner: account.owner.clone().try_into()?,
                lamports: parse_decimal(account.lamports)?,
                tree,
                queue,
                tree_type,
                leaf_index: crate::api::method::utils::parse_leaf_index(account.leaf_index)?,
                seq: account.seq.map(|s| s as u64),
                mint_data,
            };
            Ok(Some(cold_data))
        }
        Ok(Ok(_)) => Ok(None),
        Ok(Err(e)) => Err(PhotonApiError::DatabaseError(e)),
        Err(_) => Err(PhotonApiError::UnexpectedError(
            "Timeout during cold mint lookup".to_string(),
        )),
    }
}

#[derive(Debug)]
struct ColdMintData {
    hash: Hash,
    data: Option<Vec<u8>>,
    owner: SerializablePubkey,
    lamports: u64,
    tree: SerializablePubkey,
    queue: SerializablePubkey,
    tree_type: TreeType,
    leaf_index: u64,
    seq: Option<u64>,
    mint_data: MintData,
}

/// Resolve the mint race result between hot (on-chain CMint) and cold (compressed) lookups.
/// Prefers hot if account exists on-chain with lamports > 0.
fn resolve_mint_race(
    hot_result: Result<Option<(SolanaAccount, u64)>, PhotonApiError>,
    cold_result: Result<Option<ColdMintData>, PhotonApiError>,
    address: SerializablePubkey,
) -> Result<Option<MintInterface>, PhotonApiError> {
    match (hot_result, cold_result) {
        // Both succeeded — prefer hot
        (Ok(hot), Ok(cold)) => {
            if let Some((account, slot)) = hot {
                if account.lamports > 0 {
                    if let Some(interface) = parse_light_mint(&account, slot, address)? {
                        return Ok(Some(interface));
                    }
                    // Fall through to cold if hot parse returned None
                }
            }
            if let Some(cold_data) = cold {
                return Ok(cold_to_mint_interface(&cold_data, address));
            }
            Ok(None)
        }
        // Only hot succeeded
        (Ok(hot), Err(e)) => {
            log::warn!("Cold mint lookup failed: {:?}", e);
            match hot {
                Some((account, slot)) => parse_light_mint(&account, slot, address),
                None => Ok(None),
            }
        }
        // Only cold succeeded
        (Err(e), Ok(cold)) => {
            log::warn!("Hot mint lookup failed: {:?}", e);
            match cold {
                Some(cold_data) => Ok(cold_to_mint_interface(&cold_data, address)),
                None => Ok(None),
            }
        }
        // Both failed - return the hot error
        (Err(hot_err), Err(cold_err)) => {
            log::warn!(
                "Both hot and cold mint lookups failed. Hot: {:?}, Cold: {:?}",
                hot_err,
                cold_err
            );
            Err(hot_err)
        }
    }
}

/// Parse Light Protocol CMint (decompressed mint) from on-chain account
fn parse_light_mint(
    account: &SolanaAccount,
    slot: u64,
    address: SerializablePubkey,
) -> Result<Option<MintInterface>, PhotonApiError> {
    let mint_data = parse_light_mint_data(&account.data, address)?;
    match mint_data {
        Some(md) => Ok(Some(hot_to_mint_interface(account, address, slot, md))),
        None => Ok(None),
    }
}

/// Convert on-chain mint to MintInterface
fn hot_to_mint_interface(
    account: &SolanaAccount,
    address: SerializablePubkey,
    _slot: u64,
    mint_data: MintData,
) -> MintInterface {
    MintInterface {
        account: AccountInterface {
            key: address,
            account: SolanaAccountData {
                lamports: UnsignedInteger(account.lamports),
                data: Base64String(account.data.clone()),
                owner: SerializablePubkey::from(account.owner.to_bytes()),
                executable: account.executable,
                rent_epoch: UnsignedInteger(account.rent_epoch),
                space: UnsignedInteger(account.data.len() as u64),
            },
            cold: None,
        },
        mint_data,
    }
}

/// Convert compressed mint to MintInterface.
/// Uses `query_address` as fallback when the account has no address field.
fn cold_to_mint_interface(
    cold: &ColdMintData,
    query_address: SerializablePubkey,
) -> Option<MintInterface> {
    let address = query_address;
    let raw_data = cold.data.clone().unwrap_or_default();
    let space = raw_data.len() as u64;
    let (discriminator, data_payload) = split_discriminator(&raw_data);

    Some(MintInterface {
        account: AccountInterface {
            key: address,
            account: SolanaAccountData {
                lamports: UnsignedInteger(cold.lamports),
                data: Base64String(raw_data),
                owner: cold.owner,
                executable: false,
                rent_epoch: UnsignedInteger(0),
                space: UnsignedInteger(space),
            },
            cold: Some(ColdContext::Mint {
                hash: cold.hash.clone(),
                leaf_index: UnsignedInteger(cold.leaf_index),
                tree_info: TreeInfo {
                    tree: cold.tree,
                    queue: cold.queue,
                    tree_type: cold.tree_type,
                    seq: cold.seq.map(UnsignedInteger),
                },
                data: ColdData {
                    discriminator,
                    data: Base64String(data_payload),
                },
            }),
        },
        mint_data: cold.mint_data.clone(),
    })
}

/// Parse Light Protocol CMint data (decompressed compressed mint)
fn parse_light_mint_data(
    data: &[u8],
    mint_pda: SerializablePubkey,
) -> Result<Option<MintData>, PhotonApiError> {
    // Use the existing MintData::parse which handles Light Protocol mint format
    match MintData::parse(data) {
        Ok(mut mint_data) => {
            // Set the mint_pda from the address we queried
            mint_data.mint_pda = mint_pda;
            Ok(Some(mint_data))
        }
        Err(e) => {
            log::debug!("Failed to parse Light mint data: {:?}", e);
            Ok(None)
        }
    }
}
