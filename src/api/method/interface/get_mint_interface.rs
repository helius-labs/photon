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

use super::types::{
    AccountInterface, CompressedContext, GetMintInterfaceRequest, GetMintInterfaceResponse,
    MintInterface, ResolvedFrom, DB_TIMEOUT_MS, RPC_TIMEOUT_MS,
};

/// Light Token Program ID (for CMint accounts / decompressed mints)
const LIGHT_TOKEN_PROGRAM_ID: Pubkey =
    solana_pubkey::pubkey!("cTokenmWW8bLPjZEBAUgYy3zKxQZW6VKi7bqNFEVv3m");

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
        Ok(Err(e)) => {
            log::debug!("RPC error during hot mint lookup: {:?}", e);
            Ok(None)
        }
        Err(_) => {
            log::debug!("Timeout during hot mint lookup");
            Ok(None)
        }
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
            .one(conn),
    )
    .await;

    match result {
        Ok(Ok(Some((mint, Some(account))))) => {
            let mint_data = mint_model_to_mint_data(&mint)?;

            let cold_data = ColdMintData {
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
                mint_data,
            };
            Ok(Some(cold_data))
        }
        Ok(Ok(_)) => Ok(None),
        Ok(Err(e)) => Err(PhotonApiError::DatabaseError(e)),
        Err(_) => {
            log::debug!("Timeout during cold mint lookup");
            Ok(None)
        }
    }
}

#[derive(Debug)]
struct ColdMintData {
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
    mint_data: MintData,
}

/// Resolve the mint race result between hot (on-chain CMint) and cold (compressed) lookups
fn resolve_mint_race(
    hot_result: Result<Option<(SolanaAccount, u64)>, PhotonApiError>,
    cold_result: Result<Option<ColdMintData>, PhotonApiError>,
    address: SerializablePubkey,
) -> Result<Option<MintInterface>, PhotonApiError> {
    let hot = hot_result.ok().flatten();
    let cold = cold_result.ok().flatten();

    match (hot, cold) {
        // Both have data - return higher slot
        (Some((account, hot_slot)), Some(cold_data)) => {
            if hot_slot >= cold_data.slot_created {
                // Use on-chain CMint data
                parse_light_mint(&account, hot_slot, address)
            } else {
                // Use compressed data (returns None if no address - unexpected for mints)
                Ok(cold_to_mint_interface(&cold_data))
            }
        }
        // Only hot has data (on-chain CMint)
        (Some((account, slot)), None) => parse_light_mint(&account, slot, address),
        // Only cold has data (returns None if no address - unexpected for mints)
        (None, Some(cold_data)) => Ok(cold_to_mint_interface(&cold_data)),
        // Neither has data
        (None, None) => Ok(None),
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
    slot: u64,
    mint_data: MintData,
) -> MintInterface {
    MintInterface {
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
        mint_data,
    }
}

/// Convert compressed mint to MintInterface.
/// Returns None if the account is missing its address (unexpected for mints).
fn cold_to_mint_interface(cold: &ColdMintData) -> Option<MintInterface> {
    let address = match cold.address {
        Some(addr) => addr,
        None => {
            // Mints should always have addresses (they're unique PDAs)
            log::warn!(
                "Compressed mint account unexpectedly missing address field for hash: {:?}. \
                This indicates a data inconsistency.",
                cold.hash
            );
            return None;
        }
    };

    Some(MintInterface {
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