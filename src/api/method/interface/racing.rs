use std::time::Duration;

use light_compressed_account::address::derive_address;
use light_sdk_types::constants::{ADDRESS_TREE_V1, ADDRESS_TREE_V2};
use sea_orm::{sea_query::Condition, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use solana_account::Account as SolanaAccount;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use tokio::time::timeout;

use crate::api::error::PhotonApiError;
use crate::api::method::utils::parse_decimal;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::accounts;

use super::types::{
    AccountInterface, ColdContext, ColdData, SolanaAccountData, TreeInfo, TreeType, DB_TIMEOUT_MS,
    RPC_TIMEOUT_MS,
};

/// Result from a hot (on-chain RPC) lookup
#[derive(Debug)]
pub struct HotLookupResult {
    pub account: Option<SolanaAccount>,
    pub slot: u64,
}

/// Result from a cold (compressed DB) lookup
#[derive(Debug)]
pub struct ColdLookupResult {
    pub account: Option<ColdAccountData>,
    pub slot: u64,
}

/// Compressed account data from the database
#[derive(Debug, Clone)]
pub struct ColdAccountData {
    pub hash: Hash,
    pub address: Option<SerializablePubkey>,
    /// The 8-byte discriminator stored separately in the DB
    pub discriminator: Option<u64>,
    /// The account data (without discriminator)
    pub data: Option<Vec<u8>>,
    pub owner: SerializablePubkey,
    pub lamports: u64,
    pub tree: SerializablePubkey,
    /// The output queue pubkey (for V2 trees)
    pub queue: SerializablePubkey,
    /// The tree type (V1 or V2)
    pub tree_type: TreeType,
    pub leaf_index: u64,
    pub seq: Option<u64>,
    /// Slot when the account was created/compressed
    pub slot_created: u64,
}

/// Perform a hot lookup via Solana RPC
pub async fn hot_lookup(
    rpc_client: &RpcClient,
    address: &Pubkey,
) -> Result<HotLookupResult, PhotonApiError> {
    let result = timeout(
        Duration::from_millis(RPC_TIMEOUT_MS),
        rpc_client.get_account_with_commitment(address, CommitmentConfig::confirmed()),
    )
    .await;

    match result {
        Ok(Ok(response)) => Ok(HotLookupResult {
            account: response.value,
            slot: response.context.slot,
        }),
        Ok(Err(e)) => Err(PhotonApiError::UnexpectedError(format!("RPC error: {}", e))),
        Err(_) => Err(PhotonApiError::UnexpectedError("RPC timeout".to_string())),
    }
}

/// Perform a hot lookup for multiple accounts via Solana RPC
pub async fn hot_lookup_multiple(
    rpc_client: &RpcClient,
    addresses: &[Pubkey],
) -> Result<(Vec<Option<SolanaAccount>>, u64), PhotonApiError> {
    let result = timeout(
        Duration::from_millis(RPC_TIMEOUT_MS),
        rpc_client.get_multiple_accounts_with_commitment(addresses, CommitmentConfig::confirmed()),
    )
    .await;

    match result {
        Ok(Ok(response)) => Ok((response.value, response.context.slot)),
        Ok(Err(e)) => Err(PhotonApiError::UnexpectedError(format!("RPC error: {}", e))),
        Err(_) => Err(PhotonApiError::UnexpectedError("RPC timeout".to_string())),
    }
}

/// Perform a cold lookup from the compressed accounts database.
/// First searches by onchain_pubkey column (for accounts with DECOMPRESSED_PDA_DISCRIMINATOR).
/// If not found, searches by derived address column using the queried pubkey as PDA seed.
///
/// For derived address lookup, we query accounts that have addresses and check if
/// the address matches what would be derived from the query pubkey. This is done
/// efficiently by deriving addresses for distinct owners (program IDs) - typically
/// a small set of compressible programs.
///
/// ## Future Improvement
///
/// The current fallback approach derives addresses for all (owner, tree) combinations,
/// which is O(owners × trees). While efficient for small numbers of compressible programs,
/// this could be improved to O(1) by storing the PDA seed directly.
///
/// The challenge is that the PDA seed (original Solana pubkey used to derive the compressed
/// address via `derive_address(pda_seed, tree, owner)`) is not emitted in Light Protocol
/// compression events - it's only used transiently during compression.
///
/// To enable O(1) lookups, Light Protocol would need to:
/// 1. Emit the PDA seed in compression events (e.g., add `seed` field to `NewAddress` event)
/// 2. Photon would store this seed in an indexed `pda_seed` column
/// 3. Lookups would then be a simple indexed query: `WHERE pda_seed = ?`
///
/// See: light-protocol/sdk-libs/event/src/event.rs - `NewAddress` struct
pub async fn cold_lookup(
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<ColdLookupResult, PhotonApiError> {
    let address_bytes: Vec<u8> = (*address).into();
    let pda_seed = address.0.to_bytes();

    // First try: lookup by onchain_pubkey (for accounts with DECOMPRESSED_PDA_DISCRIMINATOR)
    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        accounts::Entity::find()
            .filter(accounts::Column::Spent.eq(false))
            .filter(accounts::Column::OnchainPubkey.eq(address_bytes.clone()))
            .one(conn),
    )
    .await;

    match result {
        Ok(Ok(Some(model))) => {
            return Ok(ColdLookupResult {
                account: Some(model_to_cold_data(&model)?),
                slot: model.slot_created as u64,
            });
        }
        Ok(Ok(None)) => {
            // Not found by onchain_pubkey, try fallback by derived address
        }
        Ok(Err(e)) => return Err(PhotonApiError::DatabaseError(e)),
        Err(_) => {
            return Err(PhotonApiError::UnexpectedError(
                "Database timeout".to_string(),
            ))
        }
    }

    // Second try: lookup by derived address (for fully compressed accounts)
    // We derive addresses for accounts that have an address column value.
    // The derivation uses: derive_address(pda_seed, ADDRESS_TREE_V2, owner)
    //
    // Strategy: Get distinct owners from accounts with non-null addresses,
    // derive the expected address for each owner, and query by those addresses.
    // This is efficient because:
    // 1. The number of distinct compressible programs (owners) is small
    // 2. The address column is indexed
    let owners_result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        get_distinct_owners_with_addresses(conn),
    )
    .await;

    let owners: Vec<Vec<u8>> = match owners_result {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => return Err(PhotonApiError::DatabaseError(e)),
        Err(_) => {
            return Err(PhotonApiError::UnexpectedError(
                "Database timeout getting owners".to_string(),
            ))
        }
    };

    if owners.is_empty() {
        return Ok(ColdLookupResult {
            account: None,
            slot: 0,
        });
    }

    // Derive addresses for each owner using the queried pubkey as PDA seed
    // Try both V1 and V2 address trees since accounts may use either
    let address_trees = [ADDRESS_TREE_V1, ADDRESS_TREE_V2];
    let derived_addresses: Vec<Vec<u8>> = owners
        .iter()
        .filter_map(|owner| owner.as_slice().try_into().ok())
        .flat_map(|owner_bytes: [u8; 32]| {
            address_trees
                .iter()
                .map(move |tree| derive_address(&pda_seed, tree, &owner_bytes).to_vec())
        })
        .collect();

    if derived_addresses.is_empty() {
        return Ok(ColdLookupResult {
            account: None,
            slot: 0,
        });
    }

    // Search by any of the derived addresses
    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        accounts::Entity::find()
            .filter(
                Condition::all()
                    .add(accounts::Column::Spent.eq(false))
                    .add(accounts::Column::Address.is_in(derived_addresses)),
            )
            .one(conn),
    )
    .await;

    match result {
        Ok(Ok(Some(model))) => Ok(ColdLookupResult {
            account: Some(model_to_cold_data(&model)?),
            slot: model.slot_created as u64,
        }),
        Ok(Ok(None)) => Ok(ColdLookupResult {
            account: None,
            slot: 0,
        }),
        Ok(Err(e)) => Err(PhotonApiError::DatabaseError(e)),
        Err(_) => Err(PhotonApiError::UnexpectedError(
            "Database timeout".to_string(),
        )),
    }
}

/// Convert a database model to ColdAccountData
fn model_to_cold_data(model: &accounts::Model) -> Result<ColdAccountData, PhotonApiError> {
    // Parse discriminator from BLOB (8 bytes little-endian) to u64
    let discriminator = model.discriminator.as_ref().and_then(|d| {
        if d.len() == 8 {
            let bytes: [u8; 8] = d.as_slice().try_into().ok()?;
            let disc = u64::from_le_bytes(bytes);
            log::info!(
                "model_to_cold_data: discriminator bytes={:?}, u64={}",
                bytes,
                disc
            );
            Some(disc)
        } else {
            log::warn!(
                "model_to_cold_data: discriminator has unexpected length {}, expected 8",
                d.len()
            );
            None
        }
    });

    // Parse queue - if not set, default to tree (V1 behavior)
    let tree: SerializablePubkey = model.tree.clone().try_into()?;
    let queue: SerializablePubkey = model
        .queue
        .clone()
        .map(SerializablePubkey::try_from)
        .transpose()?
        .unwrap_or(tree);

    // Parse tree_type - default to StateV1 if not set
    let tree_type = model
        .tree_type
        .map(TreeType::from)
        .unwrap_or(TreeType::StateV1);

    Ok(ColdAccountData {
        hash: model.hash.clone().try_into()?,
        address: model
            .address
            .clone()
            .map(SerializablePubkey::try_from)
            .transpose()?,
        discriminator,
        data: model.data.clone(),
        owner: model.owner.clone().try_into()?,
        lamports: parse_decimal(model.lamports)?,
        tree,
        queue,
        tree_type,
        leaf_index: crate::api::method::utils::parse_leaf_index(model.leaf_index)?,
        seq: model.seq.map(|s| s as u64),
        slot_created: model.slot_created as u64,
    })
}

/// Get distinct owners from accounts that have derived addresses.
/// These are accounts from compressible programs (their address is derived from PDA + tree + owner).
/// This is typically a small set since most programs aren't compressible.
async fn get_distinct_owners_with_addresses(
    conn: &DatabaseConnection,
) -> Result<Vec<Vec<u8>>, sea_orm::DbErr> {
    use sea_orm::{FromQueryResult, QuerySelect};

    #[derive(FromQueryResult)]
    struct OwnerResult {
        owner: Vec<u8>,
    }

    // Only get owners from accounts that have a non-null address column
    // (accounts with derived addresses from PDA seeds)
    let owners: Vec<OwnerResult> = accounts::Entity::find()
        .select_only()
        .column(accounts::Column::Owner)
        .distinct()
        .filter(accounts::Column::Spent.eq(false))
        .filter(accounts::Column::Address.is_not_null())
        .into_model::<OwnerResult>()
        .all(conn)
        .await?;

    Ok(owners.into_iter().map(|o| o.owner).collect())
}

/// Perform cold lookup for multiple addresses.
/// First searches by onchain_pubkey column (for accounts with DECOMPRESSED_PDA_DISCRIMINATOR).
/// For any not found, derives compressed addresses and searches by address column.
pub async fn cold_lookup_multiple(
    conn: &DatabaseConnection,
    addresses: &[SerializablePubkey],
) -> Result<Vec<Option<ColdAccountData>>, PhotonApiError> {
    let address_bytes: Vec<Vec<u8>> = addresses.iter().map(|a| (*a).into()).collect();

    // First try: lookup by onchain_pubkey
    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        accounts::Entity::find()
            .filter(accounts::Column::Spent.eq(false))
            .filter(accounts::Column::OnchainPubkey.is_in(address_bytes.clone()))
            .all(conn),
    )
    .await;

    let onchain_models = match result {
        Ok(Ok(models)) => models,
        Ok(Err(e)) => return Err(PhotonApiError::DatabaseError(e)),
        Err(_) => {
            return Err(PhotonApiError::UnexpectedError(
                "Database timeout".to_string(),
            ))
        }
    };

    // Create map of onchain_pubkey -> model for efficient lookup
    let mut onchain_pubkey_to_model: std::collections::HashMap<Vec<u8>, accounts::Model> =
        std::collections::HashMap::new();

    for model in onchain_models {
        if let Some(onchain) = &model.onchain_pubkey {
            onchain_pubkey_to_model.insert(onchain.clone(), model);
        }
    }

    // Find which addresses weren't found by onchain_pubkey
    let mut missing_indices: Vec<usize> = Vec::new();
    for (i, addr_bytes) in address_bytes.iter().enumerate() {
        if !onchain_pubkey_to_model.contains_key(addr_bytes) {
            missing_indices.push(i);
        }
    }

    // Second try: for missing addresses, try derived address lookup
    let mut derived_to_original: std::collections::HashMap<Vec<u8>, usize> =
        std::collections::HashMap::new();

    if !missing_indices.is_empty() {
        // Get distinct owners from accounts with derived addresses
        let owners = timeout(
            Duration::from_millis(DB_TIMEOUT_MS),
            get_distinct_owners_with_addresses(conn),
        )
        .await
        .map_err(|_| PhotonApiError::UnexpectedError("Timeout getting owners".to_string()))?
        .map_err(PhotonApiError::DatabaseError)?;

        // Derive addresses for each missing address and each owner
        // Try both V1 and V2 address trees since accounts may use either
        let address_trees = [ADDRESS_TREE_V1, ADDRESS_TREE_V2];
        let mut all_derived: Vec<Vec<u8>> = Vec::new();
        for &idx in &missing_indices {
            for owner in &owners {
                if let Ok(owner_bytes) = owner.as_slice().try_into() {
                    let owner_bytes: [u8; 32] = owner_bytes;
                    for tree in &address_trees {
                        let derived =
                            derive_address(&addresses[idx].0.to_bytes(), tree, &owner_bytes);
                        let derived_vec = derived.to_vec();
                        derived_to_original.insert(derived_vec.clone(), idx);
                        all_derived.push(derived_vec);
                    }
                }
            }
        }

        if !all_derived.is_empty() {
            let derived_result = timeout(
                Duration::from_millis(DB_TIMEOUT_MS),
                accounts::Entity::find()
                    .filter(
                        Condition::all()
                            .add(accounts::Column::Spent.eq(false))
                            .add(accounts::Column::Address.is_in(all_derived)),
                    )
                    .all(conn),
            )
            .await;

            match derived_result {
                Ok(Ok(models)) => {
                    for model in models {
                        if let Some(address) = &model.address {
                            if let Some(&original_idx) = derived_to_original.get(address) {
                                // Store using the original address bytes as key
                                onchain_pubkey_to_model
                                    .insert(address_bytes[original_idx].clone(), model);
                            }
                        }
                    }
                }
                Ok(Err(e)) => return Err(PhotonApiError::DatabaseError(e)),
                Err(_) => {
                    return Err(PhotonApiError::UnexpectedError(
                        "Database timeout during derived address lookup".to_string(),
                    ))
                }
            }
        }
    }

    // Map results back to original order
    let mut results = Vec::with_capacity(addresses.len());
    for addr_bytes in &address_bytes {
        let model = onchain_pubkey_to_model.get(addr_bytes);

        if let Some(model) = model {
            results.push(Some(model_to_cold_data(model)?));
        } else {
            results.push(None);
        }
    }
    Ok(results)
}

/// Split raw data into 8-byte discriminator and remaining payload
pub fn split_discriminator(data: &[u8]) -> (Vec<u8>, Vec<u8>) {
    if data.len() >= 8 {
        (data[..8].to_vec(), data[8..].to_vec())
    } else {
        (data.to_vec(), vec![])
    }
}

/// Convert an on-chain Solana account to AccountInterface
pub fn hot_to_interface(
    account: &SolanaAccount,
    address: SerializablePubkey,
    _slot: u64,
) -> AccountInterface {
    AccountInterface {
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
    }
}

/// Convert a compressed account to AccountInterface.
/// Always uses `query_address` (the on-chain Solana pubkey) as the key,
/// since `account.address` is the compressed address (Poseidon hash), not the on-chain pubkey.
pub fn cold_to_interface(
    account: &ColdAccountData,
    query_address: SerializablePubkey,
) -> Option<AccountInterface> {
    let address = query_address;

    // Reconstruct full data: discriminator (8 bytes) + data payload
    let discriminator_bytes: Vec<u8> = account
        .discriminator
        .map(|d| d.to_le_bytes().to_vec())
        .unwrap_or_else(|| vec![0u8; 8]);

    let data_payload = account.data.clone().unwrap_or_default();

    // Full data for the account.data field (discriminator + payload)
    let mut full_data = discriminator_bytes.clone();
    full_data.extend_from_slice(&data_payload);
    let space = full_data.len() as u64;

    Some(AccountInterface {
        key: address,
        account: SolanaAccountData {
            lamports: UnsignedInteger(account.lamports),
            data: Base64String(full_data),
            owner: account.owner,
            executable: false,
            rent_epoch: UnsignedInteger(0),
            space: UnsignedInteger(space),
        },
        cold: Some(ColdContext::Account {
            hash: account.hash.clone(),
            leaf_index: UnsignedInteger(account.leaf_index),
            tree_info: TreeInfo {
                tree: account.tree,
                queue: account.queue,
                tree_type: account.tree_type,
                seq: account.seq.map(UnsignedInteger),
                slot_created: UnsignedInteger(account.slot_created),
            },
            data: ColdData {
                discriminator: discriminator_bytes,
                data: Base64String(data_payload),
            },
        }),
    })
}

/// Race hot and cold lookups, returning the result with the higher slot
pub async fn race_hot_cold(
    rpc_client: &RpcClient,
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<Option<AccountInterface>, PhotonApiError> {
    let pubkey = Pubkey::from(address.0.to_bytes());

    // Run both lookups concurrently
    let (hot_result, cold_result) =
        tokio::join!(hot_lookup(rpc_client, &pubkey), cold_lookup(conn, address));

    resolve_race_result(hot_result, cold_result, *address)
}

/// Race hot and cold lookups for multiple accounts
pub async fn race_hot_cold_multiple(
    rpc_client: &RpcClient,
    conn: &DatabaseConnection,
    addresses: &[SerializablePubkey],
) -> Result<Vec<Option<AccountInterface>>, PhotonApiError> {
    let pubkeys: Vec<Pubkey> = addresses
        .iter()
        .map(|a| Pubkey::from(a.0.to_bytes()))
        .collect();

    // Run both lookups concurrently
    let (hot_result, cold_result) = tokio::join!(
        hot_lookup_multiple(rpc_client, &pubkeys),
        cold_lookup_multiple(conn, addresses)
    );

    // Propagate errors - we need both sources for correct racing
    let (hot_accounts, hot_slot) = hot_result?;
    let cold_accounts = cold_result?;

    // Resolve each account
    let mut results = Vec::with_capacity(addresses.len());
    for ((address, hot_account), cold_account) in addresses
        .iter()
        .zip(hot_accounts.iter())
        .zip(cold_accounts.iter())
    {
        let resolved = resolve_single_race(
            hot_account.as_ref(),
            cold_account.as_ref(),
            hot_slot,
            *address,
        );
        results.push(resolved);
    }

    Ok(results)
}

/// Resolve the race result based on slot comparison
fn resolve_race_result(
    hot_result: Result<HotLookupResult, PhotonApiError>,
    cold_result: Result<ColdLookupResult, PhotonApiError>,
    address: SerializablePubkey,
) -> Result<Option<AccountInterface>, PhotonApiError> {
    match (hot_result, cold_result) {
        // Both succeeded
        (Ok(hot), Ok(cold)) => Ok(resolve_single_race(
            hot.account.as_ref(),
            cold.account.as_ref(),
            hot.slot,
            address,
        )),
        // Only hot succeeded
        (Ok(hot), Err(e)) => {
            log::debug!("Cold lookup failed, using hot result: {:?}", e);
            Ok(hot.account.map(|a| hot_to_interface(&a, address, hot.slot)))
        }
        // Only cold succeeded
        (Err(e), Ok(cold)) => {
            log::debug!("Hot lookup failed, using cold result: {:?}", e);
            Ok(cold.account.and_then(|a| cold_to_interface(&a, address)))
        }
        // Both failed - return the hot error (RPC errors are usually more informative)
        (Err(hot_err), Err(cold_err)) => {
            log::warn!(
                "Both hot and cold lookups failed. Hot: {:?}, Cold: {:?}",
                hot_err,
                cold_err
            );
            Err(hot_err)
        }
    }
}

/// Resolve a single race between hot and cold account data.
/// Prefers hot (on-chain) if the account exists with lamports > 0.
fn resolve_single_race(
    hot_account: Option<&SolanaAccount>,
    cold_account: Option<&ColdAccountData>,
    hot_slot: u64,
    address: SerializablePubkey,
) -> Option<AccountInterface> {
    // Prefer hot if account exists on-chain
    if let Some(hot) = hot_account {
        if hot.lamports > 0 {
            return Some(hot_to_interface(hot, address, hot_slot));
        }
    }
    // Fall back to cold
    if let Some(cold) = cold_account {
        return cold_to_interface(cold, address);
    }
    // Neither found
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_single_race_hot_preferred() {
        // Hot is always preferred when it has lamports > 0, regardless of slots
        let hot = SolanaAccount {
            lamports: 1000,
            data: vec![1, 2, 3],
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
        };
        let cold = ColdAccountData {
            hash: Hash::default(),
            address: Some(SerializablePubkey::default()),
            discriminator: None,
            data: Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 6]),
            owner: SerializablePubkey::default(),
            lamports: 500,
            tree: SerializablePubkey::default(),
            queue: SerializablePubkey::default(),
            tree_type: TreeType::StateV1,
            leaf_index: 0,
            seq: Some(1),
            slot_created: 100,
        };

        let result =
            resolve_single_race(Some(&hot), Some(&cold), 200, SerializablePubkey::default());

        assert!(result.is_some());
        let interface = result.unwrap();
        assert!(interface.cold.is_none()); // hot account
        assert_eq!(interface.account.lamports.0, 1000);
    }

    #[test]
    fn test_resolve_single_race_hot_zero_lamports_falls_back_to_cold() {
        // If hot has 0 lamports, fall back to cold
        let hot = SolanaAccount {
            lamports: 0,
            data: vec![],
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
        };
        let cold = ColdAccountData {
            hash: Hash::default(),
            address: Some(SerializablePubkey::default()),
            discriminator: None,
            data: Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 6]),
            owner: SerializablePubkey::default(),
            lamports: 500,
            tree: SerializablePubkey::default(),
            queue: SerializablePubkey::default(),
            tree_type: TreeType::StateV1,
            leaf_index: 0,
            seq: Some(1),
            slot_created: 100,
        };

        let result =
            resolve_single_race(Some(&hot), Some(&cold), 200, SerializablePubkey::default());

        assert!(result.is_some());
        let interface = result.unwrap();
        assert!(interface.cold.is_some()); // cold account
        assert_eq!(interface.account.lamports.0, 500);
    }

    #[test]
    fn test_resolve_single_race_only_hot() {
        let hot = SolanaAccount {
            lamports: 1000,
            data: vec![1, 2, 3],
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
        };

        let result = resolve_single_race(Some(&hot), None, 200, SerializablePubkey::default());

        assert!(result.is_some());
        let interface = result.unwrap();
        assert!(interface.cold.is_none());
    }

    #[test]
    fn test_resolve_single_race_only_cold() {
        let cold = ColdAccountData {
            hash: Hash::default(),
            address: Some(SerializablePubkey::default()),
            discriminator: None,
            data: Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 6]),
            owner: SerializablePubkey::default(),
            lamports: 500,
            tree: SerializablePubkey::default(),
            queue: SerializablePubkey::default(),
            tree_type: TreeType::StateV1,
            leaf_index: 0,
            seq: Some(1),
            slot_created: 100,
        };

        let result = resolve_single_race(None, Some(&cold), 200, SerializablePubkey::default());

        assert!(result.is_some());
        let interface = result.unwrap();
        assert!(interface.cold.is_some());
    }

    #[test]
    fn test_resolve_single_race_neither() {
        let result = resolve_single_race(None, None, 200, SerializablePubkey::default());

        assert!(result.is_none());
    }

    #[test]
    fn test_hot_to_interface_conversion() {
        let owner = Pubkey::new_unique();
        let hot = SolanaAccount {
            lamports: 5000,
            data: vec![10, 20, 30],
            owner,
            executable: true,
            rent_epoch: 100,
        };
        let address = SerializablePubkey::default();
        let slot = 12345;

        let interface = hot_to_interface(&hot, address, slot);

        assert_eq!(interface.account.lamports.0, 5000);
        assert_eq!(interface.account.data.0, vec![10, 20, 30]);
        assert_eq!(interface.account.owner.0, owner);
        assert!(interface.account.executable);
        assert_eq!(interface.account.rent_epoch.0, 100);
        assert!(interface.cold.is_none());
    }

    #[test]
    fn test_cold_to_interface_conversion() {
        // discriminator as u64: 0x0807060504030201 in little-endian = [1,2,3,4,5,6,7,8]
        let discriminator_u64 = u64::from_le_bytes([1, 2, 3, 4, 5, 6, 7, 8]);
        let cold = ColdAccountData {
            hash: Hash::from([1u8; 32]),
            address: Some(SerializablePubkey::from([2u8; 32])),
            discriminator: Some(discriminator_u64),
            data: Some(vec![100, 200]),
            owner: SerializablePubkey::from([3u8; 32]),
            lamports: 9999,
            tree: SerializablePubkey::from([4u8; 32]),
            queue: SerializablePubkey::from([5u8; 32]),
            tree_type: TreeType::StateV2,
            leaf_index: 42,
            seq: Some(7),
            slot_created: 100,
        };

        let interface = cold_to_interface(&cold, SerializablePubkey::default())
            .expect("should return Some when address exists");

        assert_eq!(interface.account.lamports.0, 9999);
        // Full account data = discriminator bytes + payload
        assert_eq!(
            interface.account.data.0,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 100, 200]
        );
        assert!(!interface.account.executable);
        assert_eq!(interface.account.rent_epoch.0, 0);
        assert!(interface.cold.is_some());

        match interface.cold.unwrap() {
            ColdContext::Account {
                hash,
                leaf_index,
                tree_info,
                data,
            } => {
                assert_eq!(hash.0, [1u8; 32]);
                assert_eq!(leaf_index.0, 42);
                assert_eq!(tree_info.seq.unwrap().0, 7);
                assert_eq!(data.discriminator, vec![1, 2, 3, 4, 5, 6, 7, 8]);
                assert_eq!(data.data.0, vec![100, 200]);
            }
            _ => panic!("Expected ColdContext::Account"),
        }
    }

    #[test]
    fn test_cold_to_interface_no_address() {
        let query_address = SerializablePubkey::from([42u8; 32]);
        let cold = ColdAccountData {
            hash: Hash::default(),
            address: None,
            discriminator: None,
            data: None,
            owner: SerializablePubkey::default(),
            lamports: 100,
            tree: SerializablePubkey::default(),
            queue: SerializablePubkey::default(),
            tree_type: TreeType::StateV1,
            leaf_index: 0,
            seq: None,
            slot_created: 100,
        };

        let interface = cold_to_interface(&cold, query_address);

        assert!(interface.is_some());
        let iface = interface.unwrap();
        assert_eq!(iface.key, query_address);
        assert_eq!(iface.account.lamports.0, 100);
    }

    #[test]
    fn test_resolve_race_result_hot_error_cold_success() {
        let cold_result = Ok(ColdLookupResult {
            account: Some(ColdAccountData {
                hash: Hash::default(),
                address: Some(SerializablePubkey::default()),
                discriminator: None,
                data: Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3]),
                owner: SerializablePubkey::default(),
                lamports: 1000,
                tree: SerializablePubkey::default(),
                queue: SerializablePubkey::default(),
                tree_type: TreeType::StateV1,
                leaf_index: 0,
                seq: Some(1),
                slot_created: 100,
            }),
            slot: 100,
        });
        let hot_result: Result<HotLookupResult, PhotonApiError> =
            Err(PhotonApiError::UnexpectedError("RPC failed".to_string()));

        let result = resolve_race_result(hot_result, cold_result, SerializablePubkey::default());

        assert!(result.is_ok());
        let interface = result.unwrap();
        assert!(interface.is_some());
        assert!(interface.unwrap().cold.is_some());
    }

    #[test]
    fn test_resolve_race_result_cold_error_hot_success() {
        let hot_result = Ok(HotLookupResult {
            account: Some(SolanaAccount {
                lamports: 2000,
                data: vec![4, 5, 6],
                owner: Pubkey::new_unique(),
                executable: false,
                rent_epoch: 0,
            }),
            slot: 200,
        });
        let cold_result: Result<ColdLookupResult, PhotonApiError> =
            Err(PhotonApiError::UnexpectedError("DB failed".to_string()));

        let result = resolve_race_result(hot_result, cold_result, SerializablePubkey::default());

        assert!(result.is_ok());
        let interface = result.unwrap();
        assert!(interface.is_some());
        assert!(interface.unwrap().cold.is_none());
    }

    #[test]
    fn test_resolve_race_result_both_errors() {
        let hot_result: Result<HotLookupResult, PhotonApiError> =
            Err(PhotonApiError::UnexpectedError("RPC failed".to_string()));
        let cold_result: Result<ColdLookupResult, PhotonApiError> =
            Err(PhotonApiError::UnexpectedError("DB failed".to_string()));

        let result = resolve_race_result(hot_result, cold_result, SerializablePubkey::default());

        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_race_result_both_none() {
        let hot_result = Ok(HotLookupResult {
            account: None,
            slot: 100,
        });
        let cold_result = Ok(ColdLookupResult {
            account: None,
            slot: 50,
        });

        let result = resolve_race_result(hot_result, cold_result, SerializablePubkey::default());

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_split_discriminator_with_enough_data() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let (disc, payload) = split_discriminator(&data);
        assert_eq!(disc, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(payload, vec![9, 10]);
    }

    #[test]
    fn test_split_discriminator_short_data() {
        let data = vec![1, 2, 3];
        let (disc, payload) = split_discriminator(&data);
        assert_eq!(disc, vec![1, 2, 3]);
        assert!(payload.is_empty());
    }
}
