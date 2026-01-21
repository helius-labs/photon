use std::time::Duration;

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
    AccountInterface, CompressedContext, ResolvedFrom, DB_TIMEOUT_MS, RPC_TIMEOUT_MS,
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
    pub data: Option<Vec<u8>>,
    pub owner: SerializablePubkey,
    pub lamports: u64,
    pub tree: SerializablePubkey,
    pub leaf_index: u64,
    pub seq: Option<u64>,
    pub slot_created: u64,
    pub prove_by_index: bool,
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
/// Searches by both address and onchain_pubkey columns to support generic linking.
/// This allows finding compressed accounts either by their compressed address
/// or by the on-chain pubkey they map to (for decompressed accounts).
pub async fn cold_lookup(
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<ColdLookupResult, PhotonApiError> {
    let address_bytes: Vec<u8> = (*address).into();

    // Query by EITHER address OR onchain_pubkey to support generic linking
    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        accounts::Entity::find()
            .filter(
                Condition::all().add(accounts::Column::Spent.eq(false)).add(
                    Condition::any()
                        .add(accounts::Column::Address.eq(address_bytes.clone()))
                        .add(accounts::Column::OnchainPubkey.eq(address_bytes)),
                ),
            )
            .one(conn),
    )
    .await;

    match result {
        Ok(Ok(Some(model))) => {
            let cold_data = ColdAccountData {
                hash: model.hash.clone().try_into()?,
                address: model
                    .address
                    .clone()
                    .map(SerializablePubkey::try_from)
                    .transpose()?,
                data: model.data.clone(),
                owner: model.owner.clone().try_into()?,
                lamports: parse_decimal(model.lamports)?,
                tree: model.tree.clone().try_into()?,
                leaf_index: crate::api::method::utils::parse_leaf_index(model.leaf_index)?,
                seq: model.seq.map(|s| s as u64),
                slot_created: model.slot_created as u64,
                prove_by_index: model.in_output_queue,
            };
            Ok(ColdLookupResult {
                account: Some(cold_data),
                slot: model.slot_created as u64,
            })
        }
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

/// Perform cold lookup for multiple addresses.
/// Searches by both address and onchain_pubkey columns to support generic linking.
pub async fn cold_lookup_multiple(
    conn: &DatabaseConnection,
    addresses: &[SerializablePubkey],
) -> Result<Vec<Option<ColdAccountData>>, PhotonApiError> {
    let address_bytes: Vec<Vec<u8>> = addresses.iter().map(|a| (*a).into()).collect();

    // Query by EITHER address OR onchain_pubkey to support generic linking
    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        accounts::Entity::find()
            .filter(
                Condition::all().add(accounts::Column::Spent.eq(false)).add(
                    Condition::any()
                        .add(accounts::Column::Address.is_in(address_bytes.clone()))
                        .add(accounts::Column::OnchainPubkey.is_in(address_bytes.clone())),
                ),
            )
            .all(conn),
    )
    .await;

    match result {
        Ok(Ok(models)) => {
            // Create maps of address -> model and onchain_pubkey -> model for efficient lookup
            let mut address_to_model: std::collections::HashMap<Vec<u8>, _> =
                std::collections::HashMap::new();
            let mut onchain_pubkey_to_model: std::collections::HashMap<Vec<u8>, _> =
                std::collections::HashMap::new();

            for model in models {
                if let Some(addr) = &model.address {
                    address_to_model.insert(addr.clone(), model.clone());
                }
                if let Some(onchain) = &model.onchain_pubkey {
                    onchain_pubkey_to_model.insert(onchain.clone(), model);
                }
            }

            // Map results back to original order, checking both address and onchain_pubkey
            let mut results = Vec::with_capacity(addresses.len());
            for addr_bytes in &address_bytes {
                // First check address, then onchain_pubkey
                let model = address_to_model
                    .get(addr_bytes)
                    .or_else(|| onchain_pubkey_to_model.get(addr_bytes));

                if let Some(model) = model {
                    let cold_data = ColdAccountData {
                        hash: model.hash.clone().try_into()?,
                        address: model
                            .address
                            .clone()
                            .map(SerializablePubkey::try_from)
                            .transpose()?,
                        data: model.data.clone(),
                        owner: model.owner.clone().try_into()?,
                        lamports: parse_decimal(model.lamports)?,
                        tree: model.tree.clone().try_into()?,
                        leaf_index: crate::api::method::utils::parse_leaf_index(model.leaf_index)?,
                        seq: model.seq.map(|s| s as u64),
                        slot_created: model.slot_created as u64,
                        prove_by_index: model.in_output_queue,
                    };
                    results.push(Some(cold_data));
                } else {
                    results.push(None);
                }
            }
            Ok(results)
        }
        Ok(Err(e)) => Err(PhotonApiError::DatabaseError(e)),
        Err(_) => Err(PhotonApiError::UnexpectedError(
            "Database timeout".to_string(),
        )),
    }
}

/// Convert an on-chain Solana account to AccountInterface
pub fn hot_to_interface(
    account: &SolanaAccount,
    address: SerializablePubkey,
    slot: u64,
) -> AccountInterface {
    AccountInterface {
        address,
        lamports: UnsignedInteger(account.lamports),
        owner: SerializablePubkey::from(account.owner.to_bytes()),
        data: Base64String(account.data.clone()),
        executable: account.executable,
        rent_epoch: UnsignedInteger(account.rent_epoch),
        resolved_from: ResolvedFrom::Onchain,
        resolved_slot: UnsignedInteger(slot),
        compressed_context: None,
    }
}

/// Convert a compressed account to AccountInterface.
/// Returns None if the account is missing required data (like address).
pub fn cold_to_interface(account: &ColdAccountData) -> Option<AccountInterface> {
    let address = match account.address {
        Some(addr) => addr,
        None => {
            log::warn!(
                "Compressed account missing address field for hash: {:?}. \
                This account may be a fungible token account that doesn't have an address.",
                account.hash
            );
            return None;
        }
    };

    Some(AccountInterface {
        address,
        lamports: UnsignedInteger(account.lamports),
        owner: account.owner,
        data: Base64String(account.data.clone().unwrap_or_default()),
        executable: false,              // Compressed accounts are never executable
        rent_epoch: UnsignedInteger(0), // Compressed accounts don't have rent epoch
        resolved_from: ResolvedFrom::Compressed,
        resolved_slot: UnsignedInteger(account.slot_created),
        compressed_context: Some(CompressedContext {
            hash: account.hash.clone(),
            tree: account.tree,
            leaf_index: UnsignedInteger(account.leaf_index),
            seq: account.seq.map(UnsignedInteger),
            prove_by_index: account.prove_by_index,
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
    for i in 0..addresses.len() {
        let hot_account = hot_accounts.get(i).cloned().flatten();
        let cold_account = cold_accounts.get(i).cloned().flatten();

        let resolved = resolve_single_race(
            hot_account.as_ref(),
            cold_account.as_ref(),
            hot_slot,
            addresses[i],
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
            Ok(cold.account.and_then(|a| cold_to_interface(&a)))
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

/// Resolve a single race between hot and cold account data
fn resolve_single_race(
    hot_account: Option<&SolanaAccount>,
    cold_account: Option<&ColdAccountData>,
    hot_slot: u64,
    address: SerializablePubkey,
) -> Option<AccountInterface> {
    match (hot_account, cold_account) {
        // Both have data - return higher slot
        (Some(hot), Some(cold)) => {
            if hot_slot >= cold.slot_created {
                Some(hot_to_interface(hot, address, hot_slot))
            } else {
                // cold_to_interface returns None if account has no address
                cold_to_interface(cold)
            }
        }
        // Only hot has data
        (Some(hot), None) => Some(hot_to_interface(hot, address, hot_slot)),
        // Only cold has data - returns None if account has no address
        (None, Some(cold)) => cold_to_interface(cold),
        // Neither has data
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_single_race_both_present_hot_wins() {
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
            data: Some(vec![4, 5, 6]),
            owner: SerializablePubkey::default(),
            lamports: 500,
            tree: SerializablePubkey::default(),
            leaf_index: 0,
            seq: Some(1),
            slot_created: 100, // Lower slot
            prove_by_index: false,
        };

        let result = resolve_single_race(
            Some(&hot),
            Some(&cold),
            200, // Higher slot for hot
            SerializablePubkey::default(),
        );

        assert!(result.is_some());
        let interface = result.unwrap();
        assert_eq!(interface.resolved_from, ResolvedFrom::Onchain);
        assert_eq!(interface.lamports.0, 1000);
    }

    #[test]
    fn test_resolve_single_race_both_present_cold_wins() {
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
            data: Some(vec![4, 5, 6]),
            owner: SerializablePubkey::default(),
            lamports: 500,
            tree: SerializablePubkey::default(),
            leaf_index: 0,
            seq: Some(1),
            slot_created: 300, // Higher slot
            prove_by_index: false,
        };

        let result = resolve_single_race(
            Some(&hot),
            Some(&cold),
            200, // Lower slot for hot
            SerializablePubkey::default(),
        );

        assert!(result.is_some());
        let interface = result.unwrap();
        assert_eq!(interface.resolved_from, ResolvedFrom::Compressed);
        assert_eq!(interface.lamports.0, 500);
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
        assert_eq!(interface.resolved_from, ResolvedFrom::Onchain);
    }

    #[test]
    fn test_resolve_single_race_only_cold() {
        let cold = ColdAccountData {
            hash: Hash::default(),
            address: Some(SerializablePubkey::default()),
            data: Some(vec![4, 5, 6]),
            owner: SerializablePubkey::default(),
            lamports: 500,
            tree: SerializablePubkey::default(),
            leaf_index: 0,
            seq: Some(1),
            slot_created: 100,
            prove_by_index: false,
        };

        let result = resolve_single_race(None, Some(&cold), 200, SerializablePubkey::default());

        assert!(result.is_some());
        let interface = result.unwrap();
        assert_eq!(interface.resolved_from, ResolvedFrom::Compressed);
    }

    #[test]
    fn test_resolve_single_race_neither() {
        let result = resolve_single_race(None, None, 200, SerializablePubkey::default());

        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_single_race_equal_slots_hot_wins() {
        // When slots are equal, hot (on-chain) should win as it's considered more authoritative
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
            data: Some(vec![4, 5, 6]),
            owner: SerializablePubkey::default(),
            lamports: 500,
            tree: SerializablePubkey::default(),
            leaf_index: 0,
            seq: Some(1),
            slot_created: 200, // Same slot
            prove_by_index: false,
        };

        let result = resolve_single_race(
            Some(&hot),
            Some(&cold),
            200, // Same slot as cold
            SerializablePubkey::default(),
        );

        assert!(result.is_some());
        let interface = result.unwrap();
        // Hot wins when slots are equal (>= comparison)
        assert_eq!(interface.resolved_from, ResolvedFrom::Onchain);
        assert_eq!(interface.lamports.0, 1000);
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

        assert_eq!(interface.lamports.0, 5000);
        assert_eq!(interface.data.0, vec![10, 20, 30]);
        assert_eq!(interface.owner.0, owner);
        assert!(interface.executable);
        assert_eq!(interface.rent_epoch.0, 100);
        assert_eq!(interface.resolved_from, ResolvedFrom::Onchain);
        assert_eq!(interface.resolved_slot.0, 12345);
        assert!(interface.compressed_context.is_none());
    }

    #[test]
    fn test_cold_to_interface_conversion() {
        let cold = ColdAccountData {
            hash: Hash::from([1u8; 32]),
            address: Some(SerializablePubkey::from([2u8; 32])),
            data: Some(vec![100, 200]),
            owner: SerializablePubkey::from([3u8; 32]),
            lamports: 9999,
            tree: SerializablePubkey::from([4u8; 32]),
            leaf_index: 42,
            seq: Some(7),
            slot_created: 54321,
            prove_by_index: true,
        };

        let interface = cold_to_interface(&cold).expect("should return Some when address exists");

        assert_eq!(interface.lamports.0, 9999);
        assert_eq!(interface.data.0, vec![100, 200]);
        assert!(!interface.executable); // Always false for compressed
        assert_eq!(interface.rent_epoch.0, 0); // Always 0 for compressed
        assert_eq!(interface.resolved_from, ResolvedFrom::Compressed);
        assert_eq!(interface.resolved_slot.0, 54321);

        let ctx = interface.compressed_context.unwrap();
        assert_eq!(ctx.hash.0, [1u8; 32]);
        assert_eq!(ctx.leaf_index.0, 42);
        assert_eq!(ctx.seq.unwrap().0, 7);
        assert!(ctx.prove_by_index);
    }

    #[test]
    fn test_cold_to_interface_no_address() {
        // Test when compressed account has no address (e.g., fungible token account)
        let cold = ColdAccountData {
            hash: Hash::default(),
            address: None, // No address - like fungible token accounts
            data: None,
            owner: SerializablePubkey::default(),
            lamports: 100,
            tree: SerializablePubkey::default(),
            leaf_index: 0,
            seq: None,
            slot_created: 1000,
            prove_by_index: false,
        };

        let interface = cold_to_interface(&cold);

        // Should return None when account has no address
        assert!(interface.is_none());
    }

    #[test]
    fn test_resolve_race_result_hot_error_cold_success() {
        let cold_result = Ok(ColdLookupResult {
            account: Some(ColdAccountData {
                hash: Hash::default(),
                address: Some(SerializablePubkey::default()),
                data: Some(vec![1, 2, 3]),
                owner: SerializablePubkey::default(),
                lamports: 1000,
                tree: SerializablePubkey::default(),
                leaf_index: 0,
                seq: Some(1),
                slot_created: 100,
                prove_by_index: false,
            }),
            slot: 100,
        });
        let hot_result: Result<HotLookupResult, PhotonApiError> =
            Err(PhotonApiError::UnexpectedError("RPC failed".to_string()));

        let result = resolve_race_result(hot_result, cold_result, SerializablePubkey::default());

        assert!(result.is_ok());
        let interface = result.unwrap();
        assert!(interface.is_some());
        assert_eq!(interface.unwrap().resolved_from, ResolvedFrom::Compressed);
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
        assert_eq!(interface.unwrap().resolved_from, ResolvedFrom::Onchain);
    }

    #[test]
    fn test_resolve_race_result_both_errors() {
        let hot_result: Result<HotLookupResult, PhotonApiError> =
            Err(PhotonApiError::UnexpectedError("RPC failed".to_string()));
        let cold_result: Result<ColdLookupResult, PhotonApiError> =
            Err(PhotonApiError::UnexpectedError("DB failed".to_string()));

        let result = resolve_race_result(hot_result, cold_result, SerializablePubkey::default());

        // When both fail, should return the hot error
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
}
