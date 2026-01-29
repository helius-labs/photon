use std::time::Duration;

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
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
    AccountInterface, ColdContext, ColdData, SolanaAccountData, TreeInfo, DB_TIMEOUT_MS,
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
    pub data: Option<Vec<u8>>,
    pub owner: SerializablePubkey,
    pub lamports: u64,
    pub tree: SerializablePubkey,
    pub leaf_index: u64,
    pub seq: Option<u64>,
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
/// Searches by onchain_pubkey column — the input is always a Solana pubkey
/// (every account starts on-chain before compression, so onchain_pubkey is always populated).
pub async fn cold_lookup(
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<ColdLookupResult, PhotonApiError> {
    let address_bytes: Vec<u8> = (*address).into();

    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        accounts::Entity::find()
            .filter(accounts::Column::Spent.eq(false))
            .filter(accounts::Column::OnchainPubkey.eq(address_bytes))
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
/// Searches by onchain_pubkey column — the input is always Solana pubkeys.
pub async fn cold_lookup_multiple(
    conn: &DatabaseConnection,
    addresses: &[SerializablePubkey],
) -> Result<Vec<Option<ColdAccountData>>, PhotonApiError> {
    let address_bytes: Vec<Vec<u8>> = addresses.iter().map(|a| (*a).into()).collect();

    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        accounts::Entity::find()
            .filter(accounts::Column::Spent.eq(false))
            .filter(accounts::Column::OnchainPubkey.is_in(address_bytes.clone()))
            .all(conn),
    )
    .await;

    match result {
        Ok(Ok(models)) => {
            // Create map of onchain_pubkey -> model for efficient lookup
            let mut onchain_pubkey_to_model: std::collections::HashMap<Vec<u8>, _> =
                std::collections::HashMap::new();

            for model in models {
                if let Some(onchain) = &model.onchain_pubkey {
                    onchain_pubkey_to_model.insert(onchain.clone(), model);
                }
            }

            // Map results back to original order
            let mut results = Vec::with_capacity(addresses.len());
            for addr_bytes in &address_bytes {
                let model = onchain_pubkey_to_model.get(addr_bytes);

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
    let raw_data = account.data.clone().unwrap_or_default();
    let space = raw_data.len() as u64;
    let (discriminator, data_payload) = split_discriminator(&raw_data);

    Some(AccountInterface {
        key: address,
        account: SolanaAccountData {
            lamports: UnsignedInteger(account.lamports),
            data: Base64String(raw_data),
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
                seq: account.seq.map(UnsignedInteger),
            },
            data: ColdData {
                discriminator,
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
            data: Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 6]),
            owner: SerializablePubkey::default(),
            lamports: 500,
            tree: SerializablePubkey::default(),
            leaf_index: 0,
            seq: Some(1),
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
            data: Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 6]),
            owner: SerializablePubkey::default(),
            lamports: 500,
            tree: SerializablePubkey::default(),
            leaf_index: 0,
            seq: Some(1),
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
            data: Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 6]),
            owner: SerializablePubkey::default(),
            lamports: 500,
            tree: SerializablePubkey::default(),
            leaf_index: 0,
            seq: Some(1),
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
        let cold = ColdAccountData {
            hash: Hash::from([1u8; 32]),
            address: Some(SerializablePubkey::from([2u8; 32])),
            data: Some(vec![10, 20, 30, 40, 50, 60, 70, 80, 100, 200]),
            owner: SerializablePubkey::from([3u8; 32]),
            lamports: 9999,
            tree: SerializablePubkey::from([4u8; 32]),
            leaf_index: 42,
            seq: Some(7),
        };

        let interface = cold_to_interface(&cold, SerializablePubkey::default())
            .expect("should return Some when address exists");

        assert_eq!(interface.account.lamports.0, 9999);
        assert_eq!(
            interface.account.data.0,
            vec![10, 20, 30, 40, 50, 60, 70, 80, 100, 200]
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
                assert_eq!(data.discriminator, vec![10, 20, 30, 40, 50, 60, 70, 80]);
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
            data: None,
            owner: SerializablePubkey::default(),
            lamports: 100,
            tree: SerializablePubkey::default(),
            leaf_index: 0,
            seq: None,
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
                data: Some(vec![0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3]),
                owner: SerializablePubkey::default(),
                lamports: 1000,
                tree: SerializablePubkey::default(),
                leaf_index: 0,
                seq: Some(1),
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
