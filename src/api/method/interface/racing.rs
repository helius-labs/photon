use std::collections::HashMap;
use std::time::Duration;

use light_compressed_account::address::derive_address;
use light_sdk_types::constants::ADDRESS_TREE_V2;
use sea_orm::{ColumnTrait, Condition, DatabaseConnection, EntityTrait, QueryFilter};
use solana_account::Account as SolanaAccount;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use tokio::time::timeout;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::account::AccountV2;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::{accounts, token_accounts};

use super::types::{AccountInterface, SolanaAccountData, DB_TIMEOUT_MS, RPC_TIMEOUT_MS};

/// Result from a hot (on-chain RPC) lookup.
#[derive(Debug)]
pub struct HotLookupResult {
    pub account: Option<SolanaAccount>,
    pub slot: u64,
}

/// Result from a cold (compressed DB) lookup.
#[derive(Debug)]
pub struct ColdLookupResult {
    pub accounts: Vec<AccountV2>,
}

/// Perform a hot lookup via Solana RPC.
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

/// Perform a cold lookup from the compressed accounts database.
///
/// The lookup aggregates all compressed accounts associated with the queried pubkey:
/// 1) direct onchain pubkey matches (decompressed account linkage),
/// 2) derived compressed address matches (V2 address tree only),
/// 3) token account owner / ata_owner matches (can return multiple accounts).
pub async fn cold_lookup(
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<ColdLookupResult, PhotonApiError> {
    let models = find_cold_models(conn, address).await?;

    let mut accounts_v2 = Vec::with_capacity(models.len());
    for model in models {
        let account = AccountV2::try_from(model)?;
        accounts_v2.push(account);
    }

    Ok(ColdLookupResult {
        accounts: accounts_v2,
    })
}

async fn find_cold_models(
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<Vec<accounts::Model>, PhotonApiError> {
    let address_bytes: Vec<u8> = (*address).into();
    let pda_seed = address.0.to_bytes();

    let mut by_hash: HashMap<Vec<u8>, accounts::Model> = HashMap::new();

    // 1) Direct onchain pubkey linkage.
    let onchain_result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        accounts::Entity::find()
            .filter(accounts::Column::Spent.eq(false))
            .filter(accounts::Column::OnchainPubkey.eq(address_bytes.clone()))
            .all(conn),
    )
    .await;

    match onchain_result {
        Ok(Ok(models)) => {
            for model in models {
                by_hash.insert(model.hash.clone(), model);
            }
        }
        Ok(Err(e)) => return Err(PhotonApiError::DatabaseError(e)),
        Err(_) => {
            return Err(PhotonApiError::UnexpectedError(
                "Database timeout".to_string(),
            ))
        }
    }

    // 2) Derived-address fallback (V2 only).
    let owners_result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        get_distinct_owners_with_addresses(conn),
    )
    .await;

    let owners = match owners_result {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => return Err(PhotonApiError::DatabaseError(e)),
        Err(_) => {
            return Err(PhotonApiError::UnexpectedError(
                "Database timeout getting owners".to_string(),
            ))
        }
    };

    if !owners.is_empty() {
        let derived_addresses: Vec<Vec<u8>> = owners
            .iter()
            .filter_map(|owner| owner.as_slice().try_into().ok())
            .map(|owner_bytes: [u8; 32]| {
                derive_address(&pda_seed, &ADDRESS_TREE_V2, &owner_bytes).to_vec()
            })
            .collect();

        if !derived_addresses.is_empty() {
            let derived_result = timeout(
                Duration::from_millis(DB_TIMEOUT_MS),
                accounts::Entity::find()
                    .filter(
                        Condition::all()
                            .add(accounts::Column::Spent.eq(false))
                            .add(accounts::Column::Address.is_in(derived_addresses)),
                    )
                    .all(conn),
            )
            .await;

            match derived_result {
                Ok(Ok(models)) => {
                    for model in models {
                        by_hash.insert(model.hash.clone(), model);
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

    // 3) Token account linkage by token owner / ata_owner.
    let token_result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        token_accounts::Entity::find()
            .filter(token_accounts::Column::Spent.eq(false))
            .filter(
                Condition::any()
                    .add(token_accounts::Column::AtaOwner.eq(address_bytes.clone()))
                    .add(token_accounts::Column::Owner.eq(address_bytes)),
            )
            .find_also_related(accounts::Entity)
            .all(conn),
    )
    .await;

    match token_result {
        Ok(Ok(rows)) => {
            for (_token, maybe_account) in rows {
                if let Some(model) = maybe_account {
                    if !model.spent {
                        by_hash.insert(model.hash.clone(), model);
                    }
                }
            }
        }
        Ok(Err(e)) => return Err(PhotonApiError::DatabaseError(e)),
        Err(_) => {
            return Err(PhotonApiError::UnexpectedError(
                "Database timeout during token lookup".to_string(),
            ))
        }
    }

    Ok(by_hash.into_values().collect())
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

fn hot_to_solana_account_data(account: &SolanaAccount) -> SolanaAccountData {
    SolanaAccountData {
        lamports: UnsignedInteger(account.lamports),
        data: Base64String(account.data.clone()),
        owner: SerializablePubkey::from(account.owner.to_bytes()),
        executable: account.executable,
        rent_epoch: UnsignedInteger(account.rent_epoch),
        space: UnsignedInteger(account.data.len() as u64),
    }
}

fn cold_to_synthetic_account_data(account: &AccountV2) -> SolanaAccountData {
    let full_data = account
        .data
        .as_ref()
        .map(|data| {
            let mut bytes = data.discriminator.0.to_le_bytes().to_vec();
            bytes.extend_from_slice(&data.data.0);
            bytes
        })
        .unwrap_or_default();

    SolanaAccountData {
        lamports: account.lamports,
        data: Base64String(full_data.clone()),
        owner: account.owner,
        executable: false,
        rent_epoch: UnsignedInteger(0),
        space: UnsignedInteger(full_data.len() as u64),
    }
}

fn build_interface(
    address: SerializablePubkey,
    account_data: SolanaAccountData,
    cold_accounts: Vec<AccountV2>,
) -> AccountInterface {
    AccountInterface {
        key: address,
        account: account_data,
        cold: (!cold_accounts.is_empty()).then_some(cold_accounts),
    }
}

/// Race hot and cold lookups, returning a single unified interface shape.
///
/// Account selection policy:
/// 1) If hot exists with lamports > 0 and hot slot >= newest cold slot, use hot account view.
/// 2) Otherwise, if cold exists, synthesize account view from newest cold account.
/// 3) Include all cold accounts in `cold` whenever they exist.
pub async fn race_hot_cold(
    rpc_client: &RpcClient,
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<Option<AccountInterface>, PhotonApiError> {
    let pubkey = Pubkey::from(address.0.to_bytes());

    let (hot_result, cold_result) =
        tokio::join!(hot_lookup(rpc_client, &pubkey), cold_lookup(conn, address));

    resolve_race_result(hot_result, cold_result, *address)
}

fn resolve_race_result(
    hot_result: Result<HotLookupResult, PhotonApiError>,
    cold_result: Result<ColdLookupResult, PhotonApiError>,
    address: SerializablePubkey,
) -> Result<Option<AccountInterface>, PhotonApiError> {
    match (hot_result, cold_result) {
        (Ok(hot), Ok(cold)) => Ok(resolve_single_race(
            hot.account.as_ref(),
            &cold.accounts,
            hot.slot,
            address,
        )),
        (Ok(hot), Err(e)) => {
            log::debug!("Cold lookup failed, using hot result: {:?}", e);
            Ok(hot
                .account
                .as_ref()
                .filter(|account| account.lamports > 0)
                .map(|account| {
                    build_interface(address, hot_to_solana_account_data(account), vec![])
                }))
        }
        (Err(e), Ok(cold)) => {
            log::debug!("Hot lookup failed, using cold result: {:?}", e);
            Ok(resolve_single_race(None, &cold.accounts, 0, address))
        }
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

fn resolve_single_race(
    hot_account: Option<&SolanaAccount>,
    cold_accounts: &[AccountV2],
    hot_slot: u64,
    address: SerializablePubkey,
) -> Option<AccountInterface> {
    let newest_cold = cold_accounts
        .iter()
        .max_by_key(|account| account.slot_created.0);

    let account_data = match (
        hot_account.filter(|account| account.lamports > 0),
        newest_cold,
    ) {
        (Some(hot), Some(cold)) => {
            if hot_slot >= cold.slot_created.0 {
                hot_to_solana_account_data(hot)
            } else {
                cold_to_synthetic_account_data(cold)
            }
        }
        (Some(hot), None) => hot_to_solana_account_data(hot),
        (None, Some(cold)) => cold_to_synthetic_account_data(cold),
        (None, None) => return None,
    };

    Some(build_interface(
        address,
        account_data,
        cold_accounts.to_vec(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::method::get_validity_proof::MerkleContextV2;
    use crate::common::typedefs::account::AccountData;
    use crate::common::typedefs::hash::Hash;

    fn sample_cold(slot_created: u64, lamports: u64) -> AccountV2 {
        AccountV2 {
            hash: Hash::default(),
            address: Some(SerializablePubkey::default()),
            data: Some(AccountData {
                discriminator: UnsignedInteger(0x0807060504030201),
                data: Base64String(vec![100, 200]),
                data_hash: Hash::default(),
            }),
            owner: SerializablePubkey::default(),
            lamports: UnsignedInteger(lamports),
            leaf_index: UnsignedInteger(0),
            seq: Some(UnsignedInteger(1)),
            slot_created: UnsignedInteger(slot_created),
            prove_by_index: false,
            merkle_context: MerkleContextV2 {
                tree_type: 3,
                tree: SerializablePubkey::default(),
                queue: SerializablePubkey::default(),
                cpi_context: None,
                next_tree_context: None,
            },
        }
    }

    #[test]
    fn test_resolve_single_race_prefers_hot_when_newer_or_equal() {
        let hot = SolanaAccount {
            lamports: 1000,
            data: vec![1, 2, 3],
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
        };
        let cold = sample_cold(100, 500);

        let result = resolve_single_race(
            Some(&hot),
            std::slice::from_ref(&cold),
            200,
            SerializablePubkey::default(),
        )
        .expect("expected Some interface");

        assert_eq!(result.account.lamports.0, 1000);
        assert!(result.cold.is_some());
        assert_eq!(result.cold.unwrap().len(), 1);
    }

    #[test]
    fn test_resolve_single_race_prefers_newer_cold_by_slot() {
        let hot = SolanaAccount {
            lamports: 1000,
            data: vec![1, 2, 3],
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
        };
        let cold = sample_cold(300, 777);

        let result = resolve_single_race(
            Some(&hot),
            std::slice::from_ref(&cold),
            200,
            SerializablePubkey::default(),
        )
        .expect("expected Some interface");

        assert_eq!(result.account.lamports.0, 777);
        assert!(result.cold.is_some());
    }

    #[test]
    fn test_resolve_single_race_falls_back_to_cold_when_hot_deleted() {
        let hot = SolanaAccount {
            lamports: 0,
            data: vec![],
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
        };
        let cold = sample_cold(100, 500);

        let result = resolve_single_race(
            Some(&hot),
            std::slice::from_ref(&cold),
            200,
            SerializablePubkey::default(),
        )
        .expect("expected Some interface");

        assert_eq!(result.account.lamports.0, 500);
        assert!(result.cold.is_some());
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

        let result = resolve_single_race(Some(&hot), &[], 200, SerializablePubkey::default())
            .expect("expected Some interface");

        assert_eq!(result.account.lamports.0, 1000);
        assert!(result.cold.is_none());
    }

    #[test]
    fn test_resolve_single_race_only_cold() {
        let cold = sample_cold(100, 555);

        let result = resolve_single_race(
            None,
            std::slice::from_ref(&cold),
            0,
            SerializablePubkey::default(),
        )
        .expect("expected Some interface");

        assert_eq!(result.account.lamports.0, 555);
        assert!(result.cold.is_some());
    }

    #[test]
    fn test_resolve_single_race_neither() {
        let result = resolve_single_race(None, &[], 0, SerializablePubkey::default());
        assert!(result.is_none());
    }
}
