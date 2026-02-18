use std::collections::HashMap;
use std::time::Duration;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::account::AccountV2;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::token_data::AccountState;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::{accounts, token_accounts};
use light_compressed_account::address::derive_address;
use light_sdk_types::constants::ADDRESS_TREE_V2;
use sea_orm::prelude::Decimal;
use sea_orm::{ColumnTrait, Condition, DatabaseConnection, EntityTrait, QueryFilter};
use solana_account::Account as SolanaAccount;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_program_option::COption;
use solana_program_pack::Pack;
use solana_pubkey::Pubkey;
use spl_token_interface::state::Account as SplTokenAccount;
use spl_token_interface::state::AccountState as SplAccountState;
use tokio::time::timeout;

use crate::common::typedefs::token_data::TokenData;
use crate::ingester::persist::DECOMPRESSED_ACCOUNT_DISCRIMINATOR;

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
    /// Map from account hash → wallet owner bytes (from ata_owner in token_accounts table).
    pub token_wallet_owners: HashMap<Hash, [u8; 32]>,
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
    let (models, token_wallet_owners) = find_cold_models(conn, address).await?;

    let mut accounts_v2 = Vec::with_capacity(models.len());
    for model in models {
        let account = AccountV2::try_from(model)?;
        accounts_v2.push(account);
    }

    Ok(ColdLookupResult {
        accounts: accounts_v2,
        token_wallet_owners,
    })
}

async fn find_cold_models(
    conn: &DatabaseConnection,
    address: &SerializablePubkey,
) -> Result<(Vec<accounts::Model>, HashMap<Hash, [u8; 32]>), PhotonApiError> {
    let address_bytes: Vec<u8> = (*address).into();
    let pda_seed = address.0.to_bytes();

    let mut by_hash: HashMap<Vec<u8>, accounts::Model> = HashMap::new();
    let mut token_wallet_owners: HashMap<Hash, [u8; 32]> = HashMap::new();

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
            for (token, maybe_account) in rows {
                if let Some(model) = maybe_account {
                    if !model.spent {
                        if let Some(ata_owner) = token.ata_owner {
                            match (
                                Hash::try_from(model.hash.clone()),
                                <[u8; 32]>::try_from(ata_owner.as_slice()),
                            ) {
                                (Ok(hash), Ok(owner)) => {
                                    token_wallet_owners.insert(hash, owner);
                                }
                                _ => log::warn!(
                                    "Skipping invalid token wallet owner entry: hash_len={}, owner_len={}",
                                    model.hash.len(),
                                    ata_owner.len()
                                ),
                            }
                        }
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

    // Filter out decompressed PDA placeholders — they are bookmarks in the
    // Merkle tree, not truly cold accounts.
    let decompressed_disc = Decimal::from(DECOMPRESSED_ACCOUNT_DISCRIMINATOR);
    let models: Vec<_> = by_hash
        .into_values()
        .filter(|m| m.discriminator != Some(decompressed_disc))
        .collect();
    Ok((models, token_wallet_owners))
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

/// Build the 165-byte SPL Token Account layout from compressed TokenData +
/// corrected wallet owner.
fn build_spl_token_account_bytes(token_data: &TokenData, wallet_owner: &[u8; 32]) -> Vec<u8> {
    let spl_state = match token_data.state {
        AccountState::initialized => SplAccountState::Initialized,
        AccountState::frozen => SplAccountState::Frozen,
    };

    let spl_account = SplTokenAccount {
        mint: Pubkey::from(token_data.mint.0.to_bytes()),
        owner: Pubkey::from(*wallet_owner),
        amount: token_data.amount.0,
        delegate: match &token_data.delegate {
            Some(d) => COption::Some(Pubkey::from(d.0.to_bytes())),
            None => COption::None,
        },
        state: spl_state,
        is_native: COption::None,
        delegated_amount: 0,
        close_authority: COption::None,
    };

    let mut buf = vec![0u8; SplTokenAccount::LEN];
    SplTokenAccount::pack(spl_account, &mut buf).expect("buffer is exactly LEN bytes");
    buf
}

fn cold_to_synthetic_account_data(
    account: &AccountV2,
    wallet_owner: Option<&[u8]>,
) -> SolanaAccountData {
    // For token accounts, always synthesize 165-byte SPL layout.
    // Prefer ATA wallet owner when available, else fall back to compressed token owner.
    if let Ok(Some(token_data)) = account.parse_token_data() {
        let owner_arr = match wallet_owner.and_then(|bytes| <&[u8; 32]>::try_from(bytes).ok()) {
            Some(owner) => *owner,
            None => {
                if let Some(owner) = wallet_owner {
                    log::debug!(
                        "Invalid ata_owner length for token account {}, using compressed owner: {}",
                        account.hash,
                        owner.len()
                    );
                }
                token_data.owner.0.to_bytes()
            }
        };

        let spl_bytes = build_spl_token_account_bytes(&token_data, &owner_arr);
        return SolanaAccountData {
            lamports: account.lamports,
            data: Base64String(spl_bytes.clone()),
            // Preserve the original program owner from the cold account model.
            // This can be LIGHT token program and, depending on account source,
            // may also be SPL Token / Token-2022.
            owner: account.owner,
            executable: false,
            rent_epoch: UnsignedInteger(0),
            space: UnsignedInteger(spl_bytes.len() as u64),
        };
    }

    if wallet_owner
        .map(|bytes| <&[u8; 32]>::try_from(bytes).is_err())
        .unwrap_or(false)
    {
        log::debug!(
            "Ignoring invalid wallet owner length for non-token cold account: {:?}",
            wallet_owner.map(|v| v.len())
        );
    }

    let full_data = account
        .data
        .as_ref()
        .map(|d| d.data.0.clone())
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

fn parse_hot_spl_token(data: &[u8]) -> Option<SplTokenAccount> {
    SplTokenAccount::unpack(data).ok()
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
            &cold.token_wallet_owners,
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
            Ok(resolve_single_race(
                None,
                &cold.accounts,
                0,
                address,
                &cold.token_wallet_owners,
            ))
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

/// Build a synthetic `SolanaAccountData` from cold accounts, optionally
/// including a hot on-chain account's balance.
///
/// For fungible tokens: if **all** cold accounts parse as token accounts with
/// the same mint (and, when provided, the hot account shares that mint) their
/// amounts are summed into a single SPL Token layout.  When a hot account is
/// present it is used as the base for the synthetic view (it already carries the
/// correct wallet owner, delegate, state, etc.).  Otherwise the newest cold
/// account (by slot) provides those fields.
///
/// For everything else (non-token accounts, mixed mints, parse failures) the
/// function falls back to the hot account (if present and newer) or the newest
/// cold account.
fn cold_accounts_to_synthetic(
    cold_accounts: &[AccountV2],
    token_wallet_owners: &HashMap<Hash, [u8; 32]>,
    hot_account: Option<&SolanaAccount>,
    hot_slot: u64,
) -> SolanaAccountData {
    debug_assert!(!cold_accounts.is_empty());

    // Try to parse every cold account as a token account.
    let parsed: Vec<(&AccountV2, TokenData)> = cold_accounts
        .iter()
        .filter_map(|acc| acc.parse_token_data().ok().flatten().map(|td| (acc, td)))
        .collect();

    // All cold accounts must be token accounts with the same mint.
    if !parsed.is_empty() && parsed.len() == cold_accounts.len() {
        let first_mint = &parsed[0].1.mint;
        let all_same_mint = parsed.iter().all(|(_, td)| td.mint == *first_mint);

        if all_same_mint {
            // If a hot account is provided, it must also be an SPL token with
            // the same mint; otherwise skip aggregation entirely.
            let hot_contribution = match hot_account {
                Some(hot) => match parse_hot_spl_token(&hot.data) {
                    Some(spl) if spl.mint.to_bytes() == first_mint.0.to_bytes() => Some(spl),
                    Some(_) => None, // different mint — can't aggregate
                    None => None,    // not a token account
                },
                None => None,
            };

            // When hot exists but doesn't match, fall through to fallback.
            if hot_account.is_none() || hot_contribution.is_some() {
                let cold_total: u64 = parsed.iter().map(|(_, td)| td.amount.0).sum();
                let hot_amount = hot_contribution.as_ref().map_or(0, |spl| spl.amount);
                let total_amount = cold_total + hot_amount;

                // When a hot account matches, use it as the base for the
                // synthetic view — it already has the correct wallet owner,
                // delegate, state, etc.  Unpack, set aggregated amount, repack.
                if let (Some(hot), Some(mut spl)) = (hot_account, hot_contribution) {
                    spl.amount = total_amount;
                    let mut spl_bytes = vec![0u8; SplTokenAccount::LEN];
                    SplTokenAccount::pack(spl, &mut spl_bytes)
                        .expect("buffer is exactly LEN bytes");
                    return SolanaAccountData {
                        lamports: UnsignedInteger(hot.lamports),
                        data: Base64String(spl_bytes.clone()),
                        owner: SerializablePubkey::from(hot.owner.to_bytes()),
                        executable: false,
                        rent_epoch: UnsignedInteger(0),
                        space: UnsignedInteger(spl_bytes.len() as u64),
                    };
                }

                // Cold-only path: build from newest cold account.
                let (newest_acc, newest_td) = parsed
                    .iter()
                    .max_by_key(|(acc, _)| acc.slot_created.0)
                    .unwrap();

                let wallet_owner_bytes = token_wallet_owners.get(&newest_acc.hash);
                let owner_arr = match wallet_owner_bytes
                    .and_then(|bytes| <&[u8; 32]>::try_from(bytes.as_slice()).ok())
                {
                    Some(owner) => *owner,
                    None => newest_td.owner.0.to_bytes(),
                };

                let aggregated = TokenData {
                    mint: newest_td.mint,
                    owner: newest_td.owner,
                    amount: UnsignedInteger(total_amount),
                    delegate: newest_td.delegate,
                    state: newest_td.state,
                    tlv: newest_td.tlv.clone(),
                };

                let spl_bytes = build_spl_token_account_bytes(&aggregated, &owner_arr);
                return SolanaAccountData {
                    lamports: newest_acc.lamports,
                    data: Base64String(spl_bytes.clone()),
                    owner: newest_acc.owner,
                    executable: false,
                    rent_epoch: UnsignedInteger(0),
                    space: UnsignedInteger(spl_bytes.len() as u64),
                };
            }
        }
    }

    // Fallback: compare hot slot vs newest cold slot to pick the fresher view.
    let newest = cold_accounts
        .iter()
        .max_by_key(|acc| acc.slot_created.0)
        .unwrap();

    if let Some(hot) = hot_account {
        if hot_slot >= newest.slot_created.0 {
            return hot_to_solana_account_data(hot);
        }
    }

    let wallet_owner = token_wallet_owners.get(&newest.hash);
    cold_to_synthetic_account_data(newest, wallet_owner.map(|v| v.as_slice()))
}

fn resolve_single_race(
    hot_account: Option<&SolanaAccount>,
    cold_accounts: &[AccountV2],
    hot_slot: u64,
    address: SerializablePubkey,
    token_wallet_owners: &HashMap<Hash, [u8; 32]>,
) -> Option<AccountInterface> {
    let hot = hot_account.filter(|account| account.lamports > 0);
    let has_cold = !cold_accounts.is_empty();

    let account_data = match (hot, has_cold) {
        (Some(hot), true) => {
            cold_accounts_to_synthetic(cold_accounts, token_wallet_owners, Some(hot), hot_slot)
        }
        (Some(hot), false) => hot_to_solana_account_data(hot),
        (None, true) => cold_accounts_to_synthetic(cold_accounts, token_wallet_owners, None, 0),
        (None, false) => return None,
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
    use crate::common::typedefs::account::C_TOKEN_DISCRIMINATOR_V2;
    use crate::common::typedefs::hash::Hash;
    use crate::common::typedefs::token_data::AccountState;
    use crate::ingester::persist::LIGHT_TOKEN_PROGRAM_ID;
    use solana_program_pack::Pack;
    use spl_token_interface::state::Account as SplTokenAccount;

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

    /// Build a cold AccountV2 that looks like a compressed token account.
    /// Uses LIGHT_TOKEN_PROGRAM_ID as owner and a c_token discriminator.
    fn sample_token_cold(slot_created: u64, lamports: u64, token_data: &TokenData) -> AccountV2 {
        sample_token_cold_with_hash(slot_created, lamports, token_data, Hash::default())
    }

    fn sample_token_cold_with_hash(
        slot_created: u64,
        lamports: u64,
        token_data: &TokenData,
        hash: Hash,
    ) -> AccountV2 {
        use borsh::BorshSerialize;
        let mut data_bytes = Vec::new();
        token_data.serialize(&mut data_bytes).unwrap();

        let discriminator = u64::from_le_bytes(C_TOKEN_DISCRIMINATOR_V2);

        AccountV2 {
            hash,
            address: Some(SerializablePubkey::default()),
            data: Some(AccountData {
                discriminator: UnsignedInteger(discriminator),
                data: Base64String(data_bytes),
                data_hash: Hash::default(),
            }),
            owner: SerializablePubkey::from(LIGHT_TOKEN_PROGRAM_ID),
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
            &HashMap::new(),
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
            &HashMap::new(),
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
            &HashMap::new(),
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

        let result = resolve_single_race(
            Some(&hot),
            &[],
            200,
            SerializablePubkey::default(),
            &HashMap::new(),
        )
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
            &HashMap::new(),
        )
        .expect("expected Some interface");

        assert_eq!(result.account.lamports.0, 555);
        assert!(result.cold.is_some());
    }

    #[test]
    fn test_resolve_single_race_neither() {
        let result =
            resolve_single_race(None, &[], 0, SerializablePubkey::default(), &HashMap::new());
        assert!(result.is_none());
    }

    // ============ SPL Token Account reconstruction tests ============

    #[test]
    fn test_build_spl_token_account_bytes_basic() {
        let mint = Pubkey::new_unique();
        let wallet_owner = [42u8; 32];
        let token_data = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from([99u8; 32]), // compressed owner (not wallet)
            amount: UnsignedInteger(1_000_000),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };

        let bytes = build_spl_token_account_bytes(&token_data, &wallet_owner);

        assert_eq!(bytes.len(), SplTokenAccount::LEN);
        let parsed = SplTokenAccount::unpack(&bytes).expect("valid SPL layout");
        assert_eq!(parsed.mint, mint);
        assert_eq!(parsed.owner, Pubkey::from(wallet_owner));
        assert_eq!(parsed.amount, 1_000_000);
        assert!(parsed.delegate.is_none());
        assert_eq!(
            parsed.state,
            spl_token_interface::state::AccountState::Initialized
        );
        assert!(parsed.is_native.is_none());
        assert_eq!(parsed.delegated_amount, 0);
        assert!(parsed.close_authority.is_none());
    }

    #[test]
    fn test_build_spl_token_account_bytes_with_delegate() {
        let mint = Pubkey::new_unique();
        let wallet_owner = [10u8; 32];
        let delegate_key = Pubkey::new_unique();
        let token_data = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(500),
            delegate: Some(SerializablePubkey::from(delegate_key)),
            state: AccountState::frozen,
            tlv: None,
        };

        let bytes = build_spl_token_account_bytes(&token_data, &wallet_owner);

        assert_eq!(bytes.len(), SplTokenAccount::LEN);
        let parsed = SplTokenAccount::unpack(&bytes).expect("valid SPL layout");
        assert_eq!(parsed.amount, 500);
        assert_eq!(
            parsed.delegate,
            solana_program_option::COption::Some(delegate_key)
        );
        assert_eq!(
            parsed.state,
            spl_token_interface::state::AccountState::Frozen
        );
    }

    #[test]
    fn test_cold_to_synthetic_token_with_wallet_owner() {
        let mint = Pubkey::new_unique();
        let wallet_owner = [55u8; 32];
        let token_data = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(42),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };

        let cold = sample_token_cold(100, 1_000_000, &token_data);
        let result = cold_to_synthetic_account_data(&cold, Some(&wallet_owner));

        // Should produce 165-byte SPL layout
        let parsed = SplTokenAccount::unpack(&result.data.0).expect("valid SPL layout");
        assert_eq!(result.space.0, SplTokenAccount::LEN as u64);
        // Program owner should be preserved from cold account.
        assert_eq!(
            result.owner,
            SerializablePubkey::from(LIGHT_TOKEN_PROGRAM_ID)
        );
        assert_eq!(parsed.owner, Pubkey::from(wallet_owner));
    }

    #[test]
    fn test_cold_to_synthetic_token_without_wallet_owner() {
        let mint = Pubkey::new_unique();
        let compressed_owner = [99u8; 32];
        let token_data = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from(compressed_owner),
            amount: UnsignedInteger(42),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };

        let cold = sample_token_cold(100, 1_000_000, &token_data);
        // No wallet owner → should fall back to compressed token owner.
        let result = cold_to_synthetic_account_data(&cold, None);

        let parsed = SplTokenAccount::unpack(&result.data.0).expect("valid SPL layout");
        assert_eq!(result.space.0, SplTokenAccount::LEN as u64);
        assert_eq!(
            result.owner,
            SerializablePubkey::from(LIGHT_TOKEN_PROGRAM_ID)
        );
        assert_eq!(parsed.owner, Pubkey::from(compressed_owner));
    }

    #[test]
    fn test_cold_to_synthetic_token_invalid_wallet_owner_uses_compressed_owner() {
        let mint = Pubkey::new_unique();
        let compressed_owner = [77u8; 32];
        let token_data = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from(compressed_owner),
            amount: UnsignedInteger(42),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };

        let cold = sample_token_cold(100, 1_000_000, &token_data);
        let invalid_owner = [1u8; 31];
        let result = cold_to_synthetic_account_data(&cold, Some(&invalid_owner));

        let parsed = SplTokenAccount::unpack(&result.data.0).expect("valid SPL layout");
        assert_eq!(parsed.owner, Pubkey::from(compressed_owner));
    }

    #[test]
    fn test_cold_to_synthetic_non_token() {
        let cold = sample_cold(100, 500);
        let wallet_owner = [55u8; 32];

        // Non-token account with wallet_owner should be unaffected
        let result = cold_to_synthetic_account_data(&cold, Some(&wallet_owner));

        // Should use fallback (discriminator + data), NOT 165-byte SPL layout
        assert_ne!(result.data.0.len(), SplTokenAccount::LEN);
        // Owner should remain as the account's owner (default pubkey)
        assert_eq!(result.owner, SerializablePubkey::default());
    }

    // ============ Decompressed placeholder filtering tests ============

    #[test]
    fn test_find_cold_models_filters_decompressed_placeholders() {
        use crate::dao::generated::accounts;
        use sea_orm::prelude::Decimal;

        let decompressed_disc = Decimal::from(DECOMPRESSED_ACCOUNT_DISCRIMINATOR);
        let normal_disc = Decimal::from(0x0807060504030201u64);

        let make_model =
            |hash: Vec<u8>, discriminator: Option<Decimal>, lamports: i64| accounts::Model {
                hash,
                address: None,
                discriminator,
                data: None,
                data_hash: None,
                tree: vec![],
                leaf_index: 0,
                seq: Some(1),
                slot_created: 100,
                owner: vec![0u8; 32],
                lamports: Decimal::from(lamports),
                spent: false,
                prev_spent: Some(false),
                tx_hash: None,
                onchain_pubkey: None,
                tree_type: Some(3),
                nullified_in_tree: false,
                nullifier_queue_index: None,
                in_output_queue: false,
                queue: None,
                nullifier: None,
            };

        let placeholder = make_model(vec![1u8; 32], Some(decompressed_disc), 0);
        let normal = make_model(vec![2u8; 32], Some(normal_disc), 1000);
        let no_disc = make_model(vec![3u8; 32], None, 500);

        // Simulate the filtering logic from find_cold_models.
        let by_hash: HashMap<Vec<u8>, accounts::Model> = vec![
            (placeholder.hash.clone(), placeholder),
            (normal.hash.clone(), normal),
            (no_disc.hash.clone(), no_disc),
        ]
        .into_iter()
        .collect();

        let decompressed_disc_decimal = Decimal::from(DECOMPRESSED_ACCOUNT_DISCRIMINATOR);
        let filtered: Vec<_> = by_hash
            .into_values()
            .filter(|m| m.discriminator != Some(decompressed_disc_decimal))
            .collect();

        // Placeholder should be filtered out, normal and no-disc should remain.
        assert_eq!(filtered.len(), 2);
        assert!(filtered
            .iter()
            .all(|m| m.discriminator != Some(decompressed_disc_decimal)));
        let hashes: Vec<&Vec<u8>> = filtered.iter().map(|m| &m.hash).collect();
        assert!(hashes.contains(&&vec![2u8; 32]));
        assert!(hashes.contains(&&vec![3u8; 32]));
    }

    // ============ Fungible token amount aggregation tests ============

    #[test]
    fn test_resolve_aggregates_fungible_token_amounts() {
        let mint = Pubkey::new_unique();
        let wallet_owner = [55u8; 32];
        let hash1 = Hash::try_from(vec![1u8; 32]).unwrap();
        let hash2 = Hash::try_from(vec![2u8; 32]).unwrap();
        let hash3 = Hash::try_from(vec![3u8; 32]).unwrap();

        let td1 = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(100),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };
        let td2 = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(250),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };
        let td3 = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(650),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };

        let cold1 = sample_token_cold_with_hash(100, 0, &td1, hash1);
        let cold2 = sample_token_cold_with_hash(200, 0, &td2, hash2);
        let cold3 = sample_token_cold_with_hash(300, 0, &td3, hash3.clone());

        let cold_accounts = vec![cold1, cold2, cold3];

        // Map newest hash → wallet owner
        let mut token_wallet_owners = HashMap::new();
        token_wallet_owners.insert(hash3, wallet_owner);

        let result = resolve_single_race(
            None,
            &cold_accounts,
            0,
            SerializablePubkey::default(),
            &token_wallet_owners,
        )
        .expect("expected Some interface");

        // Primary view should have aggregated amount = 100 + 250 + 650 = 1000
        let parsed = SplTokenAccount::unpack(&result.account.data.0).expect("valid SPL layout");
        assert_eq!(parsed.amount, 1000);
        assert_eq!(parsed.owner, Pubkey::from(wallet_owner));

        // All cold accounts should be included
        assert_eq!(result.cold.as_ref().unwrap().len(), 3);
    }

    #[test]
    fn test_resolve_mixed_mints_uses_newest() {
        let mint_a = Pubkey::new_unique();
        let mint_b = Pubkey::new_unique();

        let hash1 = Hash::try_from(vec![1u8; 32]).unwrap();
        let hash2 = Hash::try_from(vec![2u8; 32]).unwrap();

        let td1 = TokenData {
            mint: SerializablePubkey::from(mint_a),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(100),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };
        let td2 = TokenData {
            mint: SerializablePubkey::from(mint_b),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(200),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };

        let cold1 = sample_token_cold_with_hash(100, 0, &td1, hash1);
        let cold2 = sample_token_cold_with_hash(200, 0, &td2, hash2.clone());

        let cold_accounts = vec![cold1, cold2];

        let mut token_wallet_owners = HashMap::new();
        token_wallet_owners.insert(hash2, [55u8; 32]);

        let result = resolve_single_race(
            None,
            &cold_accounts,
            0,
            SerializablePubkey::default(),
            &token_wallet_owners,
        )
        .expect("expected Some interface");

        // Mixed mints → fallback to newest by slot (cold2, amount=200)
        let parsed = SplTokenAccount::unpack(&result.account.data.0).expect("valid SPL layout");
        assert_eq!(parsed.amount, 200);
    }

    #[test]
    fn test_resolve_single_token_unchanged() {
        let mint = Pubkey::new_unique();
        let wallet_owner = [55u8; 32];
        let hash1 = Hash::try_from(vec![1u8; 32]).unwrap();

        let td = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(42),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };

        let cold = sample_token_cold_with_hash(100, 1_000_000, &td, hash1.clone());
        let cold_accounts = vec![cold];

        let mut token_wallet_owners = HashMap::new();
        token_wallet_owners.insert(hash1, wallet_owner);

        let result = resolve_single_race(
            None,
            &cold_accounts,
            0,
            SerializablePubkey::default(),
            &token_wallet_owners,
        )
        .expect("expected Some interface");

        // Single token account → amount should be 42, unchanged
        let parsed = SplTokenAccount::unpack(&result.account.data.0).expect("valid SPL layout");
        assert_eq!(parsed.amount, 42);
        assert_eq!(parsed.owner, Pubkey::from(wallet_owner));
    }

    // ============ Hot + cold aggregation tests ============

    /// Helper to build a hot SolanaAccount that looks like an SPL token ATA.
    fn sample_hot_token_account(mint: &Pubkey, owner: &Pubkey, amount: u64) -> SolanaAccount {
        use solana_program_option::COption;
        use spl_token_interface::state::AccountState as SplAccountState;

        let spl_account = SplTokenAccount {
            mint: *mint,
            owner: *owner,
            amount,
            delegate: COption::None,
            state: SplAccountState::Initialized,
            is_native: COption::None,
            delegated_amount: 0,
            close_authority: COption::None,
        };

        let mut data = vec![0u8; SplTokenAccount::LEN];
        SplTokenAccount::pack(spl_account, &mut data).unwrap();

        // Use the real SPL Token program id.
        let spl_token_program = Pubkey::try_from(spl_token_interface::ID.as_ref()).unwrap();

        SolanaAccount {
            lamports: 2_039_280, // typical rent-exempt ATA
            data,
            owner: spl_token_program,
            executable: false,
            rent_epoch: 0,
        }
    }

    #[test]
    fn test_resolve_aggregates_hot_and_cold_token_amounts() {
        let mint = Pubkey::new_unique();
        let wallet_owner = Pubkey::new_unique();
        let hash1 = Hash::try_from(vec![1u8; 32]).unwrap();
        let hash2 = Hash::try_from(vec![2u8; 32]).unwrap();

        // Hot ATA with 500 tokens.
        let hot = sample_hot_token_account(&mint, &wallet_owner, 500);

        // Two cold compressed token accounts, 100 + 400 tokens.
        let td1 = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(100),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };
        let td2 = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(400),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };

        let cold1 = sample_token_cold_with_hash(100, 0, &td1, hash1);
        let cold2 = sample_token_cold_with_hash(200, 0, &td2, hash2);
        let cold_accounts = vec![cold1, cold2];

        let result = resolve_single_race(
            Some(&hot),
            &cold_accounts,
            300, // hot_slot
            SerializablePubkey::default(),
            &HashMap::new(),
        )
        .expect("expected Some interface");

        // Primary view should have aggregated amount = 500 (hot) + 100 + 400 (cold) = 1000
        let parsed = SplTokenAccount::unpack(&result.account.data.0).expect("valid SPL layout");
        assert_eq!(parsed.amount, 1000);
        // Wallet owner should come from the hot ATA.
        assert_eq!(parsed.owner, wallet_owner);
        assert_eq!(parsed.mint, mint);
        // Cold accounts still included.
        assert_eq!(result.cold.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_resolve_hot_token_different_mint_from_cold_uses_hot() {
        let mint_hot = Pubkey::new_unique();
        let mint_cold = Pubkey::new_unique();
        let wallet_owner = Pubkey::new_unique();
        let hash1 = Hash::try_from(vec![1u8; 32]).unwrap();

        let hot = sample_hot_token_account(&mint_hot, &wallet_owner, 500);

        let td = TokenData {
            mint: SerializablePubkey::from(mint_cold),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(300),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };
        let cold = sample_token_cold_with_hash(100, 0, &td, hash1);

        let result = resolve_single_race(
            Some(&hot),
            std::slice::from_ref(&cold),
            300,
            SerializablePubkey::default(),
            &HashMap::new(),
        )
        .expect("expected Some interface");

        // Different mints → fallback to hot account (since hot is present).
        let parsed = SplTokenAccount::unpack(&result.account.data.0).expect("valid SPL layout");
        assert_eq!(parsed.amount, 500);
        assert_eq!(parsed.mint, mint_hot);
    }

    #[test]
    fn test_resolve_hot_non_token_with_cold_tokens_uses_hot() {
        let mint = Pubkey::new_unique();
        let hash1 = Hash::try_from(vec![1u8; 32]).unwrap();

        // Hot is not a token account (random data, too short for SPL).
        let hot = SolanaAccount {
            lamports: 1000,
            data: vec![1, 2, 3],
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 0,
        };

        let td = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from([99u8; 32]),
            amount: UnsignedInteger(300),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };
        let cold = sample_token_cold_with_hash(100, 0, &td, hash1);

        let result = resolve_single_race(
            Some(&hot),
            std::slice::from_ref(&cold),
            300,
            SerializablePubkey::default(),
            &HashMap::new(),
        )
        .expect("expected Some interface");

        // Hot is not a token → can't aggregate → fallback to hot.
        assert_eq!(result.account.lamports.0, 1000);
        assert_eq!(result.account.data.0, vec![1, 2, 3]);
    }
}
