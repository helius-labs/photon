use std::time::Duration;

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
use crate::dao::generated::{accounts, token_accounts};
use crate::ingester::persist::LIGHT_TOKEN_PROGRAM_ID;

use super::racing::split_discriminator;
use super::types::{
    AccountInterface, ColdContext, ColdData, GetAtaInterfaceRequest, GetAtaInterfaceResponse,
    SolanaAccountData, TokenAccountInterface, TreeInfo, TreeType, DB_TIMEOUT_MS, RPC_TIMEOUT_MS,
};

/// SPL Token program ID (TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA)
const SPL_TOKEN_PROGRAM_ID: Pubkey = solana_pubkey::Pubkey::new_from_array([
    6, 221, 246, 225, 215, 101, 161, 147, 217, 203, 225, 70, 206, 235, 121, 172, 28, 180, 133, 237,
    95, 91, 55, 145, 58, 140, 245, 133, 126, 255, 0, 169,
]);
/// SPL Token 2022 program ID (TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb)
const SPL_TOKEN_2022_PROGRAM_ID: Pubkey = solana_pubkey::Pubkey::new_from_array([
    6, 229, 125, 73, 229, 70, 55, 129, 147, 131, 175, 147, 195, 105, 248, 69, 44, 204, 103, 119,
    171, 254, 145, 234, 51, 98, 248, 141, 155, 218, 244, 191,
]);

// SPL Associated Token Account program ID
const ATA_PROGRAM_ID: [u8; 32] = [
    140, 151, 37, 143, 78, 36, 137, 241, 187, 61, 16, 41, 20, 142, 13, 131, 11, 90, 19, 153, 218,
    255, 16, 132, 4, 142, 123, 216, 219, 233, 248, 89,
];

/// Get ATA interface by owner (wallet) address and mint.
/// Looks up the compressed token account where ata_owner matches the owner.
/// Also races against on-chain lookup using the derived ATA addresses (both SPL and Light Token).
pub async fn get_ata_interface(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    request: GetAtaInterfaceRequest,
) -> Result<GetAtaInterfaceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let owner = Pubkey::from(request.owner.0.to_bytes());
    let mint = Pubkey::from(request.mint.0.to_bytes());

    // Derive both SPL ATA and Light Token ATA addresses
    let spl_ata_address = derive_spl_ata_address(&owner, &mint);
    let light_ata_address = derive_light_token_ata_address(&owner, &mint);

    // Race all lookups in parallel
    let (spl_hot_result, light_hot_result, cold_result) = tokio::join!(
        hot_ata_lookup(rpc_client, &spl_ata_address),
        hot_ata_lookup(rpc_client, &light_ata_address),
        cold_ata_lookup(conn, &request.owner, &request.mint)
    );

    let value = resolve_ata_race_multi(
        spl_hot_result,
        light_hot_result,
        cold_result,
        SerializablePubkey::from(spl_ata_address.to_bytes()),
        SerializablePubkey::from(light_ata_address.to_bytes()),
    )?;

    Ok(GetAtaInterfaceResponse { context, value })
}

/// Derive SPL ATA address from owner and mint
/// Uses the standard SPL ATA derivation: PDA([owner, token_program, mint], ata_program)
fn derive_spl_ata_address(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    let ata_program = Pubkey::new_from_array(ATA_PROGRAM_ID);
    let seeds = &[owner.as_ref(), SPL_TOKEN_PROGRAM_ID.as_ref(), mint.as_ref()];
    let (address, _bump) = Pubkey::find_program_address(seeds, &ata_program);
    address
}

/// Derive Light Token ATA address from owner and mint
/// Uses Light Protocol ATA derivation: PDA([owner, light_token_program, mint], light_token_program)
fn derive_light_token_ata_address(owner: &Pubkey, mint: &Pubkey) -> Pubkey {
    let seeds = &[
        owner.as_ref(),
        LIGHT_TOKEN_PROGRAM_ID.as_ref(),
        mint.as_ref(),
    ];

    let (address, _bump) = Pubkey::find_program_address(seeds, &LIGHT_TOKEN_PROGRAM_ID);
    address
}

/// Hot lookup for on-chain ATA or CToken account
async fn hot_ata_lookup(
    rpc_client: &RpcClient,
    ata_address: &Pubkey,
) -> Result<Option<(SolanaAccount, u64)>, PhotonApiError> {
    let result = timeout(
        Duration::from_millis(RPC_TIMEOUT_MS),
        rpc_client.get_account_with_commitment(ata_address, CommitmentConfig::confirmed()),
    )
    .await;

    match result {
        Ok(Ok(response)) => {
            if let Some(account) = response.value {
                let slot = response.context.slot;
                // Accept SPL Token, Token 2022, or Light Token program
                if account.owner == SPL_TOKEN_PROGRAM_ID
                    || account.owner == SPL_TOKEN_2022_PROGRAM_ID
                    || account.owner == LIGHT_TOKEN_PROGRAM_ID
                {
                    Ok(Some((account, slot)))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
        Ok(Err(e)) => Err(PhotonApiError::UnexpectedError(format!(
            "RPC error during hot ATA lookup: {}",
            e
        ))),
        Err(_) => Err(PhotonApiError::UnexpectedError(
            "Timeout during hot ATA lookup".to_string(),
        )),
    }
}

/// Cold lookup for compressed token account by ata_owner.
/// Queries token_accounts where ata_owner matches the owner and mint matches.
async fn cold_ata_lookup(
    conn: &DatabaseConnection,
    owner: &SerializablePubkey,
    mint: &SerializablePubkey,
) -> Result<Option<ColdAtaData>, PhotonApiError> {
    let owner_bytes: Vec<u8> = (*owner).into();
    let mint_bytes: Vec<u8> = (*mint).into();

    let result = timeout(
        Duration::from_millis(DB_TIMEOUT_MS),
        token_accounts::Entity::find()
            .filter(
                Condition::all()
                    .add(token_accounts::Column::AtaOwner.eq(owner_bytes))
                    .add(token_accounts::Column::Mint.eq(mint_bytes))
                    .add(token_accounts::Column::Spent.eq(false)),
            )
            .find_also_related(accounts::Entity)
            .one(conn),
    )
    .await;

    match result {
        Ok(Ok(Some((token_account, Some(account))))) => {
            // Pass the wallet owner (from the request) to parse_token_account_data
            // This is the actual owner who can sign transactions, not the token account PDA
            let token_data = parse_token_account_data(&token_account, owner)?;

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

            let cold_data = ColdAtaData {
                hash: account.hash.clone().try_into()?,
                data: account.data.clone(),
                owner: account.owner.clone().try_into()?,
                lamports: parse_decimal(account.lamports)?,
                tree,
                queue,
                tree_type,
                leaf_index: crate::api::method::utils::parse_leaf_index(account.leaf_index)?,
                seq: account.seq.map(|s| s as u64),
                token_data,
                onchain_pubkey: account.onchain_pubkey.clone(),
            };
            Ok(Some(cold_data))
        }
        Ok(Ok(Some((_, None)))) => {
            log::warn!("Token account found but related account is missing");
            Ok(None)
        }
        Ok(Ok(None)) => Ok(None),
        Ok(Err(e)) => Err(PhotonApiError::DatabaseError(e)),
        Err(_) => Err(PhotonApiError::UnexpectedError(
            "Timeout during cold ATA lookup".to_string(),
        )),
    }
}

/// Compressed ATA data with parsed token info
#[derive(Debug)]
struct ColdAtaData {
    hash: Hash,
    data: Option<Vec<u8>>,
    owner: SerializablePubkey,
    lamports: u64,
    tree: SerializablePubkey,
    queue: SerializablePubkey,
    tree_type: TreeType,
    leaf_index: u64,
    seq: Option<u64>,
    token_data: TokenData,
    onchain_pubkey: Option<Vec<u8>>,
}

/// Parse token data from the token_accounts table.
///
/// # Arguments
/// * `token_account` - The token account model from the database
/// * `wallet_owner` - The wallet owner (from the request) who can sign transactions.
///   For ATAs, this is the human user's wallet, not the token account PDA address.
fn parse_token_account_data(
    token_account: &token_accounts::Model,
    wallet_owner: &SerializablePubkey,
) -> Result<TokenData, PhotonApiError> {
    let state = match token_account.state {
        0 | 1 => AccountState::initialized,
        2 => AccountState::frozen,
        _ => AccountState::initialized,
    };

    let delegate = token_account
        .delegate
        .as_ref()
        .map(|d| SerializablePubkey::try_from(d.clone()))
        .transpose()?;

    // Parse TLV if present - just wrap it in Base64String
    let tlv = token_account
        .tlv
        .as_ref()
        .map(|tlv_data| Base64String(tlv_data.clone()));

    // Use the wallet_owner from the request, NOT token_account.owner.
    // For ATAs, token_account.owner is the ATA PDA address, but the client
    // needs the wallet owner's pubkey to sign transactions (Transfer2).
    Ok(TokenData {
        mint: token_account.mint.clone().try_into()?,
        owner: *wallet_owner,
        amount: UnsignedInteger(parse_decimal(token_account.amount)?),
        delegate,
        state,
        tlv,
    })
}

/// Resolve the ATA race result between multiple hot lookups and cold lookup.
/// Checks SPL ATA, Light Token ATA, and compressed token accounts.
fn resolve_ata_race_multi(
    spl_hot_result: Result<Option<(SolanaAccount, u64)>, PhotonApiError>,
    light_hot_result: Result<Option<(SolanaAccount, u64)>, PhotonApiError>,
    cold_result: Result<Option<ColdAtaData>, PhotonApiError>,
    spl_ata_address: SerializablePubkey,
    light_ata_address: SerializablePubkey,
) -> Result<Option<TokenAccountInterface>, PhotonApiError> {
    // Try Light Token ATA first (more likely for this use case)
    if let Ok(Some((account, slot))) = &light_hot_result {
        if account.lamports > 0 {
            if let Some(interface) = parse_hot_token(account, *slot, light_ata_address)? {
                return Ok(Some(interface));
            }
        }
    }

    // Try SPL ATA
    if let Ok(Some((account, slot))) = &spl_hot_result {
        if account.lamports > 0 {
            if let Some(interface) = parse_hot_token(account, *slot, spl_ata_address)? {
                return Ok(Some(interface));
            }
        }
    }

    // Try cold lookup (compressed token account with ata_owner)
    if let Ok(Some(cold_data)) = cold_result {
        // Use light_ata_address as default for compressed tokens
        return Ok(cold_to_ata_interface(&cold_data, light_ata_address));
    }

    // If no lookups succeeded, log any errors
    if let Err(e) = &spl_hot_result {
        log::debug!("SPL ATA hot lookup failed: {:?}", e);
    }
    if let Err(e) = &light_hot_result {
        log::debug!("Light Token ATA hot lookup failed: {:?}", e);
    }

    Ok(None)
}

/// Parse on-chain token account (SPL Token or CToken)
fn parse_hot_token(
    account: &SolanaAccount,
    slot: u64,
    address: SerializablePubkey,
) -> Result<Option<TokenAccountInterface>, PhotonApiError> {
    if account.owner == LIGHT_TOKEN_PROGRAM_ID {
        // CToken (decompressed Light token)
        parse_light_token_data(&account.data)
            .map(|opt| opt.map(|td| hot_to_token_interface(account, address, slot, td)))
    } else {
        // Standard SPL Token - parse manually
        parse_spl_token_data(&account.data)
            .map(|opt| opt.map(|td| hot_to_token_interface(account, address, slot, td)))
    }
}

/// Parse Light Protocol CToken data
fn parse_light_token_data(data: &[u8]) -> Result<Option<TokenData>, PhotonApiError> {
    if data.len() < 165 {
        return Ok(None);
    }

    let (token, _remaining) = match Token::zero_copy_at(data) {
        Ok(result) => result,
        Err(e) => {
            log::debug!("Failed to parse CToken with zero_copy_at: {:?}", e);
            return Ok(None);
        }
    };

    if token.base.is_uninitialized() {
        return Ok(None);
    }

    let state = if token.base.is_frozen() {
        AccountState::frozen
    } else {
        AccountState::initialized
    };

    let delegate = token
        .base
        .delegate()
        .map(|d| SerializablePubkey::from(d.to_bytes()));

    Ok(Some(TokenData {
        mint: SerializablePubkey::from(token.base.mint.to_bytes()),
        owner: SerializablePubkey::from(token.base.owner.to_bytes()),
        amount: UnsignedInteger(token.base.amount.into()),
        delegate,
        state,
        tlv: None,
    }))
}

/// Parse standard SPL Token data manually (without spl-token crate)
/// SPL Token Account layout: mint (32) + owner (32) + amount (8) + delegate (36) + state (1) + ...
fn parse_spl_token_data(data: &[u8]) -> Result<Option<TokenData>, PhotonApiError> {
    // SPL Token account minimum size is 165 bytes
    if data.len() < 165 {
        return Ok(None);
    }

    // Parse fields from SPL Token account layout
    let mint = SerializablePubkey::try_from(data[0..32].to_vec())?;
    let owner = SerializablePubkey::try_from(data[32..64].to_vec())?;
    let amount = u64::from_le_bytes(data[64..72].try_into().unwrap());

    // Delegate option: 4-byte tag (0 or 1) + 32 bytes pubkey
    let delegate_tag = u32::from_le_bytes(data[72..76].try_into().unwrap());
    let delegate = if delegate_tag == 1 {
        Some(SerializablePubkey::try_from(data[76..108].to_vec())?)
    } else {
        None
    };

    // State is at offset 108
    let state_byte = data[108];
    let state = match state_byte {
        0 => {
            // Uninitialized - return None
            return Ok(None);
        }
        1 => AccountState::initialized,
        2 => AccountState::frozen,
        _ => AccountState::initialized,
    };

    Ok(Some(TokenData {
        mint,
        owner,
        amount: UnsignedInteger(amount),
        delegate,
        state,
        tlv: None,
    }))
}

/// Convert on-chain token to TokenAccountInterface
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
                space: UnsignedInteger(account.data.len() as u64),
            },
            cold: None,
        },
        token_data,
    }
}

/// Convert compressed ATA to TokenAccountInterface.
/// Uses the ATA address (derived from wallet + mint) as the key.
fn cold_to_ata_interface(
    cold: &ColdAtaData,
    ata_address: SerializablePubkey,
) -> Option<TokenAccountInterface> {
    // Use onchain_pubkey if available, otherwise use the ATA address
    let key = cold
        .onchain_pubkey
        .as_ref()
        .and_then(|pk| SerializablePubkey::try_from(pk.clone()).ok())
        .unwrap_or(ata_address);

    let raw_data = cold.data.clone().unwrap_or_default();
    let space = raw_data.len() as u64;
    let (discriminator, data_payload) = split_discriminator(&raw_data);

    Some(TokenAccountInterface {
        account: AccountInterface {
            key,
            account: SolanaAccountData {
                lamports: UnsignedInteger(cold.lamports),
                data: Base64String(raw_data),
                owner: cold.owner,
                executable: false,
                rent_epoch: UnsignedInteger(0),
                space: UnsignedInteger(space),
            },
            cold: Some(ColdContext::Token {
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
        token_data: cold.token_data.clone(),
    })
}
