use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::mint_data::MintData;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::token_data::TokenData;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum ResolvedFrom {
    Onchain,
    Compressed,
}

/// Context information for compressed accounts
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CompressedContext {
    /// The hash of the compressed account (leaf hash in Merkle tree)
    pub hash: Hash,
    /// The Merkle tree address
    pub tree: SerializablePubkey,
    /// The leaf index in the Merkle tree
    pub leaf_index: UnsignedInteger,
    /// Sequence number (None if in output queue, Some once inserted into Merkle tree)
    pub seq: Option<UnsignedInteger>,
    /// Whether the account can be proven by index (in output queue)
    pub prove_by_index: bool,
}

/// Unified account interface that represents either on-chain or compressed account data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccountInterface {
    /// The account address (pubkey for on-chain, compressed address for compressed)
    pub address: SerializablePubkey,
    /// Account lamports balance
    pub lamports: UnsignedInteger,
    /// The program owner of this account
    pub owner: SerializablePubkey,
    /// Account data as base64 encoded bytes
    pub data: Base64String,
    /// Whether the account is executable (always false for compressed)
    pub executable: bool,
    /// Rent epoch (always 0 for compressed)
    pub rent_epoch: UnsignedInteger,
    /// Source of the account data
    pub resolved_from: ResolvedFrom,
    /// Slot at which the account data was resolved
    pub resolved_slot: UnsignedInteger,
    /// Additional context for compressed accounts (None for on-chain)
    pub compressed_context: Option<CompressedContext>,
}

/// Token account interface with parsed token data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenAccountInterface {
    /// Base account interface data
    #[serde(flatten)]
    pub account: AccountInterface,
    /// Parsed token account data
    pub token_data: TokenData,
}

/// Mint account interface with parsed mint data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MintInterface {
    /// Base account interface data
    #[serde(flatten)]
    pub account: AccountInterface,
    /// Parsed mint data
    pub mint_data: MintData,
}

// ============ Typed Lookup Types ============

/// Typed account lookup for batch requests
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum AccountLookup {
    /// Generic account/PDA lookup by address
    #[serde(rename = "account")]
    Account {
        /// The account address to look up
        address: SerializablePubkey,
    },
    /// Token account lookup by address
    #[serde(rename = "token")]
    Token {
        /// The token account address to look up
        address: SerializablePubkey,
    },
    /// ATA lookup by owner and mint (derives the ATA address)
    #[serde(rename = "ata")]
    Ata {
        /// The wallet owner address
        owner: SerializablePubkey,
        /// The mint address
        mint: SerializablePubkey,
    },
    /// Mint account lookup by address
    #[serde(rename = "mint")]
    Mint {
        /// The mint address to look up
        address: SerializablePubkey,
    },
}

/// Heterogeneous result type for batch lookups
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum InterfaceResult {
    /// Generic account result
    #[serde(rename = "account")]
    Account(AccountInterface),
    /// Token account result with parsed token data
    #[serde(rename = "token")]
    Token(TokenAccountInterface),
    /// Mint account result with parsed mint data
    #[serde(rename = "mint")]
    Mint(MintInterface),
}

// ============ Request Types ============

/// Request for getAccountInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAccountInterfaceRequest {
    /// The account address to look up
    pub address: SerializablePubkey,
}

/// Request for getTokenAccountInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetTokenAccountInterfaceRequest {
    /// The token account address to look up
    pub address: SerializablePubkey,
}

/// Request for getAtaInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAtaInterfaceRequest {
    /// The wallet owner address
    pub owner: SerializablePubkey,
    /// The mint address
    pub mint: SerializablePubkey,
}

/// Request for getMintInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMintInterfaceRequest {
    /// The mint address to look up
    pub address: SerializablePubkey,
}

/// Request for getMultipleAccountInterfaces
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleAccountInterfacesRequest {
    /// List of account addresses to look up (max 100)
    /// Server auto-detects account type (account, token, mint) based on program owner and data
    pub addresses: Vec<SerializablePubkey>,
}

// ============ Response Types ============

/// Response for getAccountInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetAccountInterfaceResponse {
    /// Current context (slot)
    pub context: Context,
    /// The account data, or None if not found
    pub value: Option<AccountInterface>,
}

/// Response for getTokenAccountInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetTokenAccountInterfaceResponse {
    /// Current context (slot)
    pub context: Context,
    /// The token account data, or None if not found
    pub value: Option<TokenAccountInterface>,
}

/// Response for getAtaInterface (same as token account)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetAtaInterfaceResponse {
    /// Current context (slot)
    pub context: Context,
    /// The ATA data, or None if not found
    pub value: Option<TokenAccountInterface>,
}

/// Response for getMintInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetMintInterfaceResponse {
    /// Current context (slot)
    pub context: Context,
    /// The mint data, or None if not found
    pub value: Option<MintInterface>,
}

/// Response for getMultipleAccountInterfaces
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetMultipleAccountInterfacesResponse {
    /// Current context (slot)
    pub context: Context,
    /// List of typed results (Some for found accounts, None for not found)
    pub value: Vec<Option<InterfaceResult>>,
}

// ============ Constants ============

/// Maximum number of accounts that can be looked up in a single batch request
pub const MAX_BATCH_SIZE: usize = 100;

/// RPC timeout in milliseconds for hot lookups
pub const RPC_TIMEOUT_MS: u64 = 5000;

/// Database timeout in milliseconds for cold lookups
pub const DB_TIMEOUT_MS: u64 = 3000;
