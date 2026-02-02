use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::mint_data::MintData;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::token_data::TokenData;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;

/// Nested Solana account fields (matches getAccountInfo shape)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SolanaAccountData {
    pub lamports: UnsignedInteger,
    pub data: Base64String,
    pub owner: SerializablePubkey,
    pub executable: bool,
    pub rent_epoch: UnsignedInteger,
    pub space: UnsignedInteger,
}

/// Tree type enum matching light-protocol's TreeType.
/// Values match light-compressed-account::TreeType: StateV1=1, StateV2=3
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[repr(u64)]
#[derive(Default)]
pub enum TreeType {
    /// Legacy V1 state tree (tree and queue are the same)
    #[serde(rename = "stateV1")]
    #[default]
    StateV1 = 1,
    /// V2 state tree with separate output queue
    #[serde(rename = "stateV2")]
    StateV2 = 3,
}

impl From<i32> for TreeType {
    fn from(value: i32) -> Self {
        match value {
            1 => TreeType::StateV1,
            3 => TreeType::StateV2,
            _ => TreeType::StateV1, // Default fallback
        }
    }
}

/// Merkle tree info for compressed accounts
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TreeInfo {
    /// The merkle tree pubkey
    pub tree: SerializablePubkey,
    /// The output queue pubkey (same as tree for V1, separate for V2)
    pub queue: SerializablePubkey,
    /// The tree type (V1 or V2)
    pub tree_type: TreeType,
    /// Sequence number of the account in the tree
    pub seq: Option<UnsignedInteger>,
}

/// Structured compressed account data (discriminator separated)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ColdData {
    pub discriminator: Vec<u8>,
    pub data: Base64String,
}

/// Compressed account context — present when account is in compressed state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ColdContext {
    #[serde(rename = "account")]
    Account {
        hash: Hash,
        #[serde(rename = "leafIndex")]
        leaf_index: UnsignedInteger,
        #[serde(rename = "treeInfo")]
        tree_info: TreeInfo,
        data: ColdData,
    },
    #[serde(rename = "token")]
    Token {
        hash: Hash,
        #[serde(rename = "leafIndex")]
        leaf_index: UnsignedInteger,
        #[serde(rename = "treeInfo")]
        tree_info: TreeInfo,
        data: ColdData,
    },
    #[serde(rename = "mint")]
    Mint {
        hash: Hash,
        #[serde(rename = "leafIndex")]
        leaf_index: UnsignedInteger,
        #[serde(rename = "treeInfo")]
        tree_info: TreeInfo,
        data: ColdData,
    },
}

/// Unified account interface — works for both on-chain and compressed accounts
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccountInterface {
    /// The on-chain Solana pubkey
    pub key: SerializablePubkey,
    /// Standard Solana account fields
    pub account: SolanaAccountData,
    /// Compressed context — null if on-chain, present if compressed
    pub cold: Option<ColdContext>,
}

/// Token account interface with parsed token data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TokenAccountInterface {
    #[serde(flatten)]
    pub account: AccountInterface,
    pub token_data: TokenData,
}

/// Mint account interface with parsed mint data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MintInterface {
    #[serde(flatten)]
    pub account: AccountInterface,
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

/// Request for getMintInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMintInterfaceRequest {
    /// The mint address to look up
    pub address: SerializablePubkey,
}

/// Request for getAtaInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAtaInterfaceRequest {
    /// The wallet address that owns the ATA
    pub owner: SerializablePubkey,
    /// The token mint address
    pub mint: SerializablePubkey,
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

/// Response for getMintInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetMintInterfaceResponse {
    /// Current context (slot)
    pub context: Context,
    /// The mint data, or None if not found
    pub value: Option<MintInterface>,
}

/// Response for getAtaInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetAtaInterfaceResponse {
    /// Current context (slot)
    pub context: Context,
    /// The token account data, or None if not found
    pub value: Option<TokenAccountInterface>,
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

/// Request for getTokenAccountInterfaces (batch)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetTokenAccountInterfacesRequest {
    /// List of token account addresses to look up (max 100)
    pub addresses: Vec<SerializablePubkey>,
}

/// Response for getTokenAccountInterfaces (batch)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetTokenAccountInterfacesResponse {
    pub context: Context,
    pub value: Vec<Option<TokenAccountInterface>>,
}

/// Request for getAccountInterfaces (batch)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAccountInterfacesRequest {
    /// List of account addresses to look up (max 100)
    pub addresses: Vec<SerializablePubkey>,
}

/// Response for getAccountInterfaces (batch)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetAccountInterfacesResponse {
    pub context: Context,
    pub value: Vec<Option<AccountInterface>>,
}

/// Request for getMintAccountInterfaces (batch)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMintAccountInterfacesRequest {
    /// List of mint addresses to look up (max 100)
    pub addresses: Vec<SerializablePubkey>,
}

/// Response for getMintAccountInterfaces (batch)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetMintAccountInterfacesResponse {
    pub context: Context,
    pub value: Vec<Option<MintInterface>>,
}

// ============ Constants ============

/// Maximum number of accounts that can be looked up in a single batch request
pub const MAX_BATCH_SIZE: usize = 100;

/// RPC timeout in milliseconds for hot lookups
pub const RPC_TIMEOUT_MS: u64 = 5000;

/// Database timeout in milliseconds for cold lookups
pub const DB_TIMEOUT_MS: u64 = 3000;
