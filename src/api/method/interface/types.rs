use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::common::typedefs::account::AccountV2;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
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

/// Unified account interface — works for both on-chain and compressed accounts
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AccountInterface {
    /// The queried Solana pubkey
    pub key: SerializablePubkey,
    /// Standard Solana account fields (hot view or synthetic cold view)
    pub account: SolanaAccountData,
    /// Compressed accounts associated with this pubkey
    pub cold: Option<Vec<AccountV2>>,
}

// ============ Request Types ============

/// Request for getAccountInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAccountInterfaceRequest {
    /// The account address to look up
    pub address: SerializablePubkey,
}

/// Request for getMultipleAccountInterfaces
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleAccountInterfacesRequest {
    /// List of account addresses to look up (max 100)
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

/// Response for getMultipleAccountInterfaces
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetMultipleAccountInterfacesResponse {
    /// Current context (slot)
    pub context: Context,
    /// List of account results (Some for found accounts, None for not found)
    pub value: Vec<Option<AccountInterface>>,
}

// ============ Constants ============

/// Maximum number of accounts that can be looked up in a single batch request
pub const MAX_BATCH_SIZE: usize = 100;

/// RPC timeout in milliseconds for hot lookups
pub const RPC_TIMEOUT_MS: u64 = 5000;

/// Database timeout in milliseconds for cold lookups
pub const DB_TIMEOUT_MS: u64 = 3000;
