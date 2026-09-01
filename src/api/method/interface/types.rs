use serde::{Deserialize, Serialize};
use solana_commitment_config::CommitmentConfig;
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
    /// Optional RPC commitment for the hot (on-chain) lookup
    pub commitment: Option<RpcCommitment>,
}

/// Request for getMultipleAccountInterfaces
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleAccountInterfacesRequest {
    /// List of account addresses to look up (max 100)
    pub addresses: Vec<SerializablePubkey>,
    /// Optional RPC commitment for hot (on-chain) lookups
    pub commitment: Option<RpcCommitment>,
}

/// Program mode for getAtaInterface canonical account selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub enum GetAtaProgramMode {
    /// Unified mode. Canonical key is light ATA.
    #[default]
    Auto,
    /// Force light-token ATA mode.
    Light,
    /// Force SPL ATA mode.
    Spl,
    /// Force Token-2022 ATA mode.
    Token2022,
}

/// Request for getAtaInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAtaInterfaceRequest {
    /// Wallet owner used for ATA derivation.
    pub owner: SerializablePubkey,
    /// Mint used for ATA derivation and cold mint scoping.
    pub mint: SerializablePubkey,
    /// Optional Solana-style config object.
    pub config: Option<GetAtaInterfaceConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAtaInterfaceConfig {
    /// Optional RPC commitment for hot lookups.
    pub commitment: Option<RpcCommitment>,
    /// Minimum context slot requirement (Solana RPC style).
    pub min_context_slot: Option<UnsignedInteger>,
    /// Optional token program id to force selection mode.
    /// Supported values:
    /// - light token program id
    /// - SPL Token program id
    /// - Token-2022 program id
    pub program_id: Option<SerializablePubkey>,
    /// Include SPL/T22 hot balances in canonical aggregation (auto mode only).
    pub wrap: Option<bool>,
    /// Allow PDA/off-curve owners for ATA derivation.
    pub allow_owner_off_curve: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub enum RpcCommitment {
    Processed,
    #[default]
    Confirmed,
    Finalized,
}

impl RpcCommitment {
    pub fn to_commitment_config(self) -> CommitmentConfig {
        match self {
            RpcCommitment::Processed => CommitmentConfig::processed(),
            RpcCommitment::Confirmed => CommitmentConfig::confirmed(),
            RpcCommitment::Finalized => CommitmentConfig::finalized(),
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetAtaHotEntry {
    pub address: SerializablePubkey,
    pub amount: UnsignedInteger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetAtaHotSources {
    pub light: Option<GetAtaHotEntry>,
    pub spl: Option<GetAtaHotEntry>,
    pub token2022: Option<GetAtaHotEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetAtaDerivedAddresses {
    pub light: SerializablePubkey,
    pub spl: SerializablePubkey,
    pub token2022: SerializablePubkey,
    pub canonical: SerializablePubkey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AtaInterfaceValue {
    /// Canonical ATA pubkey for selected mode.
    pub key: SerializablePubkey,
    pub owner: SerializablePubkey,
    pub mint: SerializablePubkey,
    pub mode: GetAtaProgramMode,
    pub wrap: bool,
    /// Derived ATA addresses for all supported token programs.
    pub addresses: GetAtaDerivedAddresses,
    /// Canonical synthetic account after aggregation.
    pub account: SolanaAccountData,
    /// Raw compressed inputs used in synthesis.
    pub cold: Option<Vec<AccountV2>>,
    /// Program-specific hot snapshot for write planners.
    pub hot: GetAtaHotSources,
}

/// Response for getAtaInterface
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetAtaInterfaceResponse {
    /// Current context (slot)
    pub context: Context,
    /// ATA interface data, or null if not found
    pub value: Option<AtaInterfaceValue>,
}

// ============ Constants ============

/// Maximum number of accounts that can be looked up in a single batch request
pub const MAX_BATCH_SIZE: usize = 100;

/// RPC timeout in milliseconds for hot lookups
pub const RPC_TIMEOUT_MS: u64 = 5000;

/// Database timeout in milliseconds for cold lookups
pub const DB_TIMEOUT_MS: u64 = 3000;
