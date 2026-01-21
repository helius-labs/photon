use crate::api::error::PhotonApiError;
use crate::api::method::utils::parse_decimal;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::mint_data::MintData;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::token_data::TokenData;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::accounts::Model;
use crate::ingester::error::IngesterError;
use crate::ingester::persist::COMPRESSED_TOKEN_PROGRAM;
use jsonrpsee_core::Serialize;
use utoipa::ToSchema;

pub const C_TOKEN_DISCRIMINATOR_V1: [u8; 8] = [2, 0, 0, 0, 0, 0, 0, 0];
pub const C_TOKEN_DISCRIMINATOR_V2: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 3];
pub const C_TOKEN_DISCRIMINATOR_V3: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 4];
/// Discriminator for compressed mints (value 1 as u64 in big-endian bytes)
/// Matches COMPRESSED_MINT_DISCRIMINATOR from light-protocol constants.rs
pub const COMPRESSED_MINT_DISCRIMINATOR: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 1];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Account {
    pub hash: Hash,
    pub address: Option<SerializablePubkey>,
    pub data: Option<AccountData>,
    pub owner: SerializablePubkey,
    pub lamports: UnsignedInteger,
    pub tree: SerializablePubkey,
    pub leaf_index: UnsignedInteger,
    // For V1 trees is always Some() since the user tx appends directly to the Merkle tree
    // for V2 batched trees:
    // 2.1. None when is in output queue
    // 2.2. Some once it was inserted into the Merkle tree from the output queue
    pub seq: Option<UnsignedInteger>,
    pub slot_created: UnsignedInteger,
}

impl Account {
    pub fn parse_token_data(&self) -> Result<Option<TokenData>, IngesterError> {
        match self.data.as_ref() {
            Some(data)
                if self.owner.0 == COMPRESSED_TOKEN_PROGRAM && data.is_c_token_discriminator() =>
            {
                let data_slice = data.data.0.as_slice();
                let token_data = TokenData::parse(data_slice).map_err(|e| {
                    IngesterError::ParserError(format!("Failed to parse token data: {:?}", e))
                })?;
                Ok(Some(token_data))
            }
            _ => Ok(None),
        }
    }

    pub fn parse_mint_data(&self) -> Result<Option<MintData>, IngesterError> {
        // First, try discriminator-based detection
        if let Some(data) = self.data.as_ref() {
            if self.owner.0 == COMPRESSED_TOKEN_PROGRAM && data.is_compressed_mint_discriminator() {
                let data_slice = data.data.0.as_slice();
                let mint_data = MintData::parse(data_slice).map_err(|e| {
                    IngesterError::ParserError(format!("Failed to parse mint data: {:?}", e))
                })?;
                return Ok(Some(mint_data));
            }
        }

        // Fallback: If owned by compressed token program and not a token account,
        // try to parse as mint (handles cases where discriminator might differ)
        if let Some(data) = self.data.as_ref() {
            if self.owner.0 == COMPRESSED_TOKEN_PROGRAM && !data.is_c_token_discriminator() {
                let data_slice = data.data.0.as_slice();
                // Try parsing - if it succeeds, it's a mint
                // Note: For v2 read-only accounts, only the hash is available (32 bytes),
                // so parsing will fail. Full mint data support for v2 requires additional work.
                if let Ok(mint_data) = MintData::parse(data_slice) {
                    // Verify it looks like a valid mint (has mint_pda set)
                    if mint_data.mint_pda.to_bytes_vec() != vec![0u8; 32] {
                        return Ok(Some(mint_data));
                    }
                }
            }
        }

        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountData {
    pub discriminator: UnsignedInteger,
    pub data: Base64String,
    pub data_hash: Hash,
}

impl AccountData {
    pub fn is_c_token_discriminator(&self) -> bool {
        let bytes = self.discriminator.0.to_le_bytes();
        bytes == C_TOKEN_DISCRIMINATOR_V1
            || bytes == C_TOKEN_DISCRIMINATOR_V2
            || bytes == C_TOKEN_DISCRIMINATOR_V3
    }

    pub fn is_compressed_mint_discriminator(&self) -> bool {
        let bytes = self.discriminator.0.to_le_bytes();
        bytes == COMPRESSED_MINT_DISCRIMINATOR
    }
}

impl TryFrom<Model> for Account {
    type Error = PhotonApiError;

    fn try_from(account: Model) -> Result<Self, Self::Error> {
        let data = match (account.data, account.data_hash, account.discriminator) {
            (Some(data), Some(data_hash), Some(discriminator)) => Some(AccountData {
                data: Base64String(data),
                data_hash: data_hash.try_into()?,
                discriminator: UnsignedInteger(parse_decimal(discriminator)?),
            }),
            (None, None, None) => None,
            _ => {
                return Err(PhotonApiError::UnexpectedError(
                    "Invalid account data".to_string(),
                ))
            }
        };

        Ok(Account {
            hash: account.hash.try_into()?,
            address: account
                .address
                .map(SerializablePubkey::try_from)
                .transpose()?,
            data,
            owner: account.owner.try_into()?,
            tree: account.tree.try_into()?,
            leaf_index: UnsignedInteger(crate::api::method::utils::parse_leaf_index(
                account.leaf_index,
            )?),
            lamports: UnsignedInteger(parse_decimal(account.lamports)?),
            slot_created: UnsignedInteger(account.slot_created as u64),
            seq: account.seq.map(|seq| UnsignedInteger(seq as u64)),
        })
    }
}
