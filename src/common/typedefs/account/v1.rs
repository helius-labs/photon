use crate::api::error::PhotonApiError;
use crate::api::method::utils::parse_decimal;
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::token_data::TokenData;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::accounts::Model;
use crate::ingester::error::IngesterError;
use crate::ingester::persist::LIGHT_TOKEN_PROGRAM_ID;
use jsonrpsee_core::Serialize;
use light_sdk_types::TOKEN_COMPRESSED_ACCOUNT_DISCRIMINATOR;
use utoipa::ToSchema;

/// Re-export V1 discriminator from light-sdk-types under local naming convention.
pub const C_TOKEN_DISCRIMINATOR_V1: [u8; 8] = TOKEN_COMPRESSED_ACCOUNT_DISCRIMINATOR;
/// V2: batched Merkle trees (not yet exported from SDK crates)
pub const C_TOKEN_DISCRIMINATOR_V2: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 3];
/// V3/ShaFlat: SHA256 flat hash with TLV extensions (not yet exported from SDK crates)
pub const C_TOKEN_DISCRIMINATOR_V3: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 4];

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
                if self.owner.0 == LIGHT_TOKEN_PROGRAM_ID && data.is_c_token_discriminator() =>
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
}

/// Parse discriminator from BLOB (8 bytes little-endian) to u64
fn parse_discriminator_blob(blob: Vec<u8>) -> Result<u64, PhotonApiError> {
    if blob.len() != 8 {
        return Err(PhotonApiError::UnexpectedError(format!(
            "Discriminator has unexpected length {}, expected 8",
            blob.len()
        )));
    }
    let bytes: [u8; 8] = blob
        .try_into()
        .map_err(|_| PhotonApiError::UnexpectedError("Invalid discriminator bytes".to_string()))?;
    Ok(u64::from_le_bytes(bytes))
}

impl TryFrom<Model> for Account {
    type Error = PhotonApiError;

    fn try_from(account: Model) -> Result<Self, Self::Error> {
        let data = match (account.data, account.data_hash, account.discriminator) {
            (Some(data), Some(data_hash), Some(discriminator)) => Some(AccountData {
                data: Base64String(data),
                data_hash: data_hash.try_into()?,
                discriminator: UnsignedInteger(parse_discriminator_blob(discriminator)?),
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
