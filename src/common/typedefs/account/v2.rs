use crate::api::error::PhotonApiError;
use crate::api::method::get_validity_proof::MerkleContextV2;
use crate::api::method::utils::parse_decimal;
use crate::common::typedefs::account::{AccountData, AccountWithContext};
use crate::common::typedefs::bs64_string::Base64String;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::token_data::TokenData;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::accounts::Model;
use crate::ingester::error::IngesterError;
use crate::ingester::persist::COMPRESSED_TOKEN_PROGRAM;
use borsh::BorshDeserialize;
use serde::Serialize;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountV2 {
    pub hash: Hash,
    pub address: Option<SerializablePubkey>,
    pub data: Option<AccountData>,
    pub owner: SerializablePubkey,
    pub lamports: UnsignedInteger,
    pub leaf_index: UnsignedInteger,
    // For legacy trees is always Some() since the user tx appends directly to the Merkle tree
    // for batched tress:
    // 2.1. None when is in output queue
    // 2.2. Some once it was inserted into the Merkle tree from the output queue
    pub seq: Option<UnsignedInteger>,
    pub slot_created: UnsignedInteger,
    // Indicates if the account is not yet provable by validity_proof. The
    // account resides in on-chain RAM, with leaf_index mapping to its position.
    // This allows the protocol to prove the account's validity using only the
    // leaf_index. Consumers use this to decide if a validity proof is needed,
    // saving one RPC roundtrip.
    pub prove_by_index: bool,
    pub merkle_context: MerkleContextV2,
}

impl AccountV2 {
    pub fn parse_token_data(&self) -> Result<Option<TokenData>, IngesterError> {
        match self.data.as_ref() {
            Some(data)
                if self.owner.0 == COMPRESSED_TOKEN_PROGRAM && data.is_c_token_discriminator() =>
            {
                let data_slice = data.data.0.as_slice();
                let token_data = TokenData::try_from_slice(data_slice).map_err(|e| {
                    IngesterError::ParserError(format!("Failed to parse token data: {:?}", e))
                })?;
                Ok(Some(token_data))
            }
            _ => Ok(None),
        }
    }
}

impl TryFrom<Model> for AccountV2 {
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

        Ok(AccountV2 {
            hash: account.hash.try_into()?,
            address: account
                .address
                .map(SerializablePubkey::try_from)
                .transpose()?,
            data,
            owner: account.owner.try_into()?,
            leaf_index: UnsignedInteger(crate::api::method::utils::parse_leaf_index(
                account.leaf_index,
            )?),
            lamports: UnsignedInteger(parse_decimal(account.lamports)?),
            slot_created: UnsignedInteger(account.slot_created as u64),
            seq: account.seq.map(|seq| UnsignedInteger(seq as u64)),
            prove_by_index: account.in_output_queue,
            merkle_context: MerkleContextV2 {
                tree_type: account.tree_type.map(|t| t as u16).unwrap_or(0),
                tree: account.tree.try_into()?,
                queue: account.queue.unwrap_or_default().try_into()?,
                cpi_context: None,
                next_tree_context: None,
            },
        })
    }
}

impl From<&AccountWithContext> for AccountV2 {
    fn from(x: &AccountWithContext) -> Self {
        AccountV2 {
            hash: x.account.hash.clone(),
            address: x.account.address.clone(),
            data: x.account.data.clone(),
            owner: x.account.owner.clone(),
            lamports: x.account.lamports,
            leaf_index: x.account.leaf_index,
            seq: x.account.seq,
            slot_created: x.account.slot_created,
            prove_by_index: x.context.in_output_queue,
            merkle_context: MerkleContextV2 {
                tree_type: x.context.tree_type,
                tree: x.account.tree.clone(),
                queue: x.context.queue.clone(),
                cpi_context: None,
                next_tree_context: None,
            },
        }
    }
}
