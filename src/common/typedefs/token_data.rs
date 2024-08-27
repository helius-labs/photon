use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use num_enum::TryFromPrimitive;
use serde::Serialize;
use utoipa::ToSchema;

use super::{
    bs64_string::Base64String, serializable_pubkey::SerializablePubkey,
    unsigned_integer::UnsignedInteger,
};

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    AnchorSerialize,
    AnchorDeserialize,
    TryFromPrimitive,
    ToSchema,
    Serialize,
)]
#[repr(u8)]
#[derive(Default)]
pub enum AccountState {
    #[allow(non_camel_case_types)]
    #[default]
    initialized,
    #[allow(non_camel_case_types)]
    frozen,
}

#[derive(
    Debug, PartialEq, Eq, AnchorDeserialize, AnchorSerialize, Clone, ToSchema, Serialize, Default,
)]
#[serde(rename_all = "camelCase")]
pub struct TokenData {
    /// The mint associated with this account
    pub mint: SerializablePubkey,
    /// The owner of this account.
    pub owner: SerializablePubkey,
    /// The amount of tokens this account holds.
    pub amount: UnsignedInteger,
    /// If `delegate` is `Some` then `delegated_amount` represents
    /// the amount authorized by the delegate
    pub delegate: Option<SerializablePubkey>,
    /// The account's state
    pub state: AccountState,
    /// Placeholder for TokenExtension tlv data (unimplemented)
    pub tlv: Option<Base64String>,
}
