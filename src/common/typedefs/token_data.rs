use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use num_enum::TryFromPrimitive;
use serde::Serialize;
use utoipa::ToSchema;

use super::{serializable_pubkey::SerializablePubkey, unsigned_integer::UnsignedInteger};

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
    /// If is_some, this is a native token, and the value logs the rent-exempt
    /// reserve. An Account is required to be rent-exempt, so the value is
    /// used by the Processor to ensure that wrapped SOL accounts do not
    /// drop below this threshold.
    pub is_native: Option<UnsignedInteger>,
    /// The amount delegated
    pub delegated_amount: UnsignedInteger, // TODO: make instruction data optional
                                           // TODO: validate that we don't need close authority
                                           // /// Optional authority to close the account.
                                           // pub close_authority: Option<Pubkey>,
}
