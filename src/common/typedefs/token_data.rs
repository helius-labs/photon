

use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::serializable_pubkey::SerializablePubkey;

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    AnchorSerialize,
    AnchorDeserialize,
    TryFromPrimitive,
    Deserialize,
    Serialize,
)]
#[repr(u8)]
pub enum AccountState {
    Uninitialized,
    Initialized,
    Frozen,
}

impl Default for AccountState {
    fn default() -> Self {
        AccountState::Initialized
    }
}

impl AccountState {
    pub fn to_string(&self) -> String {
        match self {
            AccountState::Uninitialized => "uninitialized",
            AccountState::Initialized => "initialized",
            AccountState::Frozen => "frozen",
        }
        .to_string()
    }
}

#[derive(Debug, PartialEq, Eq, AnchorDeserialize, AnchorSerialize, Clone, ToSchema, Serialize, Default)]
pub struct TokenData {
    /// The mint associated with this account
    pub mint: SerializablePubkey,
    /// The owner of this account.
    pub owner: SerializablePubkey,
    /// The amount of tokens this account holds.
    pub amount: u64,
    /// If `delegate` is `Some` then `delegated_amount` represents
    /// the amount authorized by the delegate
    pub delegate: Option<SerializablePubkey>,
    /// The account's state
    pub state: AccountState,
    /// If is_some, this is a native token, and the value logs the rent-exempt
    /// reserve. An Account is required to be rent-exempt, so the value is
    /// used by the Processor to ensure that wrapped SOL accounts do not
    /// drop below this threshold.
    pub is_native: Option<u64>,
    /// The amount delegated
    pub delegated_amount: u64, // TODO: make instruction data optional
                               // TODO: validate that we don't need close authority
                               // /// Optional authority to close the account.
                               // pub close_authority: Option<Pubkey>,
}
