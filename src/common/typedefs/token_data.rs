use borsh::{BorshDeserialize, BorshSerialize};
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
    BorshSerialize,
    BorshDeserialize,
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

#[derive(Debug, PartialEq, Eq, BorshSerialize, Clone, ToSchema, Serialize, Default)]
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
    /// TokenExtension TLV data
    pub tlv: Option<Base64String>,
}

impl TokenData {
    pub fn parse(data: &[u8]) -> Result<Self, std::io::Error> {
        let mut buf = data;

        let mint = SerializablePubkey::deserialize(&mut buf)?;
        let owner = SerializablePubkey::deserialize(&mut buf)?;
        let amount = UnsignedInteger::deserialize(&mut buf)?;
        let delegate = Option::<SerializablePubkey>::deserialize(&mut buf)?;
        let state = AccountState::deserialize(&mut buf)?;

        let tlv = if buf.is_empty() {
            None
        } else {
            let option_tag = buf[0];
            buf = &buf[1..];

            if option_tag == 0 {
                None
            } else if option_tag == 1 && !buf.is_empty() {
                Some(Base64String(buf.to_vec()))
            } else {
                None
            }
        };

        Ok(TokenData {
            mint,
            owner,
            amount,
            delegate,
            state,
            tlv,
        })
    }
}
