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

#[derive(Debug, PartialEq, Eq, Clone, ToSchema, Serialize, Default)]
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
                if option_tag != 0 && option_tag != 1 {
                    log::warn!(
                        "Unknown TLV option_tag={} with {} bytes remaining",
                        option_tag,
                        buf.len()
                    );
                }
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

impl BorshSerialize for TokenData {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
        borsh::BorshSerialize::serialize(&self.mint, writer)?;
        borsh::BorshSerialize::serialize(&self.owner, writer)?;
        borsh::BorshSerialize::serialize(&self.amount, writer)?;
        borsh::BorshSerialize::serialize(&self.delegate, writer)?;
        borsh::BorshSerialize::serialize(&self.state, writer)?;

        match &self.tlv {
            None => writer.write_all(&[0]),
            Some(tlv_bytes) => {
                writer.write_all(&[1])?;
                writer.write_all(&tlv_bytes.0)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_pubkey::Pubkey;

    fn sample_token_data(tlv: Option<Vec<u8>>) -> TokenData {
        TokenData {
            mint: SerializablePubkey::from(Pubkey::new_unique()),
            owner: SerializablePubkey::from(Pubkey::new_unique()),
            amount: UnsignedInteger(42),
            delegate: Some(SerializablePubkey::from(Pubkey::new_unique())),
            state: AccountState::initialized,
            tlv: tlv.map(Base64String),
        }
    }

    #[test]
    fn serialize_parse_roundtrip_without_tlv() {
        let token_data = sample_token_data(None);

        let mut bytes = Vec::new();
        borsh::BorshSerialize::serialize(&token_data, &mut bytes).unwrap();

        let parsed = TokenData::parse(&bytes).unwrap();
        assert_eq!(parsed, token_data);
    }

    #[test]
    fn serialize_parse_roundtrip_with_tlv_raw_bytes() {
        // Starts with a vec-length-like prefix to ensure serializer doesn't add another one.
        let raw_tlv = vec![2, 0, 0, 0, 7, 8, 9, 10];
        let token_data = sample_token_data(Some(raw_tlv.clone()));

        let mut bytes = Vec::new();
        borsh::BorshSerialize::serialize(&token_data, &mut bytes).unwrap();

        // Fixed fields: mint(32) + owner(32) + amount(8) + delegate_option(1+32) + state(1)
        let fixed_len = 32 + 32 + 8 + 33 + 1;
        assert_eq!(bytes[fixed_len], 1); // tlv option tag
        assert_eq!(&bytes[(fixed_len + 1)..], raw_tlv.as_slice());

        let parsed = TokenData::parse(&bytes).unwrap();
        assert_eq!(parsed, token_data);
    }

    #[test]
    fn serialize_parse_roundtrip_without_delegate() {
        let token_data = TokenData {
            mint: SerializablePubkey::from(Pubkey::new_unique()),
            owner: SerializablePubkey::from(Pubkey::new_unique()),
            amount: UnsignedInteger(123456789),
            delegate: None,
            state: AccountState::frozen,
            tlv: None,
        };

        let mut bytes = Vec::new();
        borsh::BorshSerialize::serialize(&token_data, &mut bytes).unwrap();

        // Fixed fields: mint(32) + owner(32) + amount(8) + delegate_option(1) + state(1) + tlv_option(1) = 75
        assert_eq!(bytes.len(), 75);

        let parsed = TokenData::parse(&bytes).unwrap();
        assert_eq!(parsed, token_data);
    }

    #[test]
    fn serialize_tlv_option_none_explicit() {
        let token_data = TokenData {
            mint: SerializablePubkey::from([1u8; 32]),
            owner: SerializablePubkey::from([2u8; 32]),
            amount: UnsignedInteger(100),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };

        let mut bytes = Vec::new();
        borsh::BorshSerialize::serialize(&token_data, &mut bytes).unwrap();

        // TLV should serialize as a single 0 byte at the end
        assert_eq!(bytes.len(), 75); // 32+32+8+1+1+1 = 75
        assert_eq!(*bytes.last().unwrap(), 0); // Last byte should be 0 (None option)

        let parsed = TokenData::parse(&bytes).unwrap();
        assert_eq!(parsed.tlv, None);
    }

    #[test]
    fn serialize_empty_tlv_vec() {
        let token_data = TokenData {
            mint: SerializablePubkey::from([3u8; 32]),
            owner: SerializablePubkey::from([4u8; 32]),
            amount: UnsignedInteger(999),
            delegate: Some(SerializablePubkey::from([5u8; 32])),
            state: AccountState::frozen,
            tlv: Some(Base64String(vec![])),
        };

        let mut bytes = Vec::new();
        borsh::BorshSerialize::serialize(&token_data, &mut bytes).unwrap();

        // Fixed + tlv option tag (1)
        let fixed_len = 32 + 32 + 8 + 33 + 1;
        assert_eq!(bytes[fixed_len], 1); // Option::Some tag
        assert_eq!(bytes.len(), fixed_len + 1); // No data after the tag

        // NOTE: parse() treats empty TLV (option=1, no data) as None per line 69
        let parsed = TokenData::parse(&bytes).unwrap();
        assert_eq!(parsed.tlv, None); // parse behavior: empty TLV -> None
    }

    #[test]
    fn byte_layout_consistency() {
        // Verify exact byte layout matches what parse() expects
        let mint = [11u8; 32];
        let owner = [22u8; 32];
        let amount = 42u64;
        let delegate = [33u8; 32];
        let state = 1u8; // frozen
        let tlv_data = vec![0xAA, 0xBB, 0xCC];

        let token_data = TokenData {
            mint: SerializablePubkey::from(mint),
            owner: SerializablePubkey::from(owner),
            amount: UnsignedInteger(amount),
            delegate: Some(SerializablePubkey::from(delegate)),
            state: AccountState::frozen,
            tlv: Some(Base64String(tlv_data.clone())),
        };

        let mut bytes = Vec::new();
        borsh::BorshSerialize::serialize(&token_data, &mut bytes).unwrap();

        // Verify manual layout
        assert_eq!(&bytes[0..32], &mint);
        assert_eq!(&bytes[32..64], &owner);
        assert_eq!(&bytes[64..72], &amount.to_le_bytes());
        assert_eq!(bytes[72], 1); // delegate option tag
        assert_eq!(&bytes[73..105], &delegate);
        assert_eq!(bytes[105], state);
        assert_eq!(bytes[106], 1); // tlv option tag
        assert_eq!(&bytes[107..], &tlv_data[..]);
    }

    #[test]
    fn compatibility_with_light_token_format() {
        // This test ensures our serialization matches the light program's expected format
        // based on the TokenData struct from light-token-interface
        let token_data = TokenData {
            mint: SerializablePubkey::from([0x10; 32]),
            owner: SerializablePubkey::from([0x20; 32]),
            amount: UnsignedInteger(1_000_000),
            delegate: Some(SerializablePubkey::from([0x30; 32])),
            state: AccountState::initialized,
            tlv: None,
        };

        let mut bytes = Vec::new();
        borsh::BorshSerialize::serialize(&token_data, &mut bytes).unwrap();

        // Verify the format:
        // 32 bytes: mint pubkey
        // 32 bytes: owner pubkey
        // 8 bytes: amount (u64 LE)
        // 1 byte: delegate option (1 = Some)
        // 32 bytes: delegate pubkey
        // 1 byte: state (0 = initialized)
        // 1 byte: tlv option (0 = None)
        assert_eq!(bytes.len(), 107);

        let parsed = TokenData::parse(&bytes).unwrap();
        assert_eq!(parsed, token_data);
    }

    #[test]
    fn various_amounts() {
        for amount in [0u64, 1, 1000, u64::MAX] {
            let token_data = TokenData {
                mint: SerializablePubkey::from(Pubkey::new_unique()),
                owner: SerializablePubkey::from(Pubkey::new_unique()),
                amount: UnsignedInteger(amount),
                delegate: None,
                state: AccountState::initialized,
                tlv: None,
            };

            let mut bytes = Vec::new();
            borsh::BorshSerialize::serialize(&token_data, &mut bytes).unwrap();
            let parsed = TokenData::parse(&bytes).unwrap();

            assert_eq!(parsed.amount.0, amount);
        }
    }

    #[test]
    fn various_tlv_sizes() {
        for tlv_size in [0, 1, 10, 100, 1000] {
            let tlv_data = vec![0x42u8; tlv_size];
            let token_data = TokenData {
                mint: SerializablePubkey::from(Pubkey::new_unique()),
                owner: SerializablePubkey::from(Pubkey::new_unique()),
                amount: UnsignedInteger(100),
                delegate: None,
                state: AccountState::initialized,
                tlv: if tlv_size == 0 {
                    None
                } else {
                    Some(Base64String(tlv_data.clone()))
                },
            };

            let mut bytes = Vec::new();
            borsh::BorshSerialize::serialize(&token_data, &mut bytes).unwrap();
            let parsed = TokenData::parse(&bytes).unwrap();

            assert_eq!(parsed, token_data);
        }
    }
}
