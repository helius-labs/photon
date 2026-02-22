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
    /// TokenExtension TLV data (raw bytes, opaque to the indexer)
    pub tlv: Option<Base64String>,
}

impl TokenData {
    /// Deserializes base fields via Borsh, then reads TLV as raw bytes (1-byte
    /// option tag + remaining bytes).
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

            match option_tag {
                0 => None,
                1 if !buf.is_empty() => Some(Base64String(buf.to_vec())),
                other => {
                    log::warn!(
                        "Unexpected TLV: option_tag={}, remaining_bytes={}",
                        other,
                        buf.len()
                    );
                    None
                }
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

    fn build_onchain_token_data_bytes(has_delegate: bool, tlv: Option<&[u8]>) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[0x11u8; 32]); // mint
        bytes.extend_from_slice(&[0x22u8; 32]); // owner
        bytes.extend_from_slice(&100u64.to_le_bytes()); // amount
        if has_delegate {
            bytes.push(1);
            bytes.extend_from_slice(&[0x33u8; 32]);
        } else {
            bytes.push(0);
        }
        bytes.push(0); // state = initialized
        match tlv {
            Some(tlv_data) => {
                bytes.push(1); // Option tag = Some
                bytes.extend_from_slice(tlv_data);
            }
            None => {
                bytes.push(0); // Option tag = None
            }
        }
        bytes
    }

    fn build_compressed_only_tlv() -> Vec<u8> {
        let mut tlv = Vec::new();
        tlv.extend_from_slice(&1u32.to_le_bytes()); // vec len = 1 element
        tlv.push(31); // ExtensionStruct enum variant = CompressedOnly
        tlv.extend_from_slice(&500u64.to_le_bytes()); // delegated_amount
        tlv.extend_from_slice(&0u64.to_le_bytes()); // withheld_transfer_fee
        tlv.push(1); // is_ata
        tlv
    }

    #[test]
    fn parse_v1_no_delegate_no_tlv() {
        let bytes = build_onchain_token_data_bytes(false, None);
        let td = TokenData::parse(&bytes).unwrap();
        assert_eq!(td.mint.0, Pubkey::from([0x11; 32]));
        assert_eq!(td.owner.0, Pubkey::from([0x22; 32]));
        assert_eq!(td.amount.0, 100);
        assert!(td.delegate.is_none());
        assert_eq!(td.state, AccountState::initialized);
        assert!(td.tlv.is_none());
    }

    #[test]
    fn parse_v1_with_delegate_no_tlv() {
        let bytes = build_onchain_token_data_bytes(true, None);
        let td = TokenData::parse(&bytes).unwrap();
        assert_eq!(td.delegate.unwrap().0, Pubkey::from([0x33; 32]));
        assert!(td.tlv.is_none());
    }

    #[test]
    fn parse_v3_with_compressed_only_tlv() {
        let tlv_raw = build_compressed_only_tlv();
        let bytes = build_onchain_token_data_bytes(true, Some(&tlv_raw));

        let td = TokenData::parse(&bytes).unwrap();
        assert_eq!(td.amount.0, 100);
        assert!(td.delegate.is_some());
        let tlv = td.tlv.unwrap();
        assert_eq!(tlv.0, tlv_raw);
    }

    #[test]
    fn parse_v3_no_delegate_with_tlv() {
        let tlv_raw = build_compressed_only_tlv();
        let bytes = build_onchain_token_data_bytes(false, Some(&tlv_raw));

        let td = TokenData::parse(&bytes).unwrap();
        assert!(td.delegate.is_none());
        assert!(td.tlv.is_some());
        assert_eq!(td.tlv.unwrap().0, tlv_raw);
    }

    #[derive(Debug, BorshDeserialize)]
    struct OldTokenData {
        pub mint: SerializablePubkey,
        pub owner: SerializablePubkey,
        pub amount: UnsignedInteger,
        pub delegate: Option<SerializablePubkey>,
        pub state: AccountState,
        pub tlv: Option<Base64String>,
    }

    #[test]
    fn old_try_from_slice_fails_with_not_all_bytes_read() {
        let tlv_raw = build_compressed_only_tlv();
        let bytes = build_onchain_token_data_bytes(true, Some(&tlv_raw));

        let err = OldTokenData::try_from_slice(&bytes)
            .expect_err("old BorshDeserialize must fail on V3 TLV data");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("Not all bytes read"),
            "Expected exact production error 'Not all bytes read', got: {}",
            err
        );
    }

    #[test]
    fn new_parse_succeeds_where_old_try_from_slice_fails() {
        let tlv_raw = build_compressed_only_tlv();
        let bytes = build_onchain_token_data_bytes(true, Some(&tlv_raw));

        // Old code fails:
        assert!(OldTokenData::try_from_slice(&bytes).is_err());
        // New code succeeds and captures TLV:
        let td = TokenData::parse(&bytes).unwrap();
        assert_eq!(td.tlv.unwrap().0, tlv_raw);
    }

    #[test]
    fn roundtrip_no_tlv() {
        let original = TokenData {
            mint: SerializablePubkey(Pubkey::new_unique()),
            owner: SerializablePubkey(Pubkey::new_unique()),
            amount: UnsignedInteger(42),
            delegate: None,
            state: AccountState::initialized,
            tlv: None,
        };
        let mut bytes = Vec::new();
        borsh::BorshSerialize::serialize(&original, &mut bytes).unwrap();
        let parsed = TokenData::parse(&bytes).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn roundtrip_with_tlv() {
        let original = TokenData {
            mint: SerializablePubkey(Pubkey::new_unique()),
            owner: SerializablePubkey(Pubkey::new_unique()),
            amount: UnsignedInteger(1_000_000),
            delegate: Some(SerializablePubkey(Pubkey::new_unique())),
            state: AccountState::frozen,
            tlv: Some(Base64String(vec![0xAA, 0xBB, 0xCC])),
        };
        let mut bytes = Vec::new();
        borsh::BorshSerialize::serialize(&original, &mut bytes).unwrap();
        let parsed = TokenData::parse(&bytes).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn byte_layout_no_delegate_no_tlv() {
        let bytes = build_onchain_token_data_bytes(false, None);
        // mint(32) + owner(32) + amount(8) + delegate_none(1) + state(1) + tlv_none(1) = 75
        assert_eq!(bytes.len(), 75);
        let td = TokenData::parse(&bytes).unwrap();
        assert!(td.tlv.is_none());
    }

    #[test]
    fn byte_layout_with_delegate_no_tlv() {
        let bytes = build_onchain_token_data_bytes(true, None);
        // mint(32) + owner(32) + amount(8) + delegate_some(1+32) + state(1) + tlv_none(1) = 107
        assert_eq!(bytes.len(), 107);
    }

    #[test]
    fn byte_layout_with_delegate_and_tlv() {
        let tlv_raw = build_compressed_only_tlv();
        // tlv_raw: u32(4) + discriminant(1) + u64(8) + u64(8) + u8(1) = 22
        assert_eq!(tlv_raw.len(), 22);
        let bytes = build_onchain_token_data_bytes(true, Some(&tlv_raw));
        // 107 (with delegate, no tlv excl the None byte) - 1 (remove None byte)
        // + 1 (Some tag) + 22 (tlv_raw) = 129
        assert_eq!(bytes.len(), 129);
    }
}
