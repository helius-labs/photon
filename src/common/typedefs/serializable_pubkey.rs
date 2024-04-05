use core::fmt;
use std::str::FromStr;

use serde::Deserialize;
use solana_sdk::pubkey::ParsePubkeyError;

use serde::de::{self, Visitor};
use serde::ser::{Serialize, Serializer};
use serde::Deserializer;
use solana_sdk::pubkey::Pubkey as SolanaPubkey;
use utoipa::openapi::{schema::Schema, RefOr};
use utoipa::openapi::{ObjectBuilder, SchemaType};
use utoipa::ToSchema;

use std::convert::TryFrom;

pub type SerializablePubkey = Pubkey;

#[derive(Default, Clone, PartialEq, Eq)]
/// A Solana public key.
pub struct Pubkey(SolanaPubkey);

impl<'__s> ToSchema<'__s> for Pubkey {
    fn schema() -> (&'__s str, RefOr<Schema>) {
        let schema = Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::String)
                .description(Some("A Solana public key represented as a base58 string."))
                .example(Some(serde_json::Value::String(
                    Pubkey(SolanaPubkey::new_unique()).to_string(),
                )))
                .build(),
        );

        ("Pubkey", RefOr::T(schema))
    }

    fn aliases() -> Vec<(&'static str, utoipa::openapi::schema::Schema)> {
        Vec::new()
    }
}

impl TryFrom<&str> for Pubkey {
    type Error = ParsePubkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Pubkey(SolanaPubkey::from_str(value)?))
    }
}

impl fmt::Display for Pubkey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<SolanaPubkey> for Pubkey {
    fn from(pubkey: SolanaPubkey) -> Self {
        Pubkey(pubkey)
    }
}

impl From<Pubkey> for Vec<u8> {
    fn from(val: Pubkey) -> Self {
        val.0.to_bytes().to_vec()
    }
}

impl From<[u8; 32]> for Pubkey {
    fn from(bytes: [u8; 32]) -> Self {
        Pubkey(SolanaPubkey::from(bytes))
    }
}

impl TryFrom<Vec<u8>> for Pubkey {
    type Error = ParsePubkeyError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Pubkey(
            SolanaPubkey::try_from(bytes).map_err(|_| ParsePubkeyError::Invalid)?,
        ))
    }
}

impl From<Pubkey> for String {
    fn from(val: Pubkey) -> Self {
        val.0.to_string()
    }
}

impl fmt::Debug for Pubkey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Pubkey({})", self.0)
    }
}

struct Base58Visitor;

impl<'de> Visitor<'de> for Base58Visitor {
    type Value = Pubkey;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a base58 encoded string")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Pubkey::try_from(value).map_err(|e| E::custom(e.to_string()))
    }
}

impl<'de> Deserialize<'de> for Pubkey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Base58Visitor)
    }
}

impl Serialize for Pubkey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let base58_string = bs58::encode(self.0).into_string();
        serializer.serialize_str(&base58_string)
    }
}

#[test]
fn test_serialization() {
    // Hacky way to get 32 bytes
    let hash = Pubkey(SolanaPubkey::new_unique());
    let serialized = serde_json::to_string(&hash).unwrap();
    let deserialized: Pubkey = serde_json::from_str(&serialized).unwrap();
    assert_eq!(hash, deserialized);
}
