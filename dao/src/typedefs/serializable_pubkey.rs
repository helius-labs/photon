use core::fmt;
use std::str::FromStr;

use schemars::JsonSchema;
use serde::Deserialize;
use solana_sdk::pubkey::ParsePubkeyError;

use schemars::gen::SchemaGenerator;
use schemars::schema::Schema;
use serde::de::{self, Visitor};
use serde::ser::{Serialize, Serializer};
use serde::Deserializer;
use solana_sdk::pubkey::Pubkey as SolanaPubkey;

use std::convert::TryFrom;

#[derive(Default, Clone, PartialEq, Eq)]
pub struct SerializablePubkey(SolanaPubkey);

impl TryFrom<&str> for SerializablePubkey {
    type Error = ParsePubkeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(SerializablePubkey(SolanaPubkey::from_str(value)?))
    }
}

impl From<SolanaPubkey> for SerializablePubkey {
    fn from(pubkey: SolanaPubkey) -> Self {
        SerializablePubkey(pubkey)
    }
}

impl Into<Vec<u8>> for SerializablePubkey {
    fn into(self) -> Vec<u8> {
        self.0.to_bytes().to_vec()
    }
}

impl TryFrom<Vec<u8>> for SerializablePubkey {
    type Error = ParsePubkeyError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(SerializablePubkey(
            SolanaPubkey::try_from(bytes).map_err(|_| ParsePubkeyError::Invalid)?,
        ))
    }
}

impl Into<String> for SerializablePubkey {
    fn into(self) -> String {
        self.0.to_string()
    }
}

impl fmt::Debug for SerializablePubkey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SerializablePubkey({})", self.0.to_string())
    }
}

impl JsonSchema for SerializablePubkey {
    fn schema_name() -> String {
        "SerializablePubkey".to_string()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        let schema = gen.subschema_for::<String>();
        let mut schema_object = schema.into_object();
        schema_object.metadata().description = Some("A base58 encoded string".to_string());
        schema_object.into()
    }
}

struct Base58Visitor;

impl<'de> Visitor<'de> for Base58Visitor {
    type Value = SerializablePubkey;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a base58 encoded string")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(SerializablePubkey::try_from(value).map_err(|e| E::custom(e.to_string()))?)
    }
}

impl<'de> Deserialize<'de> for SerializablePubkey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Base58Visitor)
    }
}

impl Serialize for SerializablePubkey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let base58_string = bs58::encode(self.0).into_string();
        serializer.serialize_str(&base58_string)
    }
}

#[test]
fn test_serialization() {
    // Hacky way to get 32 bytes
    let hash = SerializablePubkey(SolanaPubkey::new_unique());
    let serialized = serde_json::to_string(&hash).unwrap();
    let deserialized: SerializablePubkey = serde_json::from_str(&serialized).unwrap();
    assert_eq!(hash, deserialized);
}
