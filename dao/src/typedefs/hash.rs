use core::fmt;
use std::mem;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use schemars::gen::SchemaGenerator;
use schemars::schema::Schema;
use serde::de::{self, Visitor};
use serde::ser::Serializer;
use serde::Deserializer;
use thiserror::Error;

/// Maximum string length of a base58 encoded pubkey
const MAX_BASE58_LEN: usize = 44;

#[derive(Default, Clone, PartialEq, Eq)]
pub struct Hash(pub(crate) [u8; 32]);

impl Hash {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

#[derive(Error, Debug, Serialize, Clone, PartialEq, Eq)]
pub enum ParseHashError {
    #[error("String is the wrong size")]
    WrongSize,
    #[error("Invalid Base58 string")]
    Invalid,
}

impl TryFrom<&str> for Hash {
    type Error = ParseHashError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.len() > MAX_BASE58_LEN {
            return Err(ParseHashError::WrongSize);
        }
        let bytes = bs58::decode(s)
            .into_vec()
            .map_err(|_| ParseHashError::Invalid)?;

        if bytes.len() != mem::size_of::<Hash>() {
            Err(ParseHashError::WrongSize)
        } else {
            let bytes: [u8; 32] = bytes.try_into().map_err(|_| ParseHashError::Invalid)?;
            Ok(Hash(bytes))
        }
    }
}

impl Into<Vec<u8>> for Hash {
    fn into(self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl From<Vec<u8>> for Hash {
    fn from(bytes: Vec<u8>) -> Self {
        // Generally we don't want to use unwrap, but in this case we know the bytes are valid
        // because they are either coming from Solana or from our database.
        let bytes: [u8; 32] = bytes.try_into().expect("Unable to deserialize hash");
        Hash(bytes)
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: Reduce code duplication here
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

impl Into<String> for Hash {
    fn into(self) -> String {
        bs58::encode(self.0).into_string()
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Reduce code duplication here
        write!(f, "Hash({})", bs58::encode(self.0).into_string())
    }
}

impl JsonSchema for Hash {
    fn schema_name() -> String {
        "Hash".to_string()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        let schema = gen.subschema_for::<String>();
        let mut schema_object = schema.into_object();
        schema_object.metadata().description = Some("A base58 encoded string".to_string());
        schema_object.into()
    }
}

struct Base58VisitorHash;

impl<'de> Visitor<'de> for Base58VisitorHash {
    type Value = Hash;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a base58 encoded string")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(Hash::try_from(value).map_err(|e| E::custom(e.to_string()))?)
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Base58VisitorHash)
    }
}

impl Serialize for Hash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let base58_string = bs58::encode(self.0).into_string();
        serializer.serialize_str(&base58_string)
    }
}
