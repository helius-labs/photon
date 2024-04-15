use std::str::FromStr;

use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use solana_sdk::signature::Signature;
use utoipa::{
    openapi::{ObjectBuilder, RefOr, Schema, SchemaType},
    ToSchema,
};

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct SerializableSignature(pub Signature);

struct Base58Visitor;

impl<'de> Visitor<'de> for Base58Visitor {
    type Value = SerializableSignature;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a base58 encoded string")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Signature::from_str(value)
            .map_err(|e| E::custom(e.to_string()))
            .map(SerializableSignature)
    }
}

impl<'de> Deserialize<'de> for SerializableSignature {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Base58Visitor)
    }
}

impl Serialize for SerializableSignature {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let base58_string = bs58::encode(self.0).into_string();
        serializer.serialize_str(&base58_string)
    }
}

impl<'__s> ToSchema<'__s> for SerializableSignature {
    fn schema() -> (&'__s str, RefOr<Schema>) {
        let example = Some(serde_json::Value::String(
            "5J8H5sTvEhnGcB4R8K1n7mfoiWUD9RzPVGES7e3WxC7c".to_string(),
        ));
        let schema = Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::String)
                .description(Some("A Solana transaction signature."))
                .example(example.clone())
                .default(example)
                .build(),
        );

        ("SerializableSignature", RefOr::T(schema))
    }

    fn aliases() -> Vec<(&'static str, utoipa::openapi::schema::Schema)> {
        Vec::new()
    }
}
