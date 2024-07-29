use bs58;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use utoipa::{
    openapi::{ObjectBuilder, RefOr, Schema, SchemaType},
    ToSchema,
};

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Base58String(pub Vec<u8>);

impl Serialize for Base58String {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let base58_str = bs58::encode(&self.0).into_string();
        serializer.serialize_str(&base58_str)
    }
}

impl<'de> Deserialize<'de> for Base58String {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        let bytes = bs58::decode(&s)
            .into_vec()
            .map_err(serde::de::Error::custom)?;
        Ok(Base58String(bytes))
    }
}

impl<'__s> ToSchema<'__s> for Base58String {
    fn schema() -> (&'__s str, RefOr<Schema>) {
        let example = Some(serde_json::Value::String("3J98t1WpEZ73CNm".to_string()));
        let schema = Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::String)
                .description(Some("A base 58 encoded string."))
                .example(example.clone())
                .default(example)
                .build(),
        );

        ("Base58String", RefOr::T(schema))
    }

    fn aliases() -> Vec<(&'static str, utoipa::openapi::schema::Schema)> {
        Vec::new()
    }
}
