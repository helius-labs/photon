use serde::Serialize;
use utoipa::{
    openapi::{ObjectBuilder, RefOr, Schema, SchemaType},
    ToSchema,
};

use serde::Serializer;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct Base64String(pub Vec<u8>);

impl Serialize for Base64String {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[allow(deprecated)]
        let base64_encoded = base64::encode(&self.0);
        serializer.serialize_str(&base64_encoded)
    }
}

impl<'__s> ToSchema<'__s> for Base64String {
    fn schema() -> (&'__s str, RefOr<Schema>) {
        let example = Some(serde_json::Value::String(
            "SGVsbG8sIFdvcmxkIQ==".to_string(),
        ));
        let schema = Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::String)
                .description(Some("A base 64 encoded string."))
                .example(example.clone())
                .default(example)
                .build(),
        );

        ("Base64String", RefOr::T(schema))
    }

    fn aliases() -> Vec<(&'static str, utoipa::openapi::schema::Schema)> {
        Vec::new()
    }
}
