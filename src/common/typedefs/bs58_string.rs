use serde::{Deserialize, Serialize};
use utoipa::{
    openapi::{ObjectBuilder, RefOr, Schema, SchemaType},
    ToSchema,
};

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Base58String(pub String);

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
