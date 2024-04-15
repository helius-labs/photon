use serde::{Deserialize, Serialize};
use solana_sdk::signature::Signature;
use utoipa::{
    openapi::{ObjectBuilder, RefOr, Schema, SchemaType},
    ToSchema,
};

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SerializableSignature(pub Signature);

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
