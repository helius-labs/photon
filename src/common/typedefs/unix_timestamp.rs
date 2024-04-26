use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Number;
use utoipa::{
    openapi::{ObjectBuilder, RefOr, Schema, SchemaType},
    ToSchema,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct UnixTimestamp(pub u64);

impl<'__s> ToSchema<'__s> for UnixTimestamp {
    fn schema() -> (&'__s str, RefOr<Schema>) {
        let default = Some(serde_json::Value::Number(
            Number::from_str("1714081554").unwrap(),
        ));
        let schema = Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Integer)
                .description(Some("An Unix timestamp (seconds)".to_string()))
                .default(default.clone())
                .example(default)
                .build(),
        );
        ("UnixTimestamp", RefOr::T(schema))
    }

    fn aliases() -> Vec<(&'static str, utoipa::openapi::schema::Schema)> {
        Vec::new()
    }
}
