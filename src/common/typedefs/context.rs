use crate::api::error::PhotonApiError;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::blocks;
use crate::migration::Expr;
use jsonrpsee_core::Serialize;
use sea_orm::{DatabaseConnection, EntityTrait, FromQueryResult, QuerySelect};
use serde::Deserialize;
use utoipa::openapi::{ObjectBuilder, RefOr, Schema, SchemaType};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromQueryResult, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Context {
    pub slot: u64,
}

impl<'__s> ToSchema<'__s> for Context {
    fn schema() -> (&'__s str, RefOr<Schema>) {
        let schema = Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Object)
                .property("slot", UnsignedInteger::schema().1)
                .required("slot")
                .build(),
        );
        ("Context", RefOr::T(schema))
    }

    fn aliases() -> Vec<(&'static str, Schema)> {
        Vec::new()
    }
}

#[derive(FromQueryResult)]
pub struct ContextModel {
    // Postgres and SQLlite do not support u64 as return type. We need to use i64 and cast it to u64.
    pub slot: i64,
}

impl Context {
    pub async fn extract(db: &DatabaseConnection) -> Result<Self, PhotonApiError> {
        let context = blocks::Entity::find()
            .select_only()
            .column_as(Expr::col(blocks::Column::Slot).max(), "slot")
            .into_model::<ContextModel>()
            .one(db)
            .await?
            .ok_or(PhotonApiError::RecordNotFound(
                "No data has been indexed".to_string(),
            ))?;
        Ok(Context {
            slot: context.slot as u64,
        })
    }
}
