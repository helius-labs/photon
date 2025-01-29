use jsonrpsee_core::Serialize;
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use utoipa::ToSchema;
use crate::api::error::PhotonApiError;
use crate::api::method::utils::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsRequest {
    pub queue: Hash,
    pub start_offset: UnsignedInteger,
    pub end_offset: UnsignedInteger,
    pub batch: UnsignedInteger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsResponse {
    pub context: Context,
    pub value: Vec<Hash>,
}

pub async fn get_queue_elements(
    conn: &DatabaseConnection,
    _request: GetQueueElementsRequest,
) -> Result<GetQueueElementsResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let response = GetQueueElementsResponse {
        context,
        value: vec![Hash::new_unique()]
    };

    Ok(response)
}
