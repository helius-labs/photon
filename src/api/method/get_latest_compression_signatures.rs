use super::utils::GetPaginatedSignaturesResponse;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    utils::{search_for_signatures, Context, Limit, SignatureSearchType},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct GetLatestCompressionSignaturesRequest {
    pub limit: Option<Limit>,
    pub cursor: Option<String>,
}

pub async fn get_latest_compression_signatures(
    conn: &DatabaseConnection,
    request: GetLatestCompressionSignaturesRequest,
) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let signatures = search_for_signatures(
        conn,
        SignatureSearchType::Standard,
        None,
        request.cursor,
        request.limit,
    )
    .await?;

    Ok(GetPaginatedSignaturesResponse {
        value: signatures,
        context,
    })
}
