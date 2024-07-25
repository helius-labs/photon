use super::utils::{GetLatestSignaturesRequest, GetPaginatedSignaturesResponse};
use sea_orm::DatabaseConnection;

use super::{
    super::error::PhotonApiError,
    utils::{search_for_signatures, Context, SignatureSearchType},
};

pub async fn get_latest_compression_signatures(
    conn: &DatabaseConnection,
    request: GetLatestSignaturesRequest,
) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let signatures = search_for_signatures(
        conn,
        SignatureSearchType::Standard,
        None,
        true,
        request.cursor,
        request.limit,
    )
    .await?;

    Ok(GetPaginatedSignaturesResponse {
        value: signatures.into(),
        context,
    })
}
