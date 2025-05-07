use super::utils::{GetLatestSignaturesRequest, GetPaginatedSignaturesResponse};
use super::{
    super::error::PhotonApiError,
    utils::{search_for_signatures, SignatureSearchType},
};
use crate::common::typedefs::context::Context;
use sea_orm::DatabaseConnection;

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
