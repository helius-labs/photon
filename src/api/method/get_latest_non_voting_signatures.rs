use super::utils::{GetLatestSignaturesRequest, GetNonPaginatedSignaturesResponse};
use sea_orm::DatabaseConnection;

use super::{
    super::error::PhotonApiError,
    utils::{search_for_signatures, Context, SignatureSearchType},
};

pub async fn get_latest_non_voting_signatures(
    conn: &DatabaseConnection,
    request: GetLatestSignaturesRequest,
) -> Result<GetNonPaginatedSignaturesResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let signatures = search_for_signatures(
        conn,
        SignatureSearchType::Standard,
        None,
        false,
        request.cursor,
        request.limit,
    )
    .await?;

    Ok(GetNonPaginatedSignaturesResponse {
        value: super::utils::SignatureInfoList {
            items: signatures.items,
        },
        context,
    })
}
