use super::utils::{
    GetLatestSignaturesRequest, GetNonPaginatedSignaturesResponseWithError,
    SignatureInfoListWithError,
};
use sea_orm::DatabaseConnection;

use super::{
    super::error::PhotonApiError,
    utils::{search_for_signatures, Context, SignatureSearchType},
};

pub async fn get_latest_non_voting_signatures(
    conn: &DatabaseConnection,
    request: GetLatestSignaturesRequest,
) -> Result<GetNonPaginatedSignaturesResponseWithError, PhotonApiError> {
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

    Ok(GetNonPaginatedSignaturesResponseWithError {
        value: SignatureInfoListWithError {
            items: signatures.items,
        },
        context,
    })
}
