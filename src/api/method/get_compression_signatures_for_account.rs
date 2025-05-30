use super::{
    super::error::PhotonApiError,
    utils::{
        search_for_signatures, GetNonPaginatedSignaturesResponse, HashRequest, SignatureFilter,
        SignatureInfoList, SignatureSearchType,
    },
};
use crate::common::typedefs::context::Context;
use sea_orm::DatabaseConnection;

pub async fn get_compression_signatures_for_account(
    conn: &DatabaseConnection,
    request: HashRequest,
) -> Result<GetNonPaginatedSignaturesResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let hash = request.hash;

    let signatures = search_for_signatures(
        conn,
        SignatureSearchType::Standard,
        Some(SignatureFilter::Account(hash)),
        true,
        None,
        None,
    )
    .await?
    .items;

    if signatures.len() > 2 {
        return Err(PhotonApiError::UnexpectedError(
            "Got too many transactions. This is a bug. An account can be modified at most twice."
                .to_string(),
        ));
    }

    if signatures.is_empty() {
        return Err(PhotonApiError::RecordNotFound(
            "Account not found".to_string(),
        ));
    }

    Ok(GetNonPaginatedSignaturesResponse {
        value: SignatureInfoList {
            items: signatures.into_iter().map(|s| s.into()).collect(),
        },
        context,
    })
}
