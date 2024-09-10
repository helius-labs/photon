use super::utils::{
    GetLatestSignaturesRequest, GetNonPaginatedSignaturesResponseWithError, Limit,
    SignatureInfoListWithError,
};
use sea_orm::DatabaseConnection;

use super::{
    super::error::PhotonApiError,
    utils::{search_for_signatures, Context, SignatureSearchType},
};

pub const MAX_LATEST_NON_VOTING_SIGNATURES: u64 = 100;

pub async fn get_latest_non_voting_signatures(
    conn: &DatabaseConnection,
    request: GetLatestSignaturesRequest,
) -> Result<GetNonPaginatedSignaturesResponseWithError, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let limit = match request.limit {
        Some(limit) => {
            if limit.value() > MAX_LATEST_NON_VOTING_SIGNATURES {
                return Err(PhotonApiError::ValidationError(
                    // Increase the limit to the max
                    format!(
                        "Limit is too large. Max limit is {}",
                        MAX_LATEST_NON_VOTING_SIGNATURES
                    ),
                ));
            }
            limit
        }
        None => Limit::new(MAX_LATEST_NON_VOTING_SIGNATURES).unwrap(),
    };

    let signatures = search_for_signatures(
        conn,
        SignatureSearchType::Standard,
        None,
        false,
        request.cursor,
        Some(limit),
    )
    .await?;

    Ok(GetNonPaginatedSignaturesResponseWithError {
        value: SignatureInfoListWithError {
            items: signatures.items,
        },
        context,
    })
}
