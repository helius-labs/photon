use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    utils::{
        search_for_signatures, Context, HashRequest, SignatureFilter, SignatureInfoList,
        SignatureSearchType,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// We do not use generics to simplify documentation generation.
pub struct GetCompressionSignaturesForAccountResponse {
    pub context: Context,
    pub value: SignatureInfoList,
}

pub async fn get_compression_signatures_for_account(
    conn: &DatabaseConnection,
    request: HashRequest,
) -> Result<GetCompressionSignaturesForAccountResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let hash = request.hash;

    let signatures = search_for_signatures(
        conn,
        SignatureSearchType::Standard,
        Some(SignatureFilter::Account(hash)),
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

    Ok(GetCompressionSignaturesForAccountResponse {
        value: SignatureInfoList { items: signatures },
        context,
    })
}
