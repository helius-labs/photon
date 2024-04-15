use sea_orm::{DatabaseConnection};
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
pub struct GetSignaturesForCompressedAccountResponse {
    pub context: Context,
    pub value: SignatureInfoList,
}

pub async fn get_signatures_for_compressed_account(
    conn: &DatabaseConnection,
    request: HashRequest,
) -> Result<GetSignaturesForCompressedAccountResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let hash = request.0;

    let signatures = search_for_signatures(
        conn,
        SignatureSearchType::Standard,
        SignatureFilter::Account(hash),
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

    if signatures.len() == 0 {
        return Err(PhotonApiError::RecordNotFound(
            "Account not found".to_string(),
        ));
    }

    Ok(GetSignaturesForCompressedAccountResponse {
        value: SignatureInfoList { items: signatures },
        context,
    })
}
