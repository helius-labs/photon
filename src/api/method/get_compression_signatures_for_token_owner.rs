use super::{
    super::error::PhotonApiError,
    utils::{
        search_for_signatures, GetPaginatedSignaturesResponse, SignatureFilter, SignatureSearchType,
    },
};
use crate::common::typedefs::context::Context;
use crate::common::typedefs::limit::Limit;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressionSignaturesForTokenOwnerRequest {
    pub owner: SerializablePubkey,
    #[serde(default)]
    pub limit: Option<Limit>,
    #[serde(default)]
    pub cursor: Option<String>,
}

pub async fn get_compression_signatures_for_token_owner(
    conn: &DatabaseConnection,
    request: GetCompressionSignaturesForTokenOwnerRequest,
) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let signatures = search_for_signatures(
        conn,
        SignatureSearchType::Token,
        Some(SignatureFilter::Owner(request.owner)),
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
