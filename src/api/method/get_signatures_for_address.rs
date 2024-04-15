use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    get_multiple_compressed_account_proofs::{
        get_multiple_compressed_account_proofs_helper, MerkleProofWithContext,
    },
    utils::{
        search_for_signatures, Context, GetPaginatedSignaturesResponse, HashRequest, Limit,
        SignatureFilter, SignatureInfoList, SignatureSearchType,
    },
};
use crate::{
    common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey},
    dao::generated::{account_transactions, transactions},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GetSignaturesForAddressRequest {
    pub address: SerializablePubkey,
    pub limit: Option<Limit>,
    pub cursor: Option<String>,
}

pub async fn get_signatures_for_address(
    conn: &DatabaseConnection,
    request: GetSignaturesForAddressRequest,
) -> Result<GetPaginatedSignaturesResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let signatures = search_for_signatures(
        conn,
        SignatureSearchType::Standard,
        SignatureFilter::Address(request.address),
        request.cursor,
        request.limit,
    )
    .await?;

    Ok(GetPaginatedSignaturesResponse {
        value: signatures,
        context,
    })
}
