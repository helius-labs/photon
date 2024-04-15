use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    get_multiple_compressed_account_proofs::{
        get_multiple_compressed_account_proofs_helper, MerkleProofWithContext,
    },
    utils::{
        search_for_signatures, Context, HashRequest, SignatureFilter, SignatureInfoList,
        SignatureSearchType,
    },
};
use crate::{
    common::typedefs::hash::Hash,
    dao::generated::{account_transactions, transactions},
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
