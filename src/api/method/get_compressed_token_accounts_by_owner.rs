use schemars::JsonSchema;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};

use super::utils::{OwnerOrDelegate, TokenAccountListResponse};
use super::{super::error::PhotonApiError, utils::fetch_token_accounts};
use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountsByOwnerRequest {
    pub owner: SerializablePubkey,
    pub mint: Option<SerializablePubkey>,
}

pub async fn get_compressed_token_accounts_by_owner(
    conn: &DatabaseConnection,
    request: GetCompressedTokenAccountsByOwnerRequest,
) -> Result<TokenAccountListResponse, PhotonApiError> {
    let GetCompressedTokenAccountsByOwnerRequest { owner, mint } = request;
    fetch_token_accounts(conn, OwnerOrDelegate::Owner(owner), mint).await
}
