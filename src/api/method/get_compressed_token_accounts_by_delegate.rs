use schemars::JsonSchema;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};

use super::{
    super::error::PhotonApiError,
    utils::{fetch_token_accounts, OwnerOrDelegate, TokenAccountList},
};
use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountsByDelegateRequest {
    pub delegate: SerializablePubkey,
    pub mint: Option<SerializablePubkey>,
}

pub async fn get_compressed_account_token_accounts_by_delegate(
    conn: &DatabaseConnection,
    request: GetCompressedTokenAccountsByDelegateRequest,
) -> Result<TokenAccountList, PhotonApiError> {
    let GetCompressedTokenAccountsByDelegateRequest { delegate, mint } = request;
    fetch_token_accounts(conn, OwnerOrDelegate::Delegate(delegate), mint).await
}
