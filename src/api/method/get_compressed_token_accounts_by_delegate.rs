use crate::dao::generated::token_owners;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use super::{
    super::error::PhotonApiError,
    utils::{parse_token_owners_model, TokenAccountList, TokenUxto},
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

    let mut filter = token_owners::Column::Delegate.eq::<Vec<u8>>(delegate.into());
    if let Some(m) = mint {
        filter = filter.and(token_owners::Column::Mint.eq::<Vec<u8>>(m.into()));
    }

    let result = token_owners::Entity::find()
        .filter(filter)
        .all(conn)
        .await?;

    let items: Result<Vec<TokenUxto>, PhotonApiError> =
        result.into_iter().map(parse_token_owners_model).collect();
    let items = items?;

    Ok(TokenAccountList { items })
}
