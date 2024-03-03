use dao::generated::token_ownership;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use dao::typedefs::serializable_pubkey::SerializablePubkey;
use crate::error::PhotonApiError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountsByOwnerRequest {
    pub owner: SerializablePubkey,
    pub mint: Option<SerializablePubkey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TokenAccount {
    pub mint: SerializablePubkey,
    pub amount: u64,
    pub delegate: Option<SerializablePubkey>,
    pub is_native: bool,
    pub close_authority: Option<SerializablePubkey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountsByOwnerResponse {
    pub total: i64,
    pub items: Vec<TokenAccount>,
}

pub async fn get_compressed_account_token_accounts_by_owner(
    conn: &DatabaseConnection,
    request: GetCompressedTokenAccountsByOwnerRequest,
) -> Result<Option<GetCompressedTokenAccountsByOwnerResponse>, PhotonApiError> {
    let GetCompressedTokenAccountsByOwnerRequest { owner, mint } = request;

    let mut filter = token_ownership::Column::Owner.eq::<Vec<u8>>(owner.into());
    if let Some(m) = mint {
        filter = filter.and(token_ownership::Column::Mint.eq::<Vec<u8>>(m.into()));
    }

    let result = token_ownership::Entity::find()
        .filter(filter)
        .all(conn)
        .await?;

    let total = result.len() as i64;
    let items = result
        .into_iter()
        .map(|ownership| TokenAccount {
            mint: ownership.mint.into(),
            amount: ownership.amount as u64,
            delegate: ownership.delegate.map(SerializablePubkey::from),
            is_native: ownership.is_native.is_some(),
            close_authority: ownership.close_authority.map(SerializablePubkey::from),
        })
        .collect();

    Ok(Some(GetCompressedTokenAccountsByOwnerResponse {
        total,
        items,
    }))
}
