use dao::generated::{token_owners};
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::error::PhotonApiError;
use dao::typedefs::serializable_pubkey::SerializablePubkey;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenInfoByOwnerRequest {
    pub owner: SerializablePubkey,
    pub mint: Option<SerializablePubkey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TokenUxto {
    pub owner: SerializablePubkey,
    pub mint: SerializablePubkey,
    pub amount: u64,
    pub delegate: Option<SerializablePubkey>,
    pub is_native: bool,
    pub close_authority: Option<SerializablePubkey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenInfoByOwnerResponse {
    pub total: i64,
    pub items: Vec<TokenUxto>,
}

fn _parse_model(token_owner: token_owners::Model) -> Result<TokenUxto, PhotonApiError> {
    Ok(TokenUxto {
        owner: token_owner.owner.try_into()?,
        mint: token_owner.mint.try_into()?,
        amount: token_owner.amount as u64,
        delegate: token_owner
            .delegate
            .map(SerializablePubkey::try_from)
            .transpose()?,
        is_native: token_owner.is_native.is_some(),
        close_authority: token_owner
            .close_authority
            .map(SerializablePubkey::try_from)
            .transpose()?,
    })
}

pub async fn get_compressed_account_token_accounts_by_owner(
    conn: &DatabaseConnection,
    request: GetCompressedTokenInfoByOwnerRequest,
) -> Result<GetCompressedTokenInfoByOwnerResponse, PhotonApiError> {
    let GetCompressedTokenInfoByOwnerRequest { owner, mint } = request;

    let mut filter = token_owners::Column::Owner.eq::<Vec<u8>>(owner.into());
    if let Some(m) = mint {
        filter = filter.and(token_owners::Column::Mint.eq::<Vec<u8>>(m.into()));
    }

    let result = token_owners::Entity::find()
        .filter(filter)
        .all(conn)
        .await?;

    let total = result.len() as i64;
    let items: Result<Vec<TokenUxto>, PhotonApiError> =
        result.into_iter().map(_parse_model).collect();
    let items = items?;

    Ok(GetCompressedTokenInfoByOwnerResponse { total, items })
}
