use crate::common::typedefs::account::{Account, AccountV2};
use crate::dao::generated::accounts;

use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter};
use serde::Serialize;
use utoipa::ToSchema;

use super::super::error::PhotonApiError;
use super::utils::{
    parse_account_model, parse_account_model_v2, AccountDataTable, CompressedAccountRequest,
    Context,
};

// We do not use generics to simply documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountResponse {
    pub context: Context,
    pub value: Option<Account>,
}

pub async fn get_compressed_account(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<AccountResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let id = request.parse_id()?;
    let account_model = accounts::Entity::find()
        .filter(id.filter(AccountDataTable::Accounts))
        .one(conn)
        .await?;

    let account = account_model.map(parse_account_model).transpose()?;

    Ok(AccountResponse {
        value: { account },
        context,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountResponseV2 {
    pub context: Context,
    pub value: Option<AccountV2>,
}

pub async fn get_compressed_account_v2(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<AccountResponseV2, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let id = request.parse_id()?;
    let account_model = accounts::Entity::find()
        .filter(id.filter(AccountDataTable::Accounts))
        .one(conn)
        .await?;

    let account = account_model.map(parse_account_model_v2).transpose()?;

    Ok(AccountResponseV2 {
        value: { account },
        context,
    })
}
