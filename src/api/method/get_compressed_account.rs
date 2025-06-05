use crate::common::typedefs::account::{Account, AccountV2};
use crate::dao::generated::accounts;

use super::super::error::PhotonApiError;
use super::utils::{AccountDataTable, CompressedAccountRequest};
use crate::common::typedefs::context::Context;
use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter};
use serde::Serialize;
use utoipa::ToSchema;

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

    let account = account_model.map(TryFrom::try_from).transpose()?;

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

    let account = account_model.map(TryFrom::try_from).transpose()?;

    Ok(AccountResponseV2 {
        value: { account },
        context,
    })
}
