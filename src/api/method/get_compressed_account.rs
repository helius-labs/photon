use crate::dao::generated::accounts;

use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::super::error::PhotonApiError;
use super::utils::{
    parse_account_model, Account, AccountDataTable, CompressedAccountRequest, Context,
};

// We do not use generics to simply documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AccountResponse {
    pub context: Context,
    pub value: Account,
}

pub async fn get_compressed_account(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<AccountResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let id = request.parse_id()?;
    let account = parse_account_model(
        accounts::Entity::find()
            .filter(id.filter(AccountDataTable::Accounts))
            .one(conn)
            .await?
            .ok_or(id.not_found_error())?,
    )?;

    Ok(AccountResponse {
        value: { account },
        context,
    })
}
