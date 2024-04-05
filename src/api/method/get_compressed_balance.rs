use crate::dao::generated::accounts;
use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::super::error::PhotonApiError;
use super::utils::{parse_decimal, AccountDataTable, LamportModel};
use super::utils::{CompressedAccountRequest, Context};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// We do not use generics to simplify documentation generation.
pub struct GetCompressedAccountBalance {
    pub context: Context,
    pub value: u64,
}

pub async fn get_compressed_balance(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<GetCompressedAccountBalance, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let id = request.parse_id()?;

    let balance = accounts::Entity::find()
        .select_only()
        .column(accounts::Column::Lamports)
        .filter(id.filter(AccountDataTable::Accounts))
        .into_model::<LamportModel>()
        .one(conn)
        .await?
        .ok_or(id.not_found_error())?;

    Ok(GetCompressedAccountBalance {
        value: parse_decimal(balance.lamports)?,
        context,
    })
}
