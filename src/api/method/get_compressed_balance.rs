use crate::dao::generated::accounts;
use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};

use super::super::error::PhotonApiError;
use super::utils::{parse_decimal, AccountBalanceResponse, AccountDataTable, LamportModel};
use super::utils::{CompressedAccountRequest, Context};

pub async fn get_compressed_balance(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<AccountBalanceResponse, PhotonApiError> {
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

    Ok(AccountBalanceResponse {
        value: parse_decimal(balance.lamports)?,
        context,
    })
}
