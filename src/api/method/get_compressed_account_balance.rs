use super::super::error::PhotonApiError;
use super::utils::CompressedAccountRequest;
use super::utils::{parse_decimal, AccountBalanceResponse, AccountDataTable, LamportModel};
use crate::common::typedefs::context::Context;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::accounts;
use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use sqlx::types::Decimal;

pub async fn get_compressed_account_balance(
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
        .map(|x| x.lamports)
        .unwrap_or(Decimal::from(0));

    Ok(AccountBalanceResponse {
        value: UnsignedInteger(parse_decimal(balance)?),
        context,
    })
}
