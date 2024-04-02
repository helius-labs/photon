use crate::dao::generated::accounts;
use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};

use super::super::error::PhotonApiError;
use super::utils::{AccountDataTable, LamportModel};
use super::utils::{CompressedAccountRequest, Context, ResponseWithContext};

pub type GetCompressedAccountBalance = ResponseWithContext<u64>;

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
        value: balance.lamports,
        context,
    })
}
