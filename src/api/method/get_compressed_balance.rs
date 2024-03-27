use crate::dao::generated::utxos;
use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};

use super::super::error::PhotonApiError;
use super::utils::{AccountDataTable, LamportModel};
use super::utils::{CompressedAccountRequest, Context, ResponseWithContext};

pub type GetCompressedAccountBalance = ResponseWithContext<i64>;

pub async fn get_compressed_balance(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<GetCompressedAccountBalance, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let id = request.parse_id()?;

    let balance = utxos::Entity::find()
        .select_only()
        .column(utxos::Column::Lamports)
        .filter(id.get_filter(AccountDataTable::Utxos))
        .into_model::<LamportModel>()
        .one(conn)
        .await?
        .ok_or(id.get_record_not_found_error())?;

    Ok(GetCompressedAccountBalance {
        value: balance.lamports,
        context,
    })
}
