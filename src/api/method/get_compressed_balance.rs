use crate::dao::generated::utxos;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};

use super::super::error::PhotonApiError;
use super::utils::{BalanceModel, Context, GetCompressedAccountRequest, ResponseWithContext};

pub type GetCompressedAccountBalance = ResponseWithContext<i64>;

pub async fn get_compressed_balance(
    conn: &DatabaseConnection,
    request: GetCompressedAccountRequest,
) -> Result<GetCompressedAccountBalance, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let GetCompressedAccountRequest { address } = request;
    let balance = utxos::Entity::find()
        .select_only()
        .column(utxos::Column::Lamports)
        .filter(utxos::Column::Account.eq::<Vec<u8>>(address.clone().into()))
        .into_model::<BalanceModel>()
        .one(conn)
        .await?
        .ok_or(PhotonApiError::RecordNotFound(format!(
            "Account {} not found",
            address
        )))?;

    Ok(GetCompressedAccountBalance {
        value: balance.amount,
        context,
    })
}
