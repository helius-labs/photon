use crate::dao::generated::utxos;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use serde::{Deserialize, Serialize};

use super::super::error::PhotonApiError;
use super::utils::{BalanceModel, Context, GetCompressedAccountRequest};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountBalance {
    value: i64,
    context: Context,
}

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
