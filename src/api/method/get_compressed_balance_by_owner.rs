use super::super::error::PhotonApiError;
use super::utils::{parse_decimal, AccountBalanceResponse, LamportModel};
use crate::common::typedefs::context::Context;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::owner_balances;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetCompressedBalanceByOwnerRequest {
    pub owner: SerializablePubkey,
}

pub async fn get_compressed_balance_by_owner(
    conn: &DatabaseConnection,
    request: GetCompressedBalanceByOwnerRequest,
) -> Result<AccountBalanceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let owner = request.owner;

    let balances = owner_balances::Entity::find()
        .select_only()
        .column(owner_balances::Column::Lamports)
        .filter(owner_balances::Column::Owner.eq::<Vec<u8>>(owner.into()))
        .into_model::<LamportModel>()
        .all(conn)
        .await?
        .iter()
        .map(|x| parse_decimal(x.lamports))
        .collect::<Result<Vec<u64>, PhotonApiError>>()?;

    let total_balance = balances.iter().sum::<u64>();

    Ok(AccountBalanceResponse {
        value: UnsignedInteger(total_balance),
        context,
    })
}
