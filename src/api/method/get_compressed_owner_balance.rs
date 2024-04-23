use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::super::error::PhotonApiError;
use super::utils::Context;
use super::utils::{parse_decimal, AccountBalanceResponse, LamportModel};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetCompressedOwnerBalanceRequest {
    pub owner: SerializablePubkey,
}

pub async fn get_compressed_owner_balance(
    conn: &DatabaseConnection,
    request: GetCompressedOwnerBalanceRequest,
) -> Result<AccountBalanceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let owner = request.owner;

    let balances = accounts::Entity::find()
        .select_only()
        .column(accounts::Column::Lamports)
        .filter(
            accounts::Column::Owner
                .eq::<Vec<u8>>(owner.into())
                .and(accounts::Column::Spent.eq(false)),
        )
        .into_model::<LamportModel>()
        .all(conn)
        .await?
        .iter()
        .map(|x| parse_decimal(x.lamports))
        .collect::<Result<Vec<u64>, PhotonApiError>>()?;

    let total_balance = balances.iter().sum::<u64>();

    Ok(AccountBalanceResponse {
        value: total_balance,
        context,
    })
}
