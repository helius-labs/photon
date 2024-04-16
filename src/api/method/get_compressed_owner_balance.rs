use crate::dao::generated::accounts;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};

use super::super::error::PhotonApiError;
use super::utils::Context;
use super::utils::{parse_decimal, AccountBalanceResponse, LamportModel, PubkeyRequest};

pub async fn get_compressed_owner_balance(
    conn: &DatabaseConnection,
    request: PubkeyRequest,
) -> Result<AccountBalanceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let owner = request.0;

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

    let total_balance = balances.iter().fold(0, |acc, balance| acc + balance);

    Ok(AccountBalanceResponse {
        value: total_balance,
        context,
    })
}
