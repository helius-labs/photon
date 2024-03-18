use crate::dao::generated::{token_owners, utxos};
use schemars::JsonSchema;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, QueryFilter, QuerySelect,
};
use serde::{Deserialize, Serialize};

use super::super::error::PhotonApiError;
use super::utils::{
    parse_token_owners_model, parse_utxo_model, BalanceModel, Context, GetCompressedAccountRequest,
    Utxo,
};
use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountBalanceResponseValue {
    amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedTokenAccountBalanceResponse {
    value: GetCompressedTokenAccountBalanceResponseValue,
    context: Context,
}

pub async fn get_compressed_token_account_balance(
    conn: &DatabaseConnection,
    request: GetCompressedAccountRequest,
) -> Result<GetCompressedTokenAccountBalanceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let GetCompressedAccountRequest { address } = request;
    let balance = token_owners::Entity::find()
        .select_only()
        .column(token_owners::Column::Amount)
        .filter(token_owners::Column::Account.eq::<Vec<u8>>(address.clone().into()))
        .into_model::<BalanceModel>()
        .one(conn)
        .await?
        .ok_or(PhotonApiError::RecordNotFound(format!(
            "Account {} not found",
            address
        )))?;
    Ok(GetCompressedTokenAccountBalanceResponse {
        value: GetCompressedTokenAccountBalanceResponseValue {
            amount: balance.amount.to_string(),
        },
        context,
    })
}
