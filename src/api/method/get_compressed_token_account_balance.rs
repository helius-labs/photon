use crate::dao::generated::token_accounts;
use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::super::error::PhotonApiError;
use super::utils::{AccountDataTable, ResponseWithContext};
use super::utils::{BalanceModel, CompressedAccountRequest, Context};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
// This is a struct because in the future we might add other fields here like decimals or uiAmount,
// which is a string representation with decimals in the form of "10.00"
pub struct TokenAccountBalance {
    pub amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// We do not use generics to simplify documentation generation.
pub struct GetCompressedTokenAccountBalanceResponse {
    pub context: Context,
    pub value: TokenAccountBalance,
}

pub async fn get_compressed_token_account_balance(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<GetCompressedTokenAccountBalanceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let id = request.parse_id()?;
    let balance = token_accounts::Entity::find()
        .select_only()
        .column(token_accounts::Column::Amount)
        .filter(id.filter(AccountDataTable::TokenAccounts))
        .into_model::<BalanceModel>()
        .one(conn)
        .await?
        .ok_or(id.not_found_error())?;

    Ok(GetCompressedTokenAccountBalanceResponse {
        value: TokenAccountBalance {
            amount: balance.amount.to_string(),
        },
        context,
    })
}
