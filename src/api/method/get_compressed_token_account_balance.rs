use crate::dao::generated::token_owners;
use schemars::JsonSchema;
use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use serde::{Deserialize, Serialize};

use super::super::error::PhotonApiError;
use super::utils::{AccountDataTable, ResponseWithContext};
use super::utils::{BalanceModel, CompressedAccountRequest, Context};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
// This is a struct because in the future we might add other fields here like decimals or uiAmount,
// which is a string representation with decimals in the form of "10.00"
pub struct TokenAccountBalance {
    pub amount: String,
}

pub type GetCompressedTokenAccountBalanceResponse = ResponseWithContext<TokenAccountBalance>;

pub async fn get_compressed_token_account_balance(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<GetCompressedTokenAccountBalanceResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let id = request.parse_id()?;
    let balance = token_owners::Entity::find()
        .select_only()
        .column(token_owners::Column::Amount)
        .filter(id.filter(AccountDataTable::TokenOwners))
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
