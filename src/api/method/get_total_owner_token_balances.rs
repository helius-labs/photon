use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::common::typedefs::serializable_pubkey::SerializablePubkey;

use super::utils::{
    Authority, Context, GetCompressedTokenAccountsByAuthorityOptions,
    GetCompressedTokenAccountsByOwner, TokenAccountListResponse,
};
use super::{super::error::PhotonApiError, utils::fetch_token_accounts};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenBalance {
    pub mint: SerializablePubkey,
    pub balance: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenBalanceList {
    pub token_balances: Vec<TokenBalance>,
    pub cursor: Option<String>,
}

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenBalanceResponse {
    pub context: Context,
    pub value: TokenBalanceList,
}

fn fetch_total_owner_token_balance(
    conn: &DatabaseConnection,
    authority: Authority,
    options: GetCompressedTokenAccountsByAuthorityOptions,
) -> TokenBalanceList {
    unimplemented!()
}

pub async fn get_total_owner_token_balances(
    conn: &DatabaseConnection,
    request: GetCompressedTokenAccountsByOwner,
) -> Result<TokenAccountListResponse, PhotonApiError> {
    let GetCompressedTokenAccountsByOwner {
        owner,
        mint,
        cursor,
        limit,
    } = request;
    let options = GetCompressedTokenAccountsByAuthorityOptions {
        mint,
        cursor,
        limit,
    };
    fetch_token_accounts(conn, Authority::Owner(owner), options).await
}
