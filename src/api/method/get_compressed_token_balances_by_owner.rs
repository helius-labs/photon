use std::collections::HashMap;

use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::common::typedefs::bs58_string::Base58String;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;

use super::utils::{Authority, Context, GetCompressedTokenAccountsByAuthorityOptions, Limit};
use super::{super::error::PhotonApiError, utils::fetch_token_accounts};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenBalance {
    pub mint: SerializablePubkey,
    pub balance: UnsignedInteger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenBalanceList {
    pub token_balances: Vec<TokenBalance>,
    pub cursor: Option<String>,
}

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenBalancesResponse {
    pub context: Context,
    pub value: TokenBalanceList,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
pub struct GetCompressedTokenBalancesByOwnerRequest {
    pub owner: SerializablePubkey,
    pub mint: Option<SerializablePubkey>,
    pub cursor: Option<Base58String>,
    pub limit: Option<Limit>,
}

pub async fn get_compressed_token_balances_by_owner(
    conn: &DatabaseConnection,
    request: GetCompressedTokenBalancesByOwnerRequest,
) -> Result<TokenBalancesResponse, PhotonApiError> {
    let GetCompressedTokenBalancesByOwnerRequest {
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
    let token_accounts = fetch_token_accounts(conn, Authority::Owner(owner), options).await?;
    let mut mint_to_balance: HashMap<SerializablePubkey, u64> = HashMap::new();

    for token_account in token_accounts.value.items.iter() {
        let balance = mint_to_balance
            .entry(token_account.token_data.mint)
            .or_insert(0);
        *balance += token_account.token_data.amount.0;
    }
    let token_balances: Vec<TokenBalance> = mint_to_balance
        .into_iter()
        .map(|(mint, balance)| TokenBalance {
            mint,
            balance: UnsignedInteger(balance),
        })
        .collect();

    Ok(TokenBalancesResponse {
        context: token_accounts.context,
        value: TokenBalanceList {
            token_balances,
            cursor: None,
        },
    })
}
