use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::common::typedefs::bs58_string::Base58String;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::limit::Limit;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::token_owner_balances;

use super::super::error::PhotonApiError;
use super::utils::{parse_decimal, PAGE_LIMIT};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenBalance {
    pub mint: SerializablePubkey,
    pub balance: UnsignedInteger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenBalanceList {
    pub token_balances: Vec<TokenBalance>,
    pub cursor: Option<Base58String>,
}

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TokenBalancesResponse {
    pub context: Context,
    pub value: TokenBalanceList,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
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
    let context = Context::extract(conn).await?;
    let GetCompressedTokenBalancesByOwnerRequest {
        owner,
        mint,
        cursor,
        limit,
    } = request;
    let mut filter = token_owner_balances::Column::Owner.eq::<Vec<u8>>(owner.into());
    if let Some(mint) = mint {
        filter = filter.and(token_owner_balances::Column::Mint.eq::<Vec<u8>>(mint.into()));
    }
    if let Some(cursor) = cursor {
        let bytes = cursor.0;
        let expected_cursor_length = 32;
        let mint = if bytes.len() == expected_cursor_length {
            bytes.to_vec()
        } else {
            return Err(PhotonApiError::ValidationError(format!(
                "Invalid cursor length. Expected {}. Received {}.",
                expected_cursor_length,
                bytes.len()
            )));
        };
        filter = filter.and(token_owner_balances::Column::Mint.gt::<Vec<u8>>(mint.into()));
    }
    let limit = limit.map(|l| l.value()).unwrap_or(PAGE_LIMIT);

    let items = token_owner_balances::Entity::find()
        .filter(filter)
        .order_by_asc(token_owner_balances::Column::Mint)
        .limit(limit)
        .all(conn)
        .await?
        .drain(..)
        .map(|token_owner_balance| {
            Ok(TokenBalance {
                mint: token_owner_balance.mint.try_into()?,
                balance: UnsignedInteger(parse_decimal(token_owner_balance.amount)?),
            })
        })
        .collect::<Result<Vec<TokenBalance>, PhotonApiError>>()?;

    let mut cursor = items.last().map(|item| {
        Base58String({
            let item = item.clone();
            let bytes: Vec<u8> = item.mint.into();
            bytes
        })
    });
    if items.len() < limit as usize {
        cursor = None;
    }

    Ok(TokenBalancesResponse {
        value: TokenBalanceList {
            token_balances: items,
            cursor,
        },
        context,
    })
}

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TokenBalancesResponseV2 {
    pub context: Context,
    pub value: TokenBalanceListV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenBalanceListV2 {
    pub items: Vec<TokenBalance>,
    pub cursor: Option<Base58String>,
}

pub async fn get_compressed_token_balances_by_owner_v2(
    conn: &DatabaseConnection,
    request: GetCompressedTokenBalancesByOwnerRequest,
) -> Result<TokenBalancesResponseV2, PhotonApiError> {
    let response = get_compressed_token_balances_by_owner(conn, request).await?;
    let context = response.context;
    let token_balance_list = response.value;
    let token_balances = token_balance_list.token_balances;
    let cursor = token_balance_list.cursor;
    Ok(TokenBalancesResponseV2 {
        value: TokenBalanceListV2 {
            items: token_balances,
            cursor,
        },
        context,
    })
}
