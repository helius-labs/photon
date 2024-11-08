use std::collections::HashMap;

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::common::typedefs::bs58_string::Base58String;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::token_owner_balances;

use super::utils::{
    Authority, Context, GetCompressedTokenAccountsByAuthorityOptions, Limit, PAGE_LIMIT,
};
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
    let GetCompressedTokenBalancesByOwnerRequest {
        owner,
        mint,
        cursor,
        limit,
    } = request;
    let filter = token_owner_balances::Column::Owner.eq::<Vec<u8>>(owner.into());
    if let Some(mint) = mint {
        filter = filter.and(token_owner_balances::Column::Mint.eq::<Vec<u8>>(mint.into()));
    }
    if let Some(cursor) = cursor {
        let bytes = cursor.0;
        let legacy_cursor_length = 64;
        let expected_cursor_length = 32;
        let mint = match bytes.len() {
            legacy_cursor_length => {
                // HACK: Hash is not used for token owner balances, but keep it for backward compatibility with a bug interface
                let (mint, _hash) = bytes.split_at(32);
                mint.to_vec()
            }
            expected_cursor_length => bytes.to_vec(),
            _ => {
                return Err(PhotonApiError::ValidationError(format!(
                    "Invalid cursor length. Expected {}. Received {}.",
                    expected_cursor_length,
                    bytes.len()
                )));
            }
        };
        filter = filter.and(token_owner_balances::Column::Mint.gte::<Vec<u8>>(mint.into()));
    }
    let limit = limit.map(|l| l.value()).unwrap_or(PAGE_LIMIT);

    let items = token_owner_balances::Entity::find()
        .find_also_related(accounts::Entity)
        .filter(filter)
        .order_by_asc(token_accounts::Column::Mint)
        .order_by_asc(token_accounts::Column::Hash)
        .limit(limit)
        .all(conn)
        .await?
        .drain(..)
        .map(|(token_account, account)| {
            let account = account.ok_or(PhotonApiError::RecordNotFound(
                "Base account not found for token account".to_string(),
            ))?;
            Ok(TokenAcccount {
                account: parse_account_model(account)?,
                token_data: TokenData {
                    mint: token_account.mint.try_into()?,
                    owner: token_account.owner.try_into()?,
                    amount: UnsignedInteger(parse_decimal(token_account.amount)?),
                    delegate: token_account
                        .delegate
                        .map(SerializablePubkey::try_from)
                        .transpose()?,
                    state: (AccountState::try_from(token_account.state as u8)).map_err(|e| {
                        PhotonApiError::UnexpectedError(format!(
                            "Unable to parse account state {}",
                            e
                        ))
                    })?,
                    tlv: token_account.tlv.map(Base64String),
                },
            })
        })
        .collect::<Result<Vec<TokenAcccount>, PhotonApiError>>()?;

    let mut cursor = items.last().map(|item| {
        Base58String({
            let item = item.clone();
            let mut bytes: Vec<u8> = item.token_data.mint.into();
            let hash_bytes: Vec<u8> = item.account.hash.into();
            bytes.extend_from_slice(hash_bytes.as_slice());
            bytes
        })
    });
    if items.len() < limit as usize {
        cursor = None;
    }

    Ok(TokenAccountListResponse {
        value: TokenAccountList { items, cursor },
        context,
    })
}
