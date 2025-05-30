use std::collections::HashMap;

use super::{super::error::PhotonApiError, utils::PAGE_LIMIT};
use crate::common::typedefs::account::{Account, AccountV2};
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};
use utoipa::{
    openapi::{RefOr, Schema},
    ToSchema,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleCompressedAccountsRequest {
    #[serde(default)]
    pub hashes: Option<Vec<Hash>>,
    #[serde(default)]
    pub addresses: Option<Vec<SerializablePubkey>>,
}

impl GetMultipleCompressedAccountsRequest {
    pub fn adjusted_schema() -> RefOr<Schema> {
        let mut schema = GetMultipleCompressedAccountsRequest::schema().1;
        let object = match schema {
            RefOr::T(Schema::Object(ref mut object)) => {
                let example = serde_json::to_value(GetMultipleCompressedAccountsRequest {
                    hashes: Some(vec![Hash::new_unique(), Hash::new_unique()]),
                    addresses: None,
                })
                .unwrap();
                object.default = Some(example.clone());
                object.example = Some(example);
                object.description = Some("Request for compressed account data".to_string());
                object.clone()
            }
            _ => unimplemented!(),
        };
        RefOr::T(Schema::Object(object))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountList {
    pub items: Vec<Option<Account>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
pub struct AccountListV2 {
    pub items: Vec<Option<AccountV2>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
// We do not use generics in order to simplify documentation generation
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleCompressedAccountsResponse {
    pub context: Context,
    pub value: AccountList,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleCompressedAccountsResponseV2 {
    pub context: Context,
    pub value: AccountListV2,
}

pub async fn fetch_accounts_from_hashes(
    conn: &DatabaseConnection,
    hashes: Vec<Hash>,
    spent: bool,
) -> Result<Vec<Option<accounts::Model>>, PhotonApiError> {
    let raw_hashes: Vec<Vec<u8>> = hashes.into_iter().map(|hash| hash.to_vec()).collect();

    let accounts = accounts::Entity::find()
        .filter(
            accounts::Column::Hash
                .is_in(raw_hashes.clone())
                .and(accounts::Column::Spent.eq(spent)),
        )
        .all(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

    let hash_to_account: HashMap<Vec<u8>, accounts::Model> = accounts
        .into_iter()
        .map(|account| (account.hash.clone(), account))
        .collect();

    Ok(raw_hashes
        .into_iter()
        .map(|hash| hash_to_account.get(&hash).cloned())
        .collect())
}

async fn fetch_account_from_addresses(
    conn: &DatabaseConnection,
    addresses: Vec<SerializablePubkey>,
) -> Result<Vec<Option<accounts::Model>>, PhotonApiError> {
    let raw_addresses: Vec<Vec<u8>> = addresses.into_iter().map(|addr| addr.into()).collect();
    let accounts = accounts::Entity::find()
        .filter(
            accounts::Column::Address
                .is_in(raw_addresses.clone())
                .and(accounts::Column::Spent.eq(false)),
        )
        .all(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;
    let address_to_account: HashMap<Option<Vec<u8>>, accounts::Model> = accounts
        .into_iter()
        .map(|account| (account.address.clone(), account))
        .collect();
    Ok(raw_addresses
        .into_iter()
        .map(|addr| address_to_account.get(&Some(addr)).cloned())
        .collect())
}

pub async fn get_multiple_compressed_accounts(
    conn: &DatabaseConnection,
    request: GetMultipleCompressedAccountsRequest,
) -> Result<GetMultipleCompressedAccountsResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let accounts = match (request.hashes, request.addresses) {
        (Some(hashes), None) => {
            if hashes.len() > PAGE_LIMIT as usize {
                return Err(PhotonApiError::ValidationError(format!(
                    "Too many hashes requested {}. Maximum allowed: {}",
                    hashes.len(),
                    PAGE_LIMIT
                )));
            }
            fetch_accounts_from_hashes(conn, hashes, false).await?
        }
        (None, Some(addresses)) => {
            if addresses.len() > PAGE_LIMIT as usize {
                return Err(PhotonApiError::ValidationError(format!(
                    "Too many addresses requested {}. Maximum allowed: {}",
                    addresses.len(),
                    PAGE_LIMIT
                )));
            }
            fetch_account_from_addresses(conn, addresses).await?
        }
        _ => panic!("Either hashes or addresses must be provided"),
    };

    Ok(GetMultipleCompressedAccountsResponse {
        context,
        value: AccountList {
            items: accounts
                .into_iter()
                .map(|x| x.map(TryFrom::try_from).transpose())
                .collect::<Result<Vec<_>, _>>()?,
        },
    })
}

pub async fn get_multiple_compressed_accounts_v2(
    conn: &DatabaseConnection,
    request: GetMultipleCompressedAccountsRequest,
) -> Result<GetMultipleCompressedAccountsResponseV2, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let accounts = match (request.hashes, request.addresses) {
        (Some(hashes), None) => {
            if hashes.len() > PAGE_LIMIT as usize {
                return Err(PhotonApiError::ValidationError(format!(
                    "Too many hashes requested {}. Maximum allowed: {}",
                    hashes.len(),
                    PAGE_LIMIT
                )));
            }
            fetch_accounts_from_hashes(conn, hashes, false).await?
        }
        (None, Some(addresses)) => {
            if addresses.len() > PAGE_LIMIT as usize {
                return Err(PhotonApiError::ValidationError(format!(
                    "Too many addresses requested {}. Maximum allowed: {}",
                    addresses.len(),
                    PAGE_LIMIT
                )));
            }
            fetch_account_from_addresses(conn, addresses).await?
        }
        _ => Err(PhotonApiError::ValidationError(
            "Either hashes or addresses must be provided".to_string(),
        ))?,
    };

    Ok(GetMultipleCompressedAccountsResponseV2 {
        context,
        value: AccountListV2 {
            items: accounts
                .into_iter()
                .map(|x| x.map(TryFrom::try_from).transpose())
                .collect::<Result<Vec<_>, _>>()?,
        },
    })
}
