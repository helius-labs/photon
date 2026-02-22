use super::{super::error::PhotonApiError, utils::PAGE_LIMIT};
use crate::common::typedefs::account::{Account, AccountV2};
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::dao::generated::accounts;
use crate::dao::helpers::{find_accounts_by_addresses, find_accounts_by_hashes};
use sea_orm::DatabaseConnection;
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
    let hash_to_account = find_accounts_by_hashes(conn, &hashes, Some(spent))
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

    Ok(hashes
        .into_iter()
        .map(|hash| hash_to_account.get(&hash.to_vec()).cloned())
        .collect())
}

async fn fetch_account_from_addresses(
    conn: &DatabaseConnection,
    addresses: Vec<SerializablePubkey>,
) -> Result<Vec<Option<accounts::Model>>, PhotonApiError> {
    let address_to_account = find_accounts_by_addresses(conn, &addresses, Some(false))
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

    Ok(addresses
        .into_iter()
        .map(|addr| address_to_account.get(&addr.to_bytes_vec()).cloned())
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
        (Some(_), Some(_)) => {
            return Err(PhotonApiError::ValidationError(
                "Cannot provide both hashes and addresses".to_string(),
            ));
        }
        (None, None) => {
            return Err(PhotonApiError::ValidationError(
                "Either hashes or addresses must be provided".to_string(),
            ));
        }
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
