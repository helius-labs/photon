use crate::{
    common::typedefs::{account::Account, bs58_string::Base58String},
    dao::generated::accounts,
    ingester::persist::bytes_to_sql_format,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, FromQueryResult, Statement};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    utils::{Context, Limit, PAGE_LIMIT},
};
use crate::common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};

use super::utils::parse_account_model;

// Max filters allowed constant value of 5
const MAX_FILTERS: usize = 5;
const MAX_CHILD_ACCOUNTS_WITH_FILTERS: usize = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Options {
    pub cursor: Option<Hash>,
    pub limit: Option<Limit>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct Memcmp {
    pub offset: usize,
    pub bytes: Base58String,
}

#[derive(Serialize, Deserialize, Debug)]
enum FilterInstance {
    Memcmp(Memcmp),
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct FilterSelector {
    pub memcmp: Option<Memcmp>,
}

impl FilterSelector {
    fn into_filter_instance(self) -> Result<FilterInstance, PhotonApiError> {
        if let Some(memcmp) = self.memcmp {
            Ok(FilterInstance::Memcmp(memcmp))
        } else {
            Err(PhotonApiError::ValidationError(
                "Filter instance cannot be null".to_string(),
            ))
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct DataSlice {
    pub offset: usize,
    pub length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct GetCompressedAccountsByOwnerRequest {
    pub owner: SerializablePubkey,
    #[serde(default)]
    pub filters: Vec<FilterSelector>,
    #[serde(default)]
    pub dataSlice: Option<DataSlice>,
    #[serde(default)]
    pub cursor: Option<Hash>,
    #[serde(default)]
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PaginatedAccountList {
    pub items: Vec<Account>,
    pub cursor: Option<Hash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountsByOwnerResponse {
    pub context: Context,
    pub value: PaginatedAccountList,
}

pub async fn get_compressed_accounts_by_owner(
    conn: &DatabaseConnection,
    request: GetCompressedAccountsByOwnerRequest,
) -> Result<GetCompressedAccountsByOwnerResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let GetCompressedAccountsByOwnerRequest {
        owner,
        cursor,
        limit,
        filters,
        dataSlice,
    } = request;

    if filters.len() > MAX_FILTERS {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many filters. The maximum number of filters allowed is {}",
            MAX_FILTERS
        )));
    }

    let owner_string = bytes_to_sql_format(conn.get_database_backend(), owner.into());

    if !filters.is_empty() {
        let raw_sql = format!(
            "
            SELECT CASE
                    WHEN COUNT(*) = {MAX_CHILD_ACCOUNTS_WITH_FILTERS} THEN true
                    ELSE false
                END AS has_too_many_rows
            FROM (
                SELECT 1
                FROM accounts
                WHERE owner = {owner_string}
                AND spent = false
                LIMIT {MAX_CHILD_ACCOUNTS_WITH_FILTERS}
            ) AS subquery;
            "
        );

        let stmt = Statement::from_string(conn.get_database_backend(), raw_sql);

        let result = conn.query_one(stmt).await?;

        match result {
            Some(row) => {
                let has_too_many_rows: bool = row.try_get("", "has_too_many_rows")?;
                if has_too_many_rows {
                    return Err(PhotonApiError::ValidationError(format!(
                        "Owner has too many children accounts. The maximum number of accounts allowed with filters is {}",
                        MAX_CHILD_ACCOUNTS_WITH_FILTERS
                    )));
                }
            }
            None => {
                return Err(PhotonApiError::UnexpectedError(
                    "Failed to check if there are more than 100k rows".to_string(),
                ));
            }
        }
    }

    let mut filters_strings = vec![];
    filters_strings.push(format!("owner = {owner_string}"));
    filters_strings.push("spent = false".to_string());

    for filter_selector in filters {
        match filter_selector.into_filter_instance()? {
            FilterInstance::Memcmp(memcmp) => {
                let Memcmp { offset, bytes } = memcmp;
                let one_based_offset = offset + 1;
                let bytes = bytes.0;
                let bytes_len = bytes.len();
                let bytes_string = bytes_to_sql_format(conn.get_database_backend(), bytes);
                let filter_string = match conn.get_database_backend() {
                    sea_orm::DatabaseBackend::Postgres => {
                        format!(
                            "SUBSTRING(data FROM {one_based_offset} FOR {bytes_len}) = {bytes_string}"
                        )
                    }
                    sea_orm::DatabaseBackend::Sqlite => {
                        format!("SUBSTR(data, {one_based_offset}, {bytes_len}) = {bytes_string}")
                    }
                    _ => {
                        panic!("Unsupported database backend");
                    }
                };
                filters_strings.push(filter_string);
            }
        }
    }

    if let Some(cursor) = cursor {
        let cursor_string = bytes_to_sql_format(conn.get_database_backend(), cursor.into());
        filters_strings.push(format!("hash > {cursor_string}"));
    }

    let mut query_limit = PAGE_LIMIT;
    if let Some(limit) = limit {
        query_limit = limit.value();
    }

    let filters = &filters_strings.join(" AND ");

    let data_column = dataSlice
        .map(|slice| {
            let DataSlice { offset, length } = slice;
            let one_based_offset = offset + 1;
            match conn.get_database_backend() {
                sea_orm::DatabaseBackend::Postgres => {
                    format!(
                        "SUBSTRING(data FROM {} FOR {}) AS data",
                        one_based_offset, length
                    )
                }
                sea_orm::DatabaseBackend::Sqlite => {
                    format!("SUBSTR(data, {}, {}) AS data", one_based_offset, length)
                }
                _ => {
                    panic!("Unsupported database backend");
                }
            }
        })
        .unwrap_or("data".to_string());

    let raw_sql = format!(
        "
        SELECT 
            hash,
            {data_column},
            data_hash,
            address,
            owner,
            tree,
            leaf_index,
            seq,
            slot_created,
            spent,
            prev_spent,
            lamports,
            discriminator
        FROM accounts
        WHERE {filters}
        ORDER BY accounts.hash ASC
        LIMIT {query_limit}
    "
    );

    let result: Vec<accounts::Model> = accounts::Model::find_by_statement(Statement::from_string(
        conn.get_database_backend(),
        raw_sql,
    ))
    .all(conn)
    .await?;

    let items = result
        .into_iter()
        .map(parse_account_model)
        .collect::<Result<Vec<Account>, PhotonApiError>>()?;

    let mut cursor = items.last().map(|u| u.hash.clone());
    if items.len() < query_limit as usize {
        cursor = None;
    }

    Ok(GetCompressedAccountsByOwnerResponse {
        context,
        value: PaginatedAccountList { items, cursor },
    })
}
