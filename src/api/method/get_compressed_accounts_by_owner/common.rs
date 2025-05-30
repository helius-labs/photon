use crate::api::error::PhotonApiError;
use crate::api::method::get_compressed_accounts_by_owner::indexed_accounts::IndexedAccounts;
use crate::api::method::utils::PAGE_LIMIT;
use crate::common::typedefs::limit::Limit;
use crate::common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};
use crate::{common::typedefs::bs58_string::Base58String, ingester::persist::bytes_to_sql_format};
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// Max filters allowed constant value of 5
const MAX_FILTERS: usize = 5;
const MAX_CHILD_ACCOUNTS_WITH_FILTERS: usize = 1_000_000;

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
pub(crate) enum FilterInstance {
    Memcmp(Memcmp),
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct FilterSelector {
    pub memcmp: Option<Memcmp>,
}

impl FilterSelector {
    pub(crate) fn into_filter_instance(self) -> Result<FilterInstance, PhotonApiError> {
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

pub struct QueryBuilder {
    filters: Vec<String>,
    pub(crate) query_limit: u64,
    pub(crate) data_column: String,
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
            query_limit: PAGE_LIMIT,
            data_column: "data".to_string(),
        }
    }

    pub fn build_base_query(
        &mut self,
        conn: &DatabaseConnection,
        request: &GetCompressedAccountsByOwnerRequest,
    ) -> Result<(), PhotonApiError> {
        let owner_string = bytes_to_sql_format(conn.get_database_backend(), request.owner.into());

        self.filters.push(format!("owner = {owner_string}"));
        self.filters.push("spent = false".to_string());

        for filter_selector in &request.filters {
            match filter_selector.clone().into_filter_instance()? {
                FilterInstance::Memcmp(memcmp) => {
                    let filter_string = self.build_memcmp_filter(conn, memcmp)?;
                    self.filters.push(filter_string);
                }
            }
        }

        if let Some(cursor) = &request.cursor {
            let cursor_string =
                bytes_to_sql_format(conn.get_database_backend(), cursor.clone().into());
            self.filters.push(format!("hash > {cursor_string}"));
        }

        if let Some(limit) = &request.limit {
            self.query_limit = limit.value();
        }

        if let Some(slice) = &request.dataSlice {
            self.data_column = self.build_data_slice_column(conn, slice);
        }

        Ok(())
    }

    fn build_memcmp_filter(
        &self,
        conn: &DatabaseConnection,
        memcmp: Memcmp,
    ) -> Result<String, PhotonApiError> {
        let Memcmp { offset, bytes } = memcmp;
        let one_based_offset = offset + 1;
        let bytes = bytes.0;
        let bytes_len = bytes.len();
        let bytes_string = bytes_to_sql_format(conn.get_database_backend(), bytes);

        Ok(match conn.get_database_backend() {
            sea_orm::DatabaseBackend::Postgres => {
                format!("SUBSTRING(data FROM {one_based_offset} FOR {bytes_len}) = {bytes_string}")
            }
            sea_orm::DatabaseBackend::Sqlite => {
                format!("SUBSTR(data, {one_based_offset}, {bytes_len}) = {bytes_string}")
            }
            _ => {
                return Err(PhotonApiError::UnexpectedError(
                    "Unsupported database backend".to_string(),
                ))
            }
        })
    }

    fn build_data_slice_column(&self, conn: &DatabaseConnection, slice: &DataSlice) -> String {
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
            _ => panic!("Unsupported database backend"),
        }
    }

    pub async fn check_account_limits<T: IndexedAccounts>(
        conn: &DatabaseConnection,
        owner_string: &str,
        has_filters: bool,
    ) -> Result<(), PhotonApiError> {
        if !has_filters || T::is_indexed_account(owner_string) {
            return Ok(());
        }

        let owner_string = bytes_to_sql_format(conn.get_database_backend(), owner_string.into());
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

        Ok(())
    }

    pub fn get_query(&self, columns: &str) -> String {
        format!(
            "
            SELECT
                {columns}
            FROM accounts
            WHERE {}
            ORDER BY accounts.leaf_index, accounts.hash ASC
            LIMIT {}
            ",
            self.filters.join(" AND "),
            self.query_limit
        )
    }
}

pub fn validate_filters(filters: &[FilterSelector]) -> Result<(), PhotonApiError> {
    if filters.len() > MAX_FILTERS {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many filters. The maximum number of filters allowed is {}",
            MAX_FILTERS
        )));
    }
    Ok(())
}
