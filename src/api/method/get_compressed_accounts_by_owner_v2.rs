use crate::{
    dao::generated::accounts,
    ingester::persist::bytes_to_sql_format,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, FromQueryResult, Statement};
use serde::Serialize;
use utoipa::ToSchema;
use crate::api::method::get_compressed_accounts_by_owner::{DataSlice, FilterInstance, GetCompressedAccountsByOwnerRequest, Memcmp};
use super::{
    super::error::PhotonApiError,
    utils::{Context, PAGE_LIMIT},
};
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::account::AccountV2;
use super::utils::parse_account_model_v2;

// Max filters allowed constant value of 5
const MAX_FILTERS: usize = 5;
const MAX_CHILD_ACCOUNTS_WITH_FILTERS: usize = 1_000_000;
const SOL_LAYER_ACCOUNTS: [&str; 2] = [
    "S1ay5sk6FVkvsNFZShMw2YK3nfgJZ8tpBBGuHWDZ266",
    "2sYfW81EENCMe415CPhE2XzBA5iQf4TXRs31W1KP63YT",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PaginatedAccountListV2 {
    pub items: Vec<AccountV2>,
    pub cursor: Option<Hash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountsByOwnerV2Response {
    pub context: Context,
    pub value: PaginatedAccountListV2,
}

pub async fn get_compressed_accounts_by_owner_v2(
    conn: &DatabaseConnection,
    request: GetCompressedAccountsByOwnerRequest,
) -> Result<GetCompressedAccountsByOwnerV2Response, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let GetCompressedAccountsByOwnerRequest {
        owner,
        cursor,
        limit,
        filters,
        data_slice,
    } = request;

    if filters.len() > MAX_FILTERS {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many filters. The maximum number of filters allowed is {}",
            MAX_FILTERS
        )));
    }


    let owner_string = bytes_to_sql_format(conn.get_database_backend(), owner.into());
    if !filters.is_empty() && !SOL_LAYER_ACCOUNTS.contains(&owner.to_string().as_str()) {
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

    let data_column = data_slice
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
            in_queue,
            spent,
            prev_spent,
            nullifier,
            tx_hash,
            leaf_index,
            seq,
            slot_created,
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
        .map(parse_account_model_v2)
        .collect::<Result<Vec<AccountV2>, PhotonApiError>>()?;

    let mut cursor = items.last().map(|u| u.hash.clone());
    if items.len() < query_limit as usize {
        cursor = None;
    }

    Ok(GetCompressedAccountsByOwnerV2Response {
        context,
        value: PaginatedAccountListV2 { items, cursor },
    })
}
