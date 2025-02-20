use crate::api::error::PhotonApiError;
use crate::api::method::get_compressed_accounts_by_owner::common::{
    validate_filters, GetCompressedAccountsByOwnerRequest, QueryBuilder,
};
use crate::api::method::get_compressed_accounts_by_owner::indexed_accounts::Solayer;
use crate::api::method::utils::parse_account_model_v2;
use crate::api::method::utils::Context;
use crate::common::typedefs::account::AccountV2;
use crate::common::typedefs::hash::Hash;
use crate::dao::generated::accounts;
use sea_orm::{ConnectionTrait, DatabaseConnection, FromQueryResult, Statement};
use serde::Serialize;
use utoipa::ToSchema;

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

    validate_filters(&request.filters)?;

    let owner_str = request.owner.to_string();
    QueryBuilder::check_account_limits::<Solayer>(conn, &owner_str, !request.filters.is_empty())
        .await?;

    let mut query_builder = QueryBuilder::new();
    query_builder.build_base_query(conn, &request)?;

    let columns = format!(
        "hash, {}, data_hash, address, owner, tree, queue, in_output_queue, nullifier_queue_index, tx_hash, nullifier, leaf_index, seq, slot_created, spent, prev_spent, lamports, discriminator, nullified_in_tree",
        query_builder.data_column
    );

    let raw_sql = query_builder.get_query(&columns);

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
    if items.len() < query_builder.query_limit as usize {
        cursor = None;
    }

    Ok(GetCompressedAccountsByOwnerV2Response {
        context,
        value: PaginatedAccountListV2 { items, cursor },
    })
}
