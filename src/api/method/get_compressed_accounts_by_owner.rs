use crate::{
    common::typedefs::{account::Account, bs64_string::Base64String},
    dao::generated::accounts,
};
use env_logger::filter::Filter;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    utils::{Context, Limit, PAGE_LIMIT},
};
use crate::common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};

use super::utils::parse_account_model;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Options {
    pub cursor: Option<Hash>,
    pub limit: Option<Limit>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Memcmp {
    offset: usize,
    bytes: Base64String,
}

#[derive(Serialize, Deserialize, Debug)]
enum FilterInstance {
    Memcmp(Memcmp),
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
struct FilterSelector {
    memcmp: Option<Memcmp>,
}

impl FilterSelector {
    fn into_filter_instance(self) -> FilterInstance {
        if let Some(memcmp) = self.memcmp {
            FilterInstance::Memcmp(memcmp)
        } else {
            panic!("No filter selected")
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
struct DataSlice {
    offset: usize,
    length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountsByOwnerRequest {
    pub owner: SerializablePubkey,
    pub filters: Vec<FilterSelector>,
    #[allow(non_snake_case)]
    pub dataSlice: Option<DataSlice>,
    pub cursor: Option<Hash>,
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

    let mut filter = accounts::Column::Owner
        .eq::<Vec<u8>>(owner.into())
        .and(accounts::Column::Spent.eq(false));

    if let Some(cursor) = cursor {
        filter = filter.and(accounts::Column::Hash.gt::<Vec<u8>>(cursor.into()));
    }

    for filter_selector in filters {
        match filter_selector.into_filter_instance() {
            FilterInstance::Memcmp(memcmp) => {
                filter = filter.and(
                    accounts::Column::Data
                        .slice(memcmp.offset, memcmp.bytes.len())
                        .eq::<Vec<u8>>(memcmp.bytes.into()),
                );
            }
        }
    }
    let mut query_limit = PAGE_LIMIT;
    if let Some(limit) = limit {
        query_limit = limit.value();
    }

    let result = accounts::Entity::find()
        .order_by(accounts::Column::Hash, sea_orm::Order::Asc)
        .limit(query_limit)
        .filter(filter)
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
