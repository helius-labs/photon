use crate::dao::{generated::utxos, typedefs::hash::Hash};
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use serde::{Deserialize, Serialize};

use super::{
    super::error::PhotonApiError,
    utils::{Context, Limit, ResponseWithContext, PAGE_LIMIT},
};
use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

use super::utils::{parse_utxo_model, Utxo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Options {
    pub cursor: Option<Hash>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedProgramAccountsRequest(pub SerializablePubkey, pub Option<Options>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PaginatedUtxoList {
    pub items: Vec<Utxo>,
    pub cursor: Option<Hash>,
}

pub type GetCompressedProgramAccountsResponse = ResponseWithContext<PaginatedUtxoList>;

pub async fn get_compressed_program_accounts(
    conn: &DatabaseConnection,
    request: GetCompressedProgramAccountsRequest,
) -> Result<GetCompressedProgramAccountsResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let owner = request.0;

    let mut filter = utxos::Column::Owner
        .eq::<Vec<u8>>(owner.into())
        .and(utxos::Column::Spent.eq(false));

    let mut limit = PAGE_LIMIT;
    if let Some(options) = request.1 {
        if let Some(cursor) = options.cursor {
            filter = filter.and(utxos::Column::Hash.gt::<Vec<u8>>(cursor.into()));
        }
        if let Some(l) = options.limit {
            limit = l.value();
        }
    }

    let result = utxos::Entity::find()
        .order_by(utxos::Column::Hash, sea_orm::Order::Asc)
        .limit(limit)
        .filter(filter)
        .all(conn)
        .await?;

    let items = result
        .into_iter()
        .map(parse_utxo_model)
        .collect::<Result<Vec<Utxo>, PhotonApiError>>()?;
    let cursor = items.last().map(|u| u.hash.clone());

    Ok(GetCompressedProgramAccountsResponse {
        context,
        value: PaginatedUtxoList { items, cursor },
    })
}
