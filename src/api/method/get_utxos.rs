use crate::dao::generated::utxos;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use super::super::error::PhotonApiError;
use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

use super::utils::{parse_utxo_model, Utxo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetUtxosRequest {
    pub owner: SerializablePubkey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetUtxosResponse {
    // TODO: Add cursor
    pub items: Vec<Utxo>,
}

pub async fn get_utxos(
    conn: &DatabaseConnection,
    request: GetUtxosRequest,
) -> Result<GetUtxosResponse, PhotonApiError> {
    let owner = request.owner;

    let filter = utxos::Column::Owner
        .eq::<Vec<u8>>(owner.into())
        .and(utxos::Column::Spent.eq(false));
    let result = utxos::Entity::find().filter(filter).all(conn).await?;

    Ok(GetUtxosResponse {
        items: result
            .into_iter()
            .map(parse_utxo_model)
            .collect::<Result<Vec<Utxo>, PhotonApiError>>()?,
    })
}
