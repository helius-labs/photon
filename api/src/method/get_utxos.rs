use dao::generated::utxos;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::error::PhotonApiError;
use dao::typedefs::hash::Hash;
use dao::typedefs::serializable_pubkey::SerializablePubkey;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetUtxosRequest {
    pub owner: SerializablePubkey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Utxo {
    pub hash: Hash,
    pub account: Option<SerializablePubkey>,
    pub owner: SerializablePubkey,
    pub data: String,
    pub tree: Option<SerializablePubkey>,
    // TODO: Consider making lamports a u64.
    pub lamports: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetUtxosResponse {
    pub total: i64,
    pub items: Vec<Utxo>,
}

fn _parse_model(utxo: utxos::Model) -> Result<Utxo, PhotonApiError> {
    Ok(Utxo {
        hash: utxo.hash.try_into()?,
        account: utxo.account.map(SerializablePubkey::try_from).transpose()?,
        #[allow(deprecated)]
        data: base64::encode(utxo.data),
        owner: utxo.owner.try_into()?,
        tree: utxo.tree.map(|tree| tree.try_into()).transpose()?,
        lamports: utxo.lamports as u64,
    })
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
        total: result.len() as i64,
        items: result
            .into_iter()
            .map(_parse_model)
            .collect::<Result<Vec<Utxo>, PhotonApiError>>()?,
    })
}
