use dao::generated::utxos;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::error::PhotonApiError;
use dao::typedefs::hash::Hash;

use super::utils::{parse_utxo_model, Utxo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetUtxoRequest {
    pub hash: Hash,
}

pub async fn get_utxo(
    conn: &DatabaseConnection,
    request: GetUtxoRequest,
) -> Result<Utxo, PhotonApiError> {
    let filter = utxos::Column::Hash
        .eq::<Vec<u8>>(request.hash.clone().into())
        .and(utxos::Column::Spent.eq(false));

    parse_utxo_model(
        utxos::Entity::find()
            .filter(filter)
            .one(conn)
            .await?
            .ok_or(PhotonApiError::RecordNotFound(format!(
                "UTXO {} not found",
                request.hash
            )))?,
    )
}
