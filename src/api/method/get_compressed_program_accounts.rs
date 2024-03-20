use crate::dao::generated::utxos;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use super::{
    super::error::PhotonApiError,
    utils::{Context, ResponseWithContext},
};
use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

use super::utils::{parse_utxo_model, Utxo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedProgramAccountsRequest(pub SerializablePubkey);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PaginatedUtxoList {
    // TODO: Add cursor
    pub items: Vec<Utxo>,
}

pub type GetCompressedProgramAccountsResponse = ResponseWithContext<PaginatedUtxoList>;

pub async fn get_compressed_program_accounts(
    conn: &DatabaseConnection,
    request: GetCompressedProgramAccountsRequest,
) -> Result<GetCompressedProgramAccountsResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let owner = request.0;

    let filter = utxos::Column::Owner
        .eq::<Vec<u8>>(owner.into())
        .and(utxos::Column::Spent.eq(false));

    let result = utxos::Entity::find().filter(filter).all(conn).await?;

    Ok(GetCompressedProgramAccountsResponse {
        context,
        value: PaginatedUtxoList {
            items: result
                .into_iter()
                .map(parse_utxo_model)
                .collect::<Result<Vec<Utxo>, PhotonApiError>>()?,
        },
    })
}
