use crate::dao::generated::utxos;

use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter};

use super::super::error::PhotonApiError;
use super::utils::{
    parse_utxo_model, AccountDataTable, CompressedAccountRequest, Context, UtxoResponse,
};

pub async fn get_compressed_account(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<UtxoResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let id = request.parse_id()?;
    let utxo = parse_utxo_model(
        utxos::Entity::find()
            .filter(id.filter(AccountDataTable::Utxos))
            .one(conn)
            .await?
            .ok_or(id.not_found_error())?,
    )?;
    Ok(UtxoResponse {
        value: utxo,
        context,
    })
}
