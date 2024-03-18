use crate::dao::generated::utxos;

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};


use super::super::error::PhotonApiError;
use super::utils::{parse_utxo_model, GetCompressedAccountRequest, Utxo};


pub async fn get_compressed_account(
    conn: &DatabaseConnection,
    request: GetCompressedAccountRequest,
) -> Result<Utxo, PhotonApiError> {
    let GetCompressedAccountRequest { address } = request;
    parse_utxo_model(
        utxos::Entity::find()
            .filter(utxos::Column::Account.eq::<Vec<u8>>(address.clone().into()))
            .one(conn)
            .await?
            .ok_or(PhotonApiError::RecordNotFound(format!(
                "Account {} not found",
                address
            )))?,
    )
}
