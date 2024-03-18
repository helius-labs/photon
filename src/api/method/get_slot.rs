use crate::dao::generated::utxos;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};

use super::super::error::PhotonApiError;
use super::utils::{parse_utxo_model, Context, GetCompressedAccountRequest, Utxo};

pub async fn get_slot(conn: &DatabaseConnection) -> Result<u64, PhotonApiError> {
    Ok(Context::extract(conn).await?.slot)
}
