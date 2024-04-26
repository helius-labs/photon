use sea_orm::DatabaseConnection;

use super::super::error::PhotonApiError;
use super::utils::Context;

pub async fn get_indexer_slot(conn: &DatabaseConnection) -> Result<u64, PhotonApiError> {
    Ok(Context::extract(conn).await?.slot)
}
