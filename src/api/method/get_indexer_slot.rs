use sea_orm::DatabaseConnection;

use crate::common::typedefs::unsigned_integer::UnsignedInteger;

use super::super::error::PhotonApiError;
use super::utils::Context;

pub async fn get_indexer_slot(
    conn: &DatabaseConnection,
) -> Result<UnsignedInteger, PhotonApiError> {
    Ok(UnsignedInteger(Context::extract(conn).await?.slot))
}
