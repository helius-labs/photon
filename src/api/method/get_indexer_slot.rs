use crate::common::typedefs::context::Context;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use sea_orm::DatabaseConnection;

use super::super::error::PhotonApiError;

pub async fn get_indexer_slot(
    conn: &DatabaseConnection,
) -> Result<UnsignedInteger, PhotonApiError> {
    let slot = Context::extract(conn).await?.slot;

    Ok(UnsignedInteger(slot))
}
