use light_batched_merkle_tree::constants::TEST_DEFAULT_ZKP_BATCH_SIZE;
use light_batched_merkle_tree::event::BatchAppendEvent;
use sea_orm::{ConnectionTrait, DatabaseConnection, FromQueryResult, Statement};
use serde::{Deserialize, Serialize};
use solana_program::pubkey::Pubkey;
use utoipa::ToSchema;
use log::info;

use crate::api::error::PhotonApiError;
use crate::api::method::utils::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::ingester::persist::bytes_to_sql_format;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsRequest {
    pub merkle_tree: Hash,
    pub start_offset: UnsignedInteger,
    pub end_offset: UnsignedInteger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsResponse {
    pub context: Context,
    pub values: Vec<Hash>,
    pub first_leaf_index: UnsignedInteger,
}

pub async fn get_queue_elements(
    conn: &DatabaseConnection,
    request: GetQueueElementsRequest,
) -> Result<GetQueueElementsResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let merkle_tree = request.merkle_tree.to_vec();
    let tree_pubkey = Pubkey::try_from(merkle_tree.clone())
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Invalid tree pubkey: {:?}", e)))?;

    info!(
        "Getting queue elements for merkle tree {} from {} to {}",
        tree_pubkey.to_string(),
        request.start_offset.0,
        request.end_offset.0
    );

    let tree_string = bytes_to_sql_format(conn.get_database_backend(), merkle_tree);
    let start_offset = request.start_offset.0;
    let end_offset = request.end_offset.0;

    // Validate offsets
    if start_offset > end_offset {
        return Err(PhotonApiError::ValidationError(
            "start_offset must be less than or equal to end_offset".to_string(),
        ));
    }

    // Query using LIMIT/OFFSET, ordering by queue_position
    let pagination = match conn.get_database_backend() {
        sea_orm::DatabaseBackend::Postgres => format!("OFFSET {start_offset} LIMIT {}", end_offset - start_offset),
        sea_orm::DatabaseBackend::Sqlite => format!("LIMIT {} OFFSET {start_offset}", end_offset - start_offset),
        _ => panic!("Unsupported database backend"),
    };

    let raw_sql = format!(
        "
        SELECT leaf_index, hash
        FROM accounts
        WHERE tree = {tree_string}
        AND in_queue = true
        ORDER BY queue_position ASC
        {pagination}
        "
    );

    let stmt = Statement::from_string(conn.get_database_backend(), raw_sql);

    #[derive(FromQueryResult)]
    struct HashResult {
        hash: Vec<u8>,
    }

    let results: Vec<HashResult> = HashResult::find_by_statement(stmt)
        .all(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error fetching queue elements: {}", e)))?;

    let queue_elements: Vec<Hash> = results
        .into_iter()
        .map(|result| Hash::try_from(result.hash))
        .collect::<Result<Vec<Hash>, _>>()
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Invalid hash in database: {}", e)))?;

    Ok(GetQueueElementsResponse {
        context,
        values: queue_elements,
        first_leaf_index: UnsignedInteger(0), // TODO: leaf_index of first element in queue_elements
    })
}