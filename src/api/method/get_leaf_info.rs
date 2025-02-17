use std::collections::HashMap;
use std::sync::Mutex;
use jsonrpsee_core::Serialize;
use lazy_static::lazy_static;
use log::info;
use sea_orm::{ConnectionTrait, DatabaseConnection, FromQueryResult, Statement};
use serde::{Deserialize, Serializer};
use solana_program::pubkey::Pubkey;
use utoipa::ToSchema;
use crate::api::error::PhotonApiError;
use crate::api::method::get_queue_elements::{GetQueueElementsResponse, QueueElement};
use crate::api::method::utils::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::ingester::persist::bytes_to_sql_format;

#[derive(FromQueryResult, Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LeafInfo {
    pub leaf_index: i64,
    #[serde(serialize_with = "serialize_as_base58")]
    pub leaf: Vec<u8>,
    #[serde(serialize_with = "serialize_as_base58")]
    pub tx_hash: Vec<u8>,
}

fn serialize_as_base58<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let hash = Hash::new(bytes).unwrap();
    serializer.serialize_str(&hash.to_base58())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LeafInfoResponse {
    pub leaf_index: u32,
    pub leaf: Hash,
    pub tx_hash: Hash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LeafInfoList {
    pub items: Vec<LeafInfoResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetLeafInfoRequest {
    pub merkle_tree: Hash,
    pub start_offset: UnsignedInteger,
    pub end_offset: UnsignedInteger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetLeafInfoResponse {
    pub context: Context,
    pub value: LeafInfoList,
}

pub async fn get_leaf_info(
    conn: &DatabaseConnection,
    request: GetLeafInfoRequest,
) -> Result<GetLeafInfoResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let merkle_tree = request.merkle_tree.to_vec();
    let tree_pubkey = Pubkey::try_from(merkle_tree.clone())
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Invalid tree pubkey: {:?}", e)))?;

    info!(
        "get_leaf_info for merkle tree {} from {} to {}",
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

    let raw_sql = format!(
        "
        SELECT leaf_index, hash as leaf, tx_hash
        FROM accounts
        WHERE nullifier_queue_index >= {start_offset}
            AND nullifier_queue_index < {end_offset}
            AND tree = {tree_string}
        ORDER BY queue_position ASC
        "
    );

    println!("raw_sql: {}", raw_sql);

    let stmt = Statement::from_string(conn.get_database_backend(), raw_sql);

    let result: Vec<LeafInfo> = LeafInfo::find_by_statement(stmt)
        .all(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error fetching queue elements: {}", e)))?;

    let leaf_info_responses: Vec<LeafInfoResponse> = result
        .into_iter()
        .map(|info| LeafInfoResponse {
            leaf_index: info.leaf_index as u32,
            leaf: Hash::new(&info.leaf).unwrap(),
            tx_hash: Hash::new(&info.tx_hash).unwrap(),
        })
        .collect();

    Ok(GetLeafInfoResponse {
        context,
        value: LeafInfoList {
            items: leaf_info_responses
        }
    })
}