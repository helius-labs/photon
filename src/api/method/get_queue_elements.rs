use std::sync::Mutex;
use jsonrpsee_core::Serialize;
use lazy_static::lazy_static;
use log::info;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::Deserialize;
use solana_program::pubkey::Pubkey;
use utoipa::ToSchema;
use crate::api::error::PhotonApiError;
use crate::api::method::utils::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;
use crate::dao::generated::{indexed_trees, state_trees};

lazy_static! {
    pub static ref ADDRESS_QUEUE_ELEMENTS: Mutex<Vec<Hash>> = Mutex::new(Vec::new());
    pub static ref STATE_QUEUE_ELEMENTS: Mutex<Vec<Hash>> = Mutex::new(Vec::new());
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsRequest {
    pub merkle_tree: Hash,
    pub start_offset: UnsignedInteger,
    pub end_offset: UnsignedInteger,
    pub batch: UnsignedInteger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueElementsResponse {
    pub context: Context,
    pub value: Vec<Hash>,
}

pub async fn get_queue_elements(
    conn: &DatabaseConnection,
    request: GetQueueElementsRequest,
) -> Result<GetQueueElementsResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let merkle_tree = request.merkle_tree.to_vec();
    let tree_pubkey = Pubkey::try_from(merkle_tree.clone()).unwrap();
    info!("Getting queue elements for merkle tree {} : {:?}", tree_pubkey.to_string(), merkle_tree);

    println!("Indexed trees:");
    let all_indexed_trees = indexed_trees::Entity::find()
        .all(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error checking indexed trees: {}", e)))?;
    for tree in all_indexed_trees {
        println!("Indexed tree: {:?}", tree.tree);
    }

    println!("State trees:");
    let all_state_trees = state_trees::Entity::find()
        .all(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error checking indexed trees: {}", e)))?;
    for tree in all_state_trees {
        let tree_pubkey = Pubkey::try_from(tree.tree.clone()).unwrap();
        println!("State tree {} : {:?} ", tree_pubkey.to_string(), tree.tree);
    }

    let indexed_exists = indexed_trees::Entity::find()
        .filter(indexed_trees::Column::Tree.eq(merkle_tree.clone()))
        .one(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error checking indexed tree: {}", e)))?
        .is_some();

    let queue_elements = if indexed_exists {
        ADDRESS_QUEUE_ELEMENTS.lock().unwrap().clone()
    } else {
        let state_exists = state_trees::Entity::find()
            .filter(state_trees::Column::Tree.eq(merkle_tree.clone()))
            .one(conn)
            .await
            .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error checking state tree: {}", e)))?
            .is_some();

        if state_exists {
            STATE_QUEUE_ELEMENTS.lock().unwrap().clone()
        } else {
            return Err(PhotonApiError::UnexpectedError("Merkle tree not found".into()));
        }
    };

    Ok(GetQueueElementsResponse {
        context,
        value: queue_elements
    })

}
