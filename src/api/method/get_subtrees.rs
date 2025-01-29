use std::sync::Mutex;
use jsonrpsee_core::Serialize;
use lazy_static::lazy_static;
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use utoipa::ToSchema;
use crate::api::error::PhotonApiError;
use crate::api::method::utils::Context;
use crate::common::typedefs::hash::Hash;

lazy_static! {
    pub static ref SUBTREES: Mutex<Vec<Hash>> = Mutex::new(Vec::new());
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetSubtreesRequest {
    pub merkle_tree: Hash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetSubtreesResponse {
    pub context: Context,
    pub value: Vec<Hash>,
}

pub async fn get_subtrees(
    conn: &DatabaseConnection,
    _request: GetSubtreesRequest,
) -> Result<GetSubtreesResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let hash = Hash::new_unique();
    let mut subtrees = SUBTREES.lock().unwrap();
    subtrees.push(hash);
    if subtrees.len() > 10 {
        subtrees.remove(0);
    }

    let response = GetSubtreesResponse {
        context,
        value: subtrees.clone()
    };

    Ok(response)
}
