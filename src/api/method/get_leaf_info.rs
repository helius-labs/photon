use std::sync::Mutex;
use jsonrpsee_core::Serialize;
use lazy_static::lazy_static;
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use utoipa::ToSchema;
use crate::api::error::PhotonApiError;
use crate::api::method::utils::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::unsigned_integer::UnsignedInteger;


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LeafInfo {
    pub leaf_index: UnsignedInteger,
    pub leaf: Hash,
    pub tx_hash: Hash,
}

lazy_static! {
    pub static ref LEAF_INFOS: Mutex<Vec<LeafInfo>> = Mutex::new(Vec::new());
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetLeafInfoRequest {
    pub merkle_tree: Hash,
    pub zkp_batch_size: UnsignedInteger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetLeafInfoResponse {
    pub context: Context,
    pub value: Vec<LeafInfo>,
}

pub async fn get_leaf_info(
    conn: &DatabaseConnection,
    _request: GetLeafInfoRequest,
) -> Result<GetLeafInfoResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let info = LeafInfo {
        leaf_index: UnsignedInteger(0),
        leaf: Hash::new_unique(),
        tx_hash: Hash::new_unique(),
    };
    let mut infos = LEAF_INFOS.lock().unwrap();
    infos.push(info);
    if infos.len() > 10 {
        infos.remove(0);
    }

    let response = GetLeafInfoResponse {
        context,
        value: infos.clone()
    };

    Ok(response)
}
