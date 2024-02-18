use schemars::JsonSchema;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};

use crate::error::PhotonApiError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountRequest {
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountResponse {
    pub hash: String,
}

pub fn get_compressed_account(
    conn: &DatabaseConnection,
    request: GetCompressedAccountRequest,
) -> Result<GetCompressedAccountResponse, PhotonApiError> {
    todo!();
}
