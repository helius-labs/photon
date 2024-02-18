use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    request: GetCompressedAcccountProofRequest,
) -> Result<GetCompressedAcccountProofResponse, InternalApiError> {
    todo!();
}
