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
pub struct ProofOfLeaf {
    pub leaf: Hash,
    pub proof: Vec<Hash>,
}

lazy_static! {
    pub static ref PROOFS: Mutex<Vec<ProofOfLeaf>> = Mutex::new(Vec::new());
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetProofsByIndicesRequest {
    pub merkle_tree: Hash,
    pub indices: Vec<UnsignedInteger>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetProofsByIndicesResponse {
    pub context: Context,
    pub value: Vec<ProofOfLeaf>,
}

pub async fn get_proofs_by_indices(
    conn: &DatabaseConnection,
    _request: GetProofsByIndicesRequest,
) -> Result<GetProofsByIndicesResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let proof = ProofOfLeaf {
        leaf: Hash::new_unique(),
        proof: vec![Hash::new_unique()],
    };
    let mut proofs = PROOFS.lock().unwrap();
    proofs.push(proof);
    if proofs.len() > 10 {
        proofs.remove(0);
    }

    let response = GetProofsByIndicesResponse {
        context,
        value: proofs.clone()
    };

    Ok(response)
}
