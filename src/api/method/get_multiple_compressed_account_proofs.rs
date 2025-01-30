use crate::ingester::persist::persisted_state_tree::{
    get_multiple_compressed_leaf_proofs, MerkleProofWithContext,
};

use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};
use serde::{Deserialize, Serialize};
use utoipa::openapi::{RefOr, Schema};
use utoipa::ToSchema;
use super::{
    super::error::PhotonApiError,
    utils::{Context, PAGE_LIMIT},
};
use crate::common::typedefs::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleCompressedAccountProofsRequest {
    #[serde(default)]
    pub hashes: Option<Vec<Hash>>,
    #[serde(default)]
    pub indices: Option<Vec<u64>>,
}

impl GetMultipleCompressedAccountProofsRequest {
    pub fn adjusted_schema() -> RefOr<Schema> {
        let mut schema = GetMultipleCompressedAccountProofsRequest::schema().1;
        let object = match schema {
            RefOr::T(Schema::Object(ref mut object)) => {
                let example = serde_json::to_value(GetMultipleCompressedAccountProofsRequest {
                    hashes: Some(vec![Hash::new_unique(), Hash::new_unique()]),
                    indices: None,
                })
                    .unwrap();
                object.default = Some(example.clone());
                object.example = Some(example);
                object.description = Some("Request for compressed account proof data".to_string());
                object.clone()
            }
            _ => unimplemented!(),
        };
        RefOr::T(Schema::Object(object))
    }
}


// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleCompressedAccountProofsResponse {
    pub context: Context,
    pub value: Vec<MerkleProofWithContext>,
}

pub async fn get_multiple_compressed_account_proofs(
    conn: &DatabaseConnection,
    request: GetMultipleCompressedAccountProofsRequest,
) -> Result<GetMultipleCompressedAccountProofsResponse, PhotonApiError> {
    let hashes = request.hashes;
    let indices = request.indices;

    if hashes.is_none() && indices.is_none() {
        return Err(PhotonApiError::ValidationError(
            "No hashes or indices provided".to_string(),
        ));
    }

    if hashes.is_some() && indices.is_some() {
        return Err(PhotonApiError::ValidationError(
            "Provide either hashes or indices, not both".to_string(),
        ));
    }

    if let Some(indices) = indices.as_ref() {
        if indices.len() > PAGE_LIMIT as usize {
            return Err(PhotonApiError::ValidationError(format!(
                "Too many indices requested {}. Maximum allowed: {}",
                indices.len(),
                PAGE_LIMIT
            )));
        }
    }

    if let Some(hashes) = hashes.as_ref() {
        if hashes.len() > PAGE_LIMIT as usize {
            return Err(PhotonApiError::ValidationError(format!(
                "Too many hashes requested {}. Maximum allowed: {}",
                hashes.len(),
                PAGE_LIMIT
            )));
        }
    }

    let context = Context::extract(conn).await?;
    let tx = conn.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }
    let proofs = get_multiple_compressed_leaf_proofs(&tx, hashes, indices).await?;
    tx.commit().await?;
    Ok(GetMultipleCompressedAccountProofsResponse {
        value: proofs,
        context,
    })
}
