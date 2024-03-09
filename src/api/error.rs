use crate::dao::typedefs::hash::ParseHashError;
use jsonrpsee::core::Error as RpcError;
use jsonrpsee::types::error::CallError;
use solana_sdk::pubkey::ParsePubkeyError;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum PhotonApiError {
    #[error("Validation Error: {0}")]
    ValidationError(String),
    #[error("Invalid Public Key: field '{field}'")]
    InvalidPubkey { field: String },
    #[error("Database Error: {0}")]
    DatabaseError(#[from] sea_orm::DbErr),
    #[error("Record Not Found: {0}")]
    RecordNotFound(String),
    #[error("Unexpected Error: {0}")]
    UnexpectedError(String),
}

// TODO: Simplify error conversions and ensure we adhere
// to the proper RPC and HTTP codes.
impl Into<RpcError> for PhotonApiError {
    fn into(self) -> RpcError {
        match self {
            PhotonApiError::ValidationError(_)
            | PhotonApiError::InvalidPubkey { .. }
            | PhotonApiError::RecordNotFound(_) => invalid_request(self),
            PhotonApiError::DatabaseError(_) | PhotonApiError::UnexpectedError(_) => {
                internal_server_error()
            }
        }
    }
}

// The API contract receives parsed input from the user, so if we get a ParseHashError it means
// that the database itself returned an invalid hash.
impl From<ParseHashError> for PhotonApiError {
    fn from(_error: ParseHashError) -> Self {
        PhotonApiError::UnexpectedError("Invalid hash in database".to_string())
    }
}

impl From<ParsePubkeyError> for PhotonApiError {
    fn from(_error: ParsePubkeyError) -> Self {
        PhotonApiError::UnexpectedError("Invalid public key in database".to_string())
    }
}

fn invalid_request(e: PhotonApiError) -> RpcError {
    RpcError::Call(CallError::from_std_error(e))
}

fn internal_server_error() -> RpcError {
    RpcError::Call(CallError::Failed(anyhow::anyhow!("Internal server error")))
}
