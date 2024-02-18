use jsonrpsee::core::Error as RpcError;
use jsonrpsee::types::error::CallError;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum PhotonApiError {
    #[error("Validation Error: {0}")]
    ValidationError(String),
    #[error("Database Error: {0}")]
    DatabaseError(#[from] sea_orm::DbErr),
}

// TODO: Simplify error conversions and ensure we adhere
// to the proper RPC and HTTP codes.
impl Into<RpcError> for PhotonApiError {
    fn into(self) -> RpcError {
        match self {
            PhotonApiError::ValidationError(msg) => {
                RpcError::Call(CallError::Failed(anyhow::anyhow!(msg)))
            }
            PhotonApiError::DatabaseError(_) => internal_server_error(),
        }
    }
}

fn internal_server_error() -> RpcError {
    RpcError::Call(CallError::Failed(anyhow::anyhow!("Internal server error")))
}
