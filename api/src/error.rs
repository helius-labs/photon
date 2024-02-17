use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum PhotonError {
    #[error("Validation Error: {0}")]
    ValidationError(String),
    #[error("Database Error: {0}")]
    DatabaseError(#[from] sea_orm::DbErr),
}

impl From<PhotonError> for RpcError {
    fn from(err: PhotonError) -> Self {
        match err {
            PhotonError::ValidationError(msg) => RpcError::Call(CallError::from_std_error(self)),
            PhotonError::DatabaseError(_) => internal_server_error(),
        }
    }
}

fn internal_server_error() -> RpcError {
    RpcError::Call(CallError::from_std_error(Err("Internal server error")))
}
