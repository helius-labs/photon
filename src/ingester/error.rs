use crate::api::error::PhotonApiError;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum IngesterError {
    #[error("Persist logic for {event_type} has not yet been implemented")]
    EventNotImplemented { event_type: String },
    #[error("Malformed event: {msg}")]
    MalformedEvent { msg: String },
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Database configuration or data error: {0}")]
    PermanentDatabaseError(String),
    #[error("RPC error ({retryable} retryable): {message}")]
    RpcError { message: String, retryable: bool },
    #[error("Parser error: {0}")]
    ParserError(String),
    #[error("Empty batch event.")]
    EmptyBatchEvent,
    #[error("Invalid event.")]
    InvalidEvent,
}

impl IngesterError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::DatabaseError(_)
                | Self::RpcError {
                    retryable: true,
                    ..
                }
        )
    }
}

impl From<solana_client::client_error::ClientError> for IngesterError {
    fn from(error: solana_client::client_error::ClientError) -> Self {
        Self::RpcError {
            retryable: super::retry::rpc_error_is_retryable(&error),
            message: error.to_string(),
        }
    }
}

impl From<sea_orm::error::DbErr> for IngesterError {
    fn from(err: sea_orm::error::DbErr) -> Self {
        use sea_orm::{DbErr, RuntimeErr};
        // Only known schema/data errors are permanent. Unknown driver failures
        // must remain retryable (e.g. a worker or protocol failure during an outage).
        let permanent = match &err {
            DbErr::Type(_)
            | DbErr::Json(_)
            | DbErr::ConvertFromU64(_)
            | DbErr::TryIntoErr { .. }
            | DbErr::AttrNotSet(_)
            | DbErr::Migration(_) => true,
            DbErr::Conn(RuntimeErr::SqlxError(sqlx::Error::Database(error)))
            | DbErr::Exec(RuntimeErr::SqlxError(sqlx::Error::Database(error)))
            | DbErr::Query(RuntimeErr::SqlxError(sqlx::Error::Database(error))) => {
                if let Some(error) = error.try_downcast_ref::<sqlx::postgres::PgDatabaseError>() {
                    matches!(error.code().get(..2), Some("22" | "23" | "28" | "42"))
                } else if error
                    .try_downcast_ref::<sqlx::sqlite::SqliteError>()
                    .is_some()
                {
                    error
                        .code()
                        .and_then(|code| code.parse::<u32>().ok())
                        .is_some_and(|code| matches!(code & 0xff, 1 | 19 | 20 | 26))
                } else {
                    false
                }
            }
            _ => false,
        };
        if !permanent {
            Self::DatabaseError(err.to_string())
        } else {
            Self::PermanentDatabaseError(err.to_string())
        }
    }
}

impl From<PhotonApiError> for IngesterError {
    fn from(err: PhotonApiError) -> Self {
        match err {
            PhotonApiError::DatabaseError(error) => error.into(),
            error => Self::ParserError(error.to_string()),
        }
    }
}
