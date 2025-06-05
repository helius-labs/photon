use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum IngesterError {
    #[error("Persist logic for {event_type} has not yet been implemented")]
    EventNotImplemented { event_type: String },
    #[error("Malformed event: {msg}")]
    MalformedEvent { msg: String },
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Parser error: {0}")]
    ParserError(String),
    #[error("Empty batch event.")]
    EmptyBatchEvent,
    #[error("Invalid event.")]
    InvalidEvent,
}

impl From<sea_orm::error::DbErr> for IngesterError {
    fn from(err: sea_orm::error::DbErr) -> Self {
        IngesterError::DatabaseError(format!("DatabaseError: {}", err))
    }
}
