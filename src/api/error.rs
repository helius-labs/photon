use crate::common::typedefs::hash::ParseHashError;
use crate::metric;
use cadence_macros::statsd_count;
use jsonrpsee::core::Error as RpcError;
use jsonrpsee::types::error::CallError;
use log::error;
use solana_pubkey::ParsePubkeyError as SolanaPubkeyParseError;
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
    #[error("Node is behind {0} slots")]
    StaleSlot(u64),
}

// TODO: Simplify error conversions and ensure we adhere
// to the proper RPC and HTTP codes.
impl From<PhotonApiError> for RpcError {
    fn from(val: PhotonApiError) -> Self {
        match val {
            PhotonApiError::ValidationError(_) => {
                metric! {
                    statsd_count!("validation_api_error", 1);
                }
                invalid_request(val)
            }
            PhotonApiError::InvalidPubkey { .. } => {
                metric! {
                    statsd_count!("invalid_pubkey_api_error", 1);
                }
                invalid_request(val)
            }
            PhotonApiError::RecordNotFound(_) => {
                metric! {
                    statsd_count!("record_not_found_api_error", 1);
                }
                invalid_request(val)
            }
            PhotonApiError::StaleSlot(_) => {
                metric! {
                    statsd_count!("stale_slot_api_error", 1);
                }
                invalid_request(val)
            }
            PhotonApiError::DatabaseError(e) => {
                error!("Internal server database error: {}", e);
                metric! {
                    statsd_count!("internal_database_api_error", 1);
                }
                internal_server_error()
            }
            PhotonApiError::UnexpectedError(e) => {
                error!("Internal server error: {}", e);
                metric! {
                    statsd_count!("unexpected_api_error", 1);
                }
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

impl From<SolanaPubkeyParseError> for PhotonApiError {
    fn from(_error: SolanaPubkeyParseError) -> Self {
        PhotonApiError::UnexpectedError("Invalid public key in database".to_string())
    }
}

fn invalid_request(e: PhotonApiError) -> RpcError {
    RpcError::Call(CallError::from_std_error(e))
}

fn internal_server_error() -> RpcError {
    RpcError::Call(CallError::Failed(anyhow::anyhow!("Internal server error")))
}
