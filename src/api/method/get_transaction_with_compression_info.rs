use crate::common::typedefs::account::Account;
use crate::common::typedefs::serializable_signature::SerializableSignature;
use crate::common::typedefs::token_data::TokenData;
use crate::ingester::parser::parse_transaction;
use crate::ingester::persist::parse_token_data;

use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_client::rpc_request::RpcRequest;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use utoipa::{
    openapi::{ObjectBuilder, RefOr, Schema, SchemaType},
    ToSchema,
};

use super::super::error::PhotonApiError;

const RPC_CONFIG: RpcTransactionConfig = RpcTransactionConfig {
    encoding: Some(UiTransactionEncoding::Base64),
    commitment: Some(CommitmentConfig {
        commitment: CommitmentLevel::Confirmed,
    }),
    max_supported_transaction_version: Some(0),
};

// We do not use generics to simply documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GetTransactionRequest {
    pub signature: SerializableSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct CompressionInfo {
    pub closed_accounts: Vec<AccountWithOptionalTokenData>,
    pub opened_accounts: Vec<AccountWithOptionalTokenData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct AccountWithOptionalTokenData {
    pub account: Account,
    pub optional_token_data: Option<TokenData>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct GetTransactionResponse {
    pub transaction: EncodedConfirmedTransactionWithStatusMeta,
    pub compression_info: CompressionInfo,
}

impl<'__s> ToSchema<'__s> for GetTransactionResponse {
    fn schema() -> (&'__s str, RefOr<Schema>) {
        let schema = Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Object)
                .description(Some(
                    "A Solana transaction with additional compression information",
                ))
                // TODO: Improve OpenAPI documentation here.
                .property(
                    "transaction",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Object)
                        .description(Some(
                            "An encoded confirmed transaction with status meta".to_string(),
                        ))
                        .build(),
                )
                .property("compression_info", CompressionInfo::schema().1)
                .build(),
        );

        ("GetTransactionResponse", RefOr::T(schema))
    }

    fn aliases() -> Vec<(&'static str, utoipa::openapi::schema::Schema)> {
        Vec::new()
    }
}
fn parse_optional_token_data(
    account: Account,
) -> Result<AccountWithOptionalTokenData, PhotonApiError> {
    let hash = account.hash.clone();
    Ok(AccountWithOptionalTokenData {
        optional_token_data: parse_token_data(&account).map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to parse token data for account {}: {}",
                hash, e
            ))
        })?,
        account,
    })
}

fn parse_optional_token_data_for_multiple_accounts(
    accounts: Vec<Account>,
) -> Result<Vec<AccountWithOptionalTokenData>, PhotonApiError> {
    accounts
        .into_iter()
        .map(parse_optional_token_data)
        .collect()
}

fn clone_tx(
    txn: &EncodedConfirmedTransactionWithStatusMeta,
) -> EncodedConfirmedTransactionWithStatusMeta {
    EncodedConfirmedTransactionWithStatusMeta {
        slot: txn.slot,
        transaction: txn.transaction.clone(),
        block_time: txn.block_time,
    }
}

pub fn get_transaction_helper(
    signature: SerializableSignature,
    txn: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<GetTransactionResponse, PhotonApiError> {
    // Ignore if tx failed or meta is missed
    let meta = txn.transaction.meta.as_ref();
    if meta.map(|meta| meta.status.is_err()).unwrap_or(true) {
        return Err(PhotonApiError::ValidationError(
            "Transaction missing metatada information".to_string(),
        ));
    }
    let slot = txn.slot;

    let status_update = parse_transaction(
        &clone_tx(&txn).try_into().map_err(|_e| {
            PhotonApiError::UnexpectedError(format!("Failed to parse transaction {}", signature.0))
        })?,
        slot,
    )
    .map_err(|_e| {
        PhotonApiError::UnexpectedError(format!("Failed to parse transaction {}", signature.0))
    })?;

    Ok(GetTransactionResponse {
        transaction: txn,
        compression_info: CompressionInfo {
            closed_accounts: parse_optional_token_data_for_multiple_accounts(
                status_update.in_accounts,
            )?,
            opened_accounts: parse_optional_token_data_for_multiple_accounts(
                status_update.out_accounts,
            )?,
        },
    })
}

pub async fn get_transaction_with_compression_info(
    rpc_client: &RpcClient,
    request: GetTransactionRequest,
) -> Result<GetTransactionResponse, PhotonApiError> {
    let txn: EncodedConfirmedTransactionWithStatusMeta = rpc_client
        .send(
            RpcRequest::GetTransaction,
            serde_json::json!([request.signature.0.to_string(), RPC_CONFIG,]),
        )
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to fetch transaction {}: {}",
                request.signature.0, e
            ))
        })?;
    get_transaction_helper(request.signature, txn)
}
