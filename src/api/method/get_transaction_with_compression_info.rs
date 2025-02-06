use crate::common::typedefs::serializable_signature::SerializableSignature;
use crate::common::typedefs::token_data::TokenData;
use crate::ingester::parser::parse_transaction;
use crate::ingester::persist::parse_token_data;
use crate::dao::generated::accounts::Model;

use sea_orm::DatabaseConnection;
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
use crate::api::method::utils::parse_account_model_v2;
use crate::common::typedefs::account::AccountV2;
use super::{
    super::error::PhotonApiError, get_multiple_compressed_accounts::fetch_accounts_from_hashes,
};

const RPC_CONFIG: RpcTransactionConfig = RpcTransactionConfig {
    encoding: Some(UiTransactionEncoding::Base64),
    commitment: Some(CommitmentConfig {
        commitment: CommitmentLevel::Confirmed,
    }),
    max_supported_transaction_version: Some(0),
};

// We do not use generics to simply documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetTransactionRequest {
    pub signature: SerializableSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct CompressionInfo {
    pub closedAccounts: Vec<AccountWithOptionalTokenData>,
    pub openedAccounts: Vec<AccountWithOptionalTokenData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct AccountWithOptionalTokenData {
    pub account: AccountV2,
    pub optionalTokenData: Option<TokenData>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct GetTransactionResponse {
    pub transaction: EncodedConfirmedTransactionWithStatusMeta,
    pub compressionInfo: CompressionInfo,
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

    fn aliases() -> Vec<(&'static str, Schema)> {
        Vec::new()
    }
}
fn parse_optional_token_data(
    account: AccountV2,
) -> Result<AccountWithOptionalTokenData, PhotonApiError> {
    let hash = account.hash.clone();
    Ok(AccountWithOptionalTokenData {
        optionalTokenData: parse_token_data(&account).map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to parse token data for account {}: {}",
                hash, e
            ))
        })?,
        account,
    })
}

fn parse_optional_token_data_for_multiple_accounts(
    accounts: Vec<AccountV2>,
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

pub async fn get_transaction_helper(
    conn: &DatabaseConnection,
    signature: SerializableSignature,
    txn: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<GetTransactionResponse, PhotonApiError> {
    // Ignore if tx failed or meta is missed
    let meta = txn.transaction.meta.as_ref();
    if meta.is_none() {
        return Err(PhotonApiError::ValidationError(
            "Transaction missing metadata information".to_string(),
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

    let closed_accounts = fetch_accounts_from_hashes(
        conn,
        status_update.in_accounts.iter().cloned().collect(),
        true,
    )
    .await?
    .into_iter()
    .map(|x| {
        x.ok_or(PhotonApiError::RecordNotFound(
            "Account not found".to_string(),
        ))
    })
    .collect::<Result<Vec<Model>, PhotonApiError>>()?
    .into_iter()
    .map(parse_account_model_v2)
    .collect::<Result<Vec<AccountV2>, PhotonApiError>>()?;

    Ok(GetTransactionResponse {
        transaction: txn,
        compressionInfo: CompressionInfo {
            closedAccounts: parse_optional_token_data_for_multiple_accounts(closed_accounts)?,
            openedAccounts: parse_optional_token_data_for_multiple_accounts(
                status_update.out_accounts,
            )?,
        },
    })
}

pub async fn get_transaction_with_compression_info(
    conn: &DatabaseConnection,
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
    get_transaction_helper(conn, request.signature, txn).await
}
