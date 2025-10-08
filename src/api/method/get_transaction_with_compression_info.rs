use super::{
    super::error::PhotonApiError, get_multiple_compressed_accounts::fetch_accounts_from_hashes,
};
use crate::common::typedefs::account::AccountV2;
use crate::common::typedefs::account::AccountWithContext;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::token_data::TokenData;
use crate::common::typedefs::{account::Account, serializable_signature::SerializableSignature};
use crate::dao::generated::accounts::Model;
use crate::ingester::parser::parse_transaction;
use crate::ingester::typedefs::block_info::TransactionInfo;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcTransactionConfig;
use solana_client::rpc_request::RpcRequest;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::convert::TryFrom;
use utoipa::{
    openapi::{ObjectBuilder, RefOr, Schema, SchemaType},
    ToSchema,
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
    pub account: Account,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct AccountWithOptionalTokenDataV2 {
    pub account: AccountV2,
    pub optionalTokenData: Option<TokenData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct ClosedAccountWithOptionalTokenDataV2 {
    pub account: ClosedAccountV2,
    pub optionalTokenData: Option<TokenData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct CompressionInfoV2 {
    pub closedAccounts: Vec<ClosedAccountWithOptionalTokenDataV2>,
    pub openedAccounts: Vec<AccountWithOptionalTokenDataV2>,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct GetTransactionResponseV2 {
    pub transaction: EncodedConfirmedTransactionWithStatusMeta,
    pub compressionInfo: CompressionInfoV2,
}

impl<'__s> ToSchema<'__s> for GetTransactionResponseV2 {
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
                .property("compression_info", CompressionInfoV2::schema().1)
                .build(),
        );

        ("GetTransactionResponseV2", RefOr::T(schema))
    }

    fn aliases() -> Vec<(&'static str, Schema)> {
        Vec::new()
    }
}

fn parse_optional_token_data(
    account: Account,
) -> Result<AccountWithOptionalTokenData, PhotonApiError> {
    let hash = account.hash.clone();
    Ok(AccountWithOptionalTokenData {
        optionalTokenData: account.parse_token_data().map_err(|e| {
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

async fn fetch_closed_accounts(
    conn: &DatabaseConnection,
    in_account_hashes: Vec<Hash>,
) -> Result<Vec<AccountWithContext>, PhotonApiError> {
    fetch_accounts_from_hashes(conn, in_account_hashes, true)
        .await?
        .into_iter()
        .map(|x| {
            x.ok_or(PhotonApiError::RecordNotFound(
                "Account not found".to_string(),
            ))
        })
        .collect::<Result<Vec<Model>, PhotonApiError>>()?
        .into_iter()
        .map(TryFrom::try_from)
        .collect::<Result<Vec<AccountWithContext>, PhotonApiError>>()
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

    let tx_info: TransactionInfo = clone_tx(&txn).try_into().map_err(|_e| {
        PhotonApiError::UnexpectedError(format!("Failed to convert transaction {}", signature.0))
    })?;
    let status_update = parse_transaction(conn, &tx_info, slot)
        .await
        .map_err(|_e| {
            PhotonApiError::UnexpectedError(format!("Failed to parse transaction {}", signature.0))
        })?;

    let closed_accounts =
        fetch_closed_accounts(conn, status_update.in_accounts.iter().cloned().collect()).await?;

    let closed_accounts = closed_accounts
        .into_iter()
        .map(|x| x.account)
        .collect::<Vec<Account>>();

    let out_accounts = status_update
        .out_accounts
        .into_iter()
        .map(|x| x.account)
        .collect::<Vec<Account>>();

    Ok(GetTransactionResponse {
        transaction: txn,
        compressionInfo: CompressionInfo {
            closedAccounts: parse_optional_token_data_for_multiple_accounts(closed_accounts)?,
            openedAccounts: parse_optional_token_data_for_multiple_accounts(out_accounts)?,
        },
    })
}

async fn fetch_transaction_from_rpc(
    rpc_client: &RpcClient,
    signature: &SerializableSignature,
) -> Result<EncodedConfirmedTransactionWithStatusMeta, PhotonApiError> {
    rpc_client
        .send(
            RpcRequest::GetTransaction,
            serde_json::json!([signature.0.to_string(), RPC_CONFIG,]),
        )
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to fetch transaction {}: {}",
                signature.0, e
            ))
        })
}

pub async fn get_transaction_with_compression_info(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    request: GetTransactionRequest,
) -> Result<GetTransactionResponse, PhotonApiError> {
    let txn = fetch_transaction_from_rpc(rpc_client, &request.signature).await?;
    get_transaction_helper(conn, request.signature, txn).await
}

fn parse_optional_token_data_v2(
    account: AccountV2,
) -> Result<AccountWithOptionalTokenDataV2, PhotonApiError> {
    let hash = account.hash.clone();
    Ok(AccountWithOptionalTokenDataV2 {
        optionalTokenData: account.parse_token_data().map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to parse token data for account {}: {}",
                hash, e
            ))
        })?,
        account,
    })
}

fn parse_optional_token_data_for_multiple_closed_accounts_v2(
    accounts: Vec<ClosedAccountV2>,
) -> Result<Vec<ClosedAccountWithOptionalTokenDataV2>, PhotonApiError> {
    accounts
        .into_iter()
        .map(parse_optional_token_data_closed_account_v2)
        .collect()
}

fn parse_optional_token_data_closed_account_v2(
    account: ClosedAccountV2,
) -> Result<ClosedAccountWithOptionalTokenDataV2, PhotonApiError> {
    let hash = account.account.hash.clone();
    Ok(ClosedAccountWithOptionalTokenDataV2 {
        optionalTokenData: account.account.parse_token_data().map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to parse token data for account {}: {}",
                hash, e
            ))
        })?,
        account,
    })
}

fn parse_optional_token_data_for_multiple_accounts_v2(
    accounts: Vec<AccountV2>,
) -> Result<Vec<AccountWithOptionalTokenDataV2>, PhotonApiError> {
    accounts
        .into_iter()
        .map(parse_optional_token_data_v2)
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClosedAccountV2 {
    pub account: AccountV2,
    pub nullifier: Hash,
    pub tx_hash: Hash,
}

pub async fn get_transaction_helper_v2(
    conn: &DatabaseConnection,
    signature: SerializableSignature,
    txn: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<GetTransactionResponseV2, PhotonApiError> {
    // Ignore if tx failed or meta is missed
    let meta = txn.transaction.meta.as_ref();
    if meta.is_none() {
        return Err(PhotonApiError::ValidationError(
            "Transaction missing metadata information".to_string(),
        ));
    }
    let slot = txn.slot;

    let tx_info: TransactionInfo = clone_tx(&txn).try_into().map_err(|_e| {
        PhotonApiError::UnexpectedError(format!("Failed to convert transaction {}", signature.0))
    })?;

    let status_update = parse_transaction(conn, &tx_info, slot)
        .await
        .map_err(|_e| {
            PhotonApiError::UnexpectedError(format!("Failed to parse transaction {}", signature.0))
        })?;

    let closed_accounts =
        fetch_closed_accounts(conn, status_update.in_accounts.iter().cloned().collect()).await?;
    let closed_accounts = closed_accounts
        .into_iter()
        .map(|x| -> Result<ClosedAccountV2, PhotonApiError> {
            Ok(ClosedAccountV2 {
                account: AccountV2::from(&x),
                nullifier: x.context.nullifier.unwrap_or_default(),
                tx_hash: x.context.tx_hash.unwrap_or_default(),
            })
        })
        .collect::<Result<Vec<ClosedAccountV2>, PhotonApiError>>()?;

    let out_accounts = status_update
        .out_accounts
        .into_iter()
        .map(|x| AccountV2::from(&x))
        .collect::<Vec<AccountV2>>();

    Ok(GetTransactionResponseV2 {
        transaction: txn,
        compressionInfo: CompressionInfoV2 {
            closedAccounts: parse_optional_token_data_for_multiple_closed_accounts_v2(
                closed_accounts,
            )?,
            openedAccounts: parse_optional_token_data_for_multiple_accounts_v2(out_accounts)?,
        },
    })
}

pub async fn get_transaction_with_compression_info_v2(
    conn: &DatabaseConnection,
    rpc_client: &RpcClient,
    request: GetTransactionRequest,
) -> Result<GetTransactionResponseV2, PhotonApiError> {
    let txn = fetch_transaction_from_rpc(rpc_client, &request.signature).await?;
    get_transaction_helper_v2(conn, request.signature, txn).await
}
