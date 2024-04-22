// use crate::common::typedefs::account::Account;
// use crate::common::typedefs::bs64_string::Base64String;
// use crate::common::typedefs::hash::Hash;
// use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
// use crate::common::typedefs::serializable_signature::SerializableSignature;
// use crate::dao::generated::accounts;
// use crate::ingester::parser::indexer_events::{
//     CompressedAccount, CompressedAccountData, TokenData,
// };
// use crate::ingester::parser::parse_transaction;

// use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter};
// use serde::{Deserialize, Serialize};
// use solana_client::nonblocking::rpc_client::RpcClient;
// use solana_client::rpc_config::RpcTransactionConfig;
// use solana_client::rpc_request::RpcRequest;
// use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
// use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
// use utoipa::ToSchema;

// use super::super::error::PhotonApiError;
// use super::utils::{
//     parse_account_model, parse_discriminator, AccountDataTable, CompressedAccountRequest, Context,
// };

// const RPC_CONFIG: RpcTransactionConfig = RpcTransactionConfig {
//     encoding: Some(UiTransactionEncoding::Base64),
//     commitment: Some(CommitmentConfig {
//         commitment: CommitmentLevel::Confirmed,
//     }),
//     max_supported_transaction_version: Some(0),
// };

// // We do not use generics to simply documentation generation.
// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// pub struct GetTransactionRequest {
//     pub signature: SerializableSignature,
// }

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// pub struct CompressionInfo {
//     pub closed_accounts: Vec<AccountWithOptionalTokenData>,
//     pub opened_accounts: Vec<AccountWithOptionalTokenData>,
// }

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// pub struct AccountWithOptionalTokenData {
//     pub account: Account,
//     pub optional_token_data: Option<TokenData>,
// }

// pub struct GetTransactionResponse {
//     pub transaction: EncodedConfirmedTransactionWithStatusMeta,
//     pub compression_info: CompressionInfo,
// }

// // fn parsed_enriched_account(account: EnrichedAccount) -> Result<Account, PhotonApiError> {
// //     let EnrichedAccount {
// //         account,
// //         tree,
// //         seq,
// //         hash,
// //         slot,
// //         leaf_index,
// //     } = account;
// //     let CompressedAccount {
// //         owner,
// //         lamports,
// //         address,
// //         data,
// //     } = account;
// //     Ok(Account {
// //         owner: SerializablePubkey::from(owner),
// //         lamports,
// //         address: address.map(SerializablePubkey::from),
// //         #[allow(deprecated)]
// //         data: data.map(|x| Base64String(base64::encode(x.data))),
// //         discriminator: data.and_then(|x| parse_discriminator(x.discriminator.to_vec())),
// //         data_hash: data.map(|data| Hash::from(data.data_hash)),
// //         hash: Hash::from(hash),
// //         tree: SerializablePubkey::from(tree),
// //         leaf_index,
// //         seq,
// //         slot_updated: slot,
// //     })
// // }

// pub async fn get_transaction(
//     conn: &DatabaseConnection,
//     rpc_client: &RpcClient,
//     request: GetTransactionRequest,
// ) -> Result<GetTransactionResponse, PhotonApiError> {
//     let context = Context::extract(conn).await?;
//     let txn: EncodedConfirmedTransactionWithStatusMeta = rpc_client
//         .send(
//             RpcRequest::GetTransaction,
//             serde_json::json!([request.signature.0.to_string(), RPC_CONFIG,]),
//         )
//         .await
//         .map_err(|e| {
//             PhotonApiError::UnexpectedError(format!(
//                 "Failed to fetch transaction {}: {}",
//                 request.signature.0, e
//             ))
//         })?;

//     // Ignore if tx failed or meta is missed
//     let meta = txn.transaction.meta.as_ref();
//     if meta.map(|meta| meta.status.is_err()).unwrap_or(true) {
//         return Err(PhotonApiError::ValidationError(
//             "Transaction missing metatada information".to_string(),
//         ));
//     }
//     let slot = txn.slot;
//     let status_update = parse_transaction(
//         &txn.try_into().map_err(|_e| {
//             PhotonApiError::UnexpectedError(format!(
//                 "Failed to parse transaction {}",
//                 request.signature.0
//             ))
//         })?,
//         slot,
//     )
//     .map_err(|_e| {
//         PhotonApiError::UnexpectedError(format!(
//             "Failed to parse transaction {}",
//             request.signature.0
//         ))
//     })?;

//     // let opened_accounts = status_update
//     //     .in_accounts
//     //     .into_iter()
//     //     .map(parsed_enriched_account)
//     //     .collect::<Result<Vec<Account>, PhotonApiError>>()?;

//     // let closed_accounts = status_update
//     //     .out_accounts
//     //     .into_iter()
//     //     .map(parsed_enriched_account)
//     //     .collect::<Result<Vec<Account>, PhotonApiError>>()?;

//     todo!();
//     // Ok(GetTransactionResponse {
//     //     transaction: txn,
//     //     compression_info: CompressionInfo {
//     //         closed_accounts,
//     //         opened_accounts,
//     //     },
//     // })
// }
