// use crate::{api::method::utils::AccountIdentifier, dao::generated::utxos};
// use schemars::JsonSchema;
// use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
// use serde::{Deserialize, Serialize};

// use super::{
//     super::error::PhotonApiError,
//     utils::{Context, ResponseWithContext},
// };
// use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

// use super::utils::{parse_utxo_model, Utxo};

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
// #[serde(deny_unknown_fields, rename_all = "camelCase")]
// pub struct GetMultipleCompressedAccounts {
//     pub hashes: Option<Vec<Hashes>>,
//     pub addresses: Option<Vec<SerializablePubkey>>,
// }

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
// #[serde(deny_unknown_fields, rename_all = "camelCase")]
// pub struct UtxoList {
//     // TODO: Add cursor
//     pub items: Vec<Utxo>,
// }

// pub type GetCompressedAccountsResponse = ResponseWithContext<UtxoList>;

// pub async fn get_multiple_compressed_accounts(
//     conn: &DatabaseConnection,
//     request: GetMultipleCompressedAccounts,
// ) -> Result<GetCompressedAccountsResponse, PhotonApiError> {
//     let context = Context::extract(conn).await?;

//     use std::collections::HashSet;

//     let (filter, identifier_type) = match (&request.hashes, &request.addresses) {
//         (Some(hashes), None) => (
//             utxos::Column::Hash.is_in(hashes.iter().cloned().map(Into::into).collect::<Vec<_>>()),
//             AccountIdentifier::Hash(()),
//         ),
//         (None, Some(addresses)) => (
//             utxos::Column::Account.is_in(
//                 addresses
//                     .iter()
//                     .cloned()
//                     .map(Into::into)
//                     .collect::<Vec<_>>(),
//             ),
//             addresses
//                 .iter()
//                 .map(|a| a.to_string())
//                 .collect::<HashSet<_>>(),
//         ),
//         _ => {
//             return Err(PhotonApiError::ValidationError(
//                 "Either hashes or addresses must be provided".to_string(),
//             ))
//         }
//     };

//     let found_accounts = utxos::Entity::find()
//         .filter(filter.and(utxos::Column::Spent.eq(false)))
//         .all(conn)
//         .await?
//         .into_iter()
//         .map(|utxo| match account_type {
//             "hashes" => utxo.hash.to_string(),
//             "addresses" => utxo.account.unwrap_or_default().to_string(),
//             _ => unreachable!(),
//         })
//         .collect::<HashSet<_>>();

//     let not_found_accounts: Vec<_> = input_accounts
//         .difference(&found_accounts)
//         .cloned()
//         .collect();

//     if !not_found_accounts.is_empty() {
//         return Err(PhotonApiError::RecordNotFound(format!(
//             "Some accounts were not found: {:?}",
//             not_found_accounts
//         )));
//     }

//     Ok(GetCompressedAccountsResponse {
//         context,
//         value: UtxoList {
//             items: result
//                 .into_iter()
//                 .map(parse_utxo_model)
//                 .collect::<Result<Vec<Utxo>, PhotonApiError>>()?,
//         },
//     })
// }
