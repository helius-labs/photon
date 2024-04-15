// use sea_orm::DatabaseConnection;
// use serde::{Deserialize, Serialize};
// use utoipa::ToSchema;

// use super::{
//     super::error::PhotonApiError,
//     get_multiple_compressed_account_proofs::{
//         get_multiple_compressed_account_proofs_helper, MerkleProofWithContext,
//     },
//     utils::Context,
// };
// use crate::{common::typedefs::hash::Hash, dao::generated::account_transactions};

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
// #[serde(deny_unknown_fields, rename_all = "camelCase")]
// pub struct HashRequest(Hash);

// #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// // We do not use generics to simplify documentation generation.
// pub struct GetCompressedAccountTransactionsResponse {
//     pub context: Context,
//     pub value: MerkleProofWithContext,
// }

// pub async fn get_compressed_account_transactions(
//     conn: &DatabaseConnection,
//     request: HashRequest,
// ) -> Result<GetCompressedAccountTransactionsResponse, PhotonApiError> {
//     let context = Context::extract(conn).await?;
//     let hash = request.0;

//     let transactions = account_transactions::Entity::find()
//         .filter(account_transactions::Column::Hash.contains(hash))
//         .all(conn)
//         .await?;
// }
