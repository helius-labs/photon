use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::dao::generated::accounts;
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, FromQueryResult, QueryFilter,
    QueryOrder, QuerySelect,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

const MAX_QUEUE_ELEMENTS: u16 = 30_000;

/// Parameters for requesting input queue leaf indices.
/// Returns (hash, queue_index, leaf_index) for nullifier queue items.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueLeafIndicesRequest {
    pub tree: Hash,
    pub limit: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_index: Option<u64>,
}

/// A lightweight queue leaf index entry
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct QueueLeafIndex {
    pub hash: Hash,
    pub queue_index: u64,
    pub leaf_index: u64,
}

/// Response containing queue leaf indices
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueLeafIndicesResponse {
    pub context: Context,
    pub value: Vec<QueueLeafIndex>,
}

#[derive(FromQueryResult, Debug)]
struct QueueLeafIndexModel {
    hash: Vec<u8>,
    nullifier_queue_index: i64,
    leaf_index: i64,
}

pub async fn get_queue_leaf_indices(
    conn: &DatabaseConnection,
    request: GetQueueLeafIndicesRequest,
) -> Result<GetQueueLeafIndicesResponse, PhotonApiError> {
    if request.limit > MAX_QUEUE_ELEMENTS {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many queue elements requested {}. Maximum allowed: {}",
            request.limit, MAX_QUEUE_ELEMENTS
        )));
    }

    let context = Context::extract(conn).await?;

    let mut query_condition = Condition::all()
        .add(accounts::Column::Tree.eq(request.tree.to_vec()))
        .add(accounts::Column::NullifierQueueIndex.is_not_null())
        .add(accounts::Column::NullifiedInTree.eq(false))
        .add(accounts::Column::Spent.eq(true));

    if let Some(start_queue_index) = request.start_index {
        query_condition = query_condition
            .add(accounts::Column::NullifierQueueIndex.gte(start_queue_index as i64));
    }

    let queue_elements: Vec<QueueLeafIndexModel> = accounts::Entity::find()
        .filter(query_condition)
        .order_by_asc(accounts::Column::NullifierQueueIndex)
        .limit(request.limit as u64)
        .into_model::<QueueLeafIndexModel>()
        .all(conn)
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("DB error fetching queue leaf indices: {}", e))
        })?;

    let value = queue_elements
        .into_iter()
        .map(|e| {
            Ok(QueueLeafIndex {
                hash: Hash::new(e.hash.as_slice()).map_err(|err| {
                    PhotonApiError::UnexpectedError(format!(
                        "Invalid hash for queue element at queue_index {}: {}",
                        e.nullifier_queue_index, err
                    ))
                })?,
                queue_index: e.nullifier_queue_index as u64,
                leaf_index: e.leaf_index as u64,
            })
        })
        .collect::<Result<Vec<QueueLeafIndex>, PhotonApiError>>()?;

    Ok(GetQueueLeafIndicesResponse { context, value })
}
