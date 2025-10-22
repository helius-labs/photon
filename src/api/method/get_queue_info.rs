use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::dao::generated::{accounts, address_queues, tree_metadata};
use light_compressed_account::{QueueType, TreeType};
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetQueueInfoRequest {
    #[serde(default)]
    pub trees: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetQueueInfoResponse {
    pub queues: Vec<QueueInfo>,
    pub slot: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct QueueInfo {
    pub tree: String,
    pub queue: String,
    pub queue_type: u8,
    pub queue_size: u64,
}

async fn fetch_queue_sizes(
    db: &DatabaseConnection,
    tree_filter: Option<Vec<Vec<u8>>>,
) -> Result<HashMap<(Vec<u8>, u8), u64>, PhotonApiError> {
    let mut result = HashMap::new();

    let mut query = tree_metadata::Entity::find().filter(
        tree_metadata::Column::TreeType
            .is_in([TreeType::StateV2 as i32, TreeType::AddressV2 as i32]),
    );

    if let Some(trees) = tree_filter {
        query = query.filter(tree_metadata::Column::TreePubkey.is_in(trees));
    }

    let trees = query
        .all(db)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

    for tree in trees {
        let tree_pubkey = tree.tree_pubkey.clone();

        match tree.tree_type {
            t if t == TreeType::StateV2 as i32 => {
                let nullifier_count = accounts::Entity::find()
                    .filter(accounts::Column::Tree.eq(tree_pubkey.clone()))
                    .filter(accounts::Column::NullifierQueueIndex.is_not_null())
                    .filter(accounts::Column::NullifiedInTree.eq(false))
                    .count(db)
                    .await
                    .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

                result.insert(
                    (tree_pubkey.clone(), QueueType::InputStateV2 as u8),
                    nullifier_count,
                );

                let output_queue_size = accounts::Entity::find()
                    .filter(accounts::Column::Tree.eq(tree_pubkey.clone()))
                    .filter(accounts::Column::InOutputQueue.eq(true))
                    .count(db)
                    .await
                    .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

                result.insert(
                    (tree_pubkey, QueueType::OutputStateV2 as u8),
                    output_queue_size,
                );
            }
            t if t == TreeType::AddressV2 as i32 => {
                let address_count = address_queues::Entity::find()
                    .filter(address_queues::Column::Tree.eq(tree_pubkey.clone()))
                    .count(db)
                    .await
                    .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

                result.insert((tree_pubkey, QueueType::AddressV2 as u8), address_count);
            }
            _ => continue,
        }
    }

    Ok(result)
}


pub async fn get_queue_info(
    db: &DatabaseConnection,
    request: GetQueueInfoRequest,
) -> Result<GetQueueInfoResponse, PhotonApiError> {
    let tree_filter = if let Some(trees) = request.trees {
        let parsed: Result<Vec<Vec<u8>>, _> = trees
            .iter()
            .map(|s| {
                Pubkey::try_from(s.as_str())
                    .map(|p| p.to_bytes().to_vec())
                    .map_err(|e| PhotonApiError::ValidationError(format!("Invalid pubkey: {}", e)))
            })
            .collect();
        Some(parsed?)
    } else {
        None
    };

    // Fetch queue sizes
    let queue_sizes = fetch_queue_sizes(db, tree_filter).await?;

    // Get tree metadata for queue pubkeys
    let tree_pubkeys: Vec<Vec<u8>> = queue_sizes
        .keys()
        .map(|(tree, _)| tree.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    let tree_metadata_list = tree_metadata::Entity::find()
        .filter(tree_metadata::Column::TreePubkey.is_in(tree_pubkeys))
        .all(db)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

    let tree_to_queue: HashMap<Vec<u8>, Vec<u8>> = tree_metadata_list
        .into_iter()
        .map(|t| (t.tree_pubkey, t.queue_pubkey))
        .collect();

    // Build response
    let queues: Vec<QueueInfo> = queue_sizes
        .into_iter()
        .map(|((tree_bytes, queue_type), size)| {
            let queue_bytes = tree_to_queue
                .get(&tree_bytes)
                .cloned()
                .unwrap_or_else(|| vec![0u8; 32]);

            QueueInfo {
                tree: bs58::encode(&tree_bytes).into_string(),
                queue: bs58::encode(&queue_bytes).into_string(),
                queue_type,
                queue_size: size,
            }
        })
        .collect();

    // Get current slot using standard Context
    let slot = Context::extract(db).await?.slot;

    Ok(GetQueueInfoResponse { queues, slot })
}
