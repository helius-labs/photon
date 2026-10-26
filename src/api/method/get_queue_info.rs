use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::dao::generated::tree_metadata;
use light_compressed_account::{QueueType, TreeType};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter, Statement, Value,
};
use std::collections::HashMap;

const MAX_TREE_PARAMETERS: usize = 500;

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
    trees: &[tree_metadata::Model],
) -> Result<HashMap<(Vec<u8>, u8), u64>, PhotonApiError> {
    let mut result = HashMap::new();
    // Reuse parameters across the two state-queue arms, keeping each statement below
    // SQLite's portable parameter limit even when no explicit tree filter was supplied.
    for chunk in trees.chunks(MAX_TREE_PARAMETERS) {
        let mut values: Vec<Value> = Vec::with_capacity(chunk.len());
        let mut state_placeholders = Vec::new();
        let mut address_placeholders = Vec::new();
        let state_trees = chunk
            .iter()
            .filter(|tree| tree.tree_type == TreeType::StateV2 as i32);
        let address_trees = chunk
            .iter()
            .filter(|tree| tree.tree_type == TreeType::AddressV2 as i32);
        // SQLite binds named parameters by their first appearance in the UNION arms.
        for tree in state_trees.chain(address_trees) {
            values.push(tree.tree_pubkey.clone().into());
            let placeholder = format!("${}", values.len());
            match tree.tree_type {
                t if t == TreeType::StateV2 as i32 => state_placeholders.push(placeholder),
                t if t == TreeType::AddressV2 as i32 => address_placeholders.push(placeholder),
                _ => continue,
            }
        }

        let mut queries = Vec::new();
        if !state_placeholders.is_empty() {
            let trees = state_placeholders.join(", ");
            queries.push(format!(
                "SELECT tree, CAST({} AS BIGINT) AS queue_type, COUNT(*) AS queue_size \
                 FROM accounts WHERE tree IN ({trees}) \
                 AND nullifier_queue_index IS NOT NULL AND nullified_in_tree = FALSE GROUP BY tree",
                QueueType::InputStateV2 as u8,
            ));
            queries.push(format!(
                "SELECT tree, CAST({} AS BIGINT) AS queue_type, COUNT(*) AS queue_size \
                 FROM accounts WHERE tree IN ({trees}) AND in_output_queue = TRUE GROUP BY tree",
                QueueType::OutputStateV2 as u8,
            ));
        }
        if !address_placeholders.is_empty() {
            let trees = address_placeholders.join(", ");
            queries.push(format!(
                "SELECT tree, CAST({} AS BIGINT) AS queue_type, COUNT(*) AS queue_size \
                 FROM address_queues WHERE tree IN ({trees}) GROUP BY tree",
                QueueType::AddressV2 as u8,
            ));
        }

        for row in db
            .query_all(Statement::from_sql_and_values(
                db.get_database_backend(),
                &queries.join(" UNION ALL "),
                values,
            ))
            .await
            .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?
        {
            let tree: Vec<u8> = row.try_get("", "tree")?;
            let queue_type: i64 = row.try_get("", "queue_type")?;
            let queue_size: i64 = row.try_get("", "queue_size")?;
            result.insert((tree, queue_type as u8), queue_size as u64);
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
    let mut queue_sizes = fetch_queue_sizes(db, &trees).await?;

    let mut queues = Vec::new();
    for tree in trees {
        let queue_types: &[QueueType] = if tree.tree_type == TreeType::StateV2 as i32 {
            &[QueueType::InputStateV2, QueueType::OutputStateV2]
        } else {
            &[QueueType::AddressV2]
        };
        for queue_type in queue_types {
            let queue_type = *queue_type as u8;
            let size = queue_sizes
                .remove(&(tree.tree_pubkey.clone(), queue_type))
                .unwrap_or(0);
            queues.push(QueueInfo {
                tree: bs58::encode(&tree.tree_pubkey).into_string(),
                queue: bs58::encode(&tree.queue_pubkey).into_string(),
                queue_type,
                queue_size: size,
            });
        }
    }

    let slot = Context::extract(db).await?.slot;

    Ok(GetQueueInfoResponse { queues, slot })
}
