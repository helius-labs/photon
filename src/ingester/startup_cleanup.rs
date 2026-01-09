use crate::dao::generated::{address_queues, indexed_trees, tree_metadata};
use light_compressed_account::TreeType;
use log::{debug, info};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
};

/// Cleans up stale address queue entries on startup.
///
/// For each AddressV2 tree:
/// 1. Get the current next_index from indexed_trees (MAX(leaf_index) + 1)
/// 2. Delete address_queue entries where queue_index < next_index - 1
///
/// This handles cases where photon was restarted or re-indexed and stale
/// queue entries remain from batches that were already processed.
pub async fn cleanup_stale_address_queues(
    db: &DatabaseConnection,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting address queue cleanup...");

    let address_trees = tree_metadata::Entity::find()
        .filter(tree_metadata::Column::TreeType.eq(TreeType::AddressV2 as i32))
        .all(db)
        .await?;

    if address_trees.is_empty() {
        debug!("No AddressV2 trees found, skipping cleanup");
        return Ok(());
    }

    let mut total_deleted = 0u64;

    for tree in address_trees {
        let tree_pubkey = &tree.tree_pubkey;

        // Get current next_index from indexed_trees (MAX(leaf_index) + 1)
        let current_next_index = indexed_trees::Entity::find()
            .filter(indexed_trees::Column::Tree.eq(tree_pubkey.clone()))
            .order_by_desc(indexed_trees::Column::LeafIndex)
            .one(db)
            .await?
            .map(|t| (t.leaf_index + 1) as i64)
            .unwrap_or(1);

        // Address queue indices are 0-based, tree indices are 1-based
        // So queue entries with queue_index < current_next_index - 1 are stale
        let queue_threshold = current_next_index - 1;

        if queue_threshold <= 0 {
            debug!(
                "Tree {}: next_index={}, no cleanup needed",
                bs58::encode(tree_pubkey).into_string(),
                current_next_index
            );
            continue;
        }

        let stale_count = address_queues::Entity::find()
            .filter(
                address_queues::Column::Tree
                    .eq(tree_pubkey.clone())
                    .and(address_queues::Column::QueueIndex.lt(queue_threshold)),
            )
            .count(db)
            .await?;

        if stale_count == 0 {
            debug!(
                "Tree {}: next_index={}, no stale entries",
                bs58::encode(tree_pubkey).into_string(),
                current_next_index
            );
            continue;
        }

        let delete_result = address_queues::Entity::delete_many()
            .filter(
                address_queues::Column::Tree
                    .eq(tree_pubkey.clone())
                    .and(address_queues::Column::QueueIndex.lt(queue_threshold)),
            )
            .exec(db)
            .await?;

        let deleted = delete_result.rows_affected;
        total_deleted += deleted;

        info!(
            "Tree {}: deleted {} stale queue entries (queue_index < {}, next_index={})",
            bs58::encode(tree_pubkey).into_string(),
            deleted,
            queue_threshold,
            current_next_index
        );
    }

    if total_deleted > 0 {
        info!(
            "Address queue cleanup complete: deleted {} total stale entries",
            total_deleted
        );
    } else {
        info!("Address queue cleanup complete: no stale entries found");
    }

    let duplicate_deleted = cleanup_duplicate_addresses(db).await?;
    if duplicate_deleted > 0 {
        info!(
            "Duplicate address cleanup: deleted {} addresses already in indexed_trees",
            duplicate_deleted
        );
    }

    Ok(())
}

/// Cleans up address_queues entries where the address already exists in indexed_trees
async fn cleanup_duplicate_addresses(
    db: &DatabaseConnection,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    info!("Checking for duplicate addresses in queue...");

    let queue_entries = address_queues::Entity::find().all(db).await?;

    if queue_entries.is_empty() {
        return Ok(0);
    }

    let queue_addresses: Vec<Vec<u8>> = queue_entries.iter().map(|e| e.address.clone()).collect();

    let existing_entries = indexed_trees::Entity::find()
        .filter(indexed_trees::Column::Value.is_in(queue_addresses))
        .all(db)
        .await?;

    if existing_entries.is_empty() {
        return Ok(0);
    }

    let existing_addresses: Vec<Vec<u8>> =
        existing_entries.iter().map(|e| e.value.clone()).collect();

    let result = address_queues::Entity::delete_many()
        .filter(address_queues::Column::Address.is_in(existing_addresses))
        .exec(db)
        .await?;

    Ok(result.rows_affected)
}
