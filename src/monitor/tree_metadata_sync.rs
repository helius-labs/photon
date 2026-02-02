use log::{debug, info, warn};
use sea_orm::{ConnectionTrait, DatabaseConnection, EntityTrait, Set};
use solana_account::Account;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_pubkey::Pubkey;

use crate::api::error::PhotonApiError;
use crate::dao::generated::{prelude::*, tree_metadata};
use crate::ingester::parser::{get_compression_program_id, EXPECTED_TREE_OWNER};
use crate::monitor::v1_tree_accounts::{AddressMerkleTreeAccount, StateMerkleTreeAccount};
use light_batched_merkle_tree::merkle_tree::BatchedMerkleTreeAccount;
use light_compressed_account::TreeType;

/// Tree account data extracted from on-chain account
pub struct TreeAccountData {
    pub queue_pubkey: Pubkey,
    pub root_history_capacity: usize,
    pub height: u32,
    pub sequence_number: u64,
    pub next_index: u64,
    pub owner: Pubkey,
}

fn check_tree_owner(owner: &Pubkey) -> bool {
    match EXPECTED_TREE_OWNER {
        Some(expected_owner) => {
            let owner_bytes = owner.to_bytes();
            let expected_bytes = expected_owner.to_bytes();
            owner_bytes == expected_bytes
        }
        None => true,
    }
}

pub async fn sync_tree_metadata(
    rpc_client: &RpcClient,
    db: &DatabaseConnection,
) -> Result<(), PhotonApiError> {
    info!("Starting tree metadata sync from on-chain...");

    let compression_program = get_compression_program_id();
    let program_id = Pubkey::from(compression_program.to_bytes());
    info!("Fetching all accounts for program: {}", program_id);

    let current_slot = rpc_client.get_slot().await.map_err(|e| {
        PhotonApiError::UnexpectedError(format!("Failed to fetch current slot: {}", e))
    })?;
    info!("Current slot: {}", current_slot);

    let accounts = rpc_client
        .get_program_accounts(&program_id)
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to fetch program accounts: {}", e))
        })?;

    info!("Found {} accounts to process", accounts.len());

    let mut synced_count = 0;
    let mut failed_count = 0;

    for (pubkey, mut account) in accounts {
        match process_tree_account(db, pubkey, &mut account, current_slot).await {
            Ok(true) => synced_count += 1,
            Ok(false) => {} // Not a tree account, skip
            Err(e) => {
                warn!("Failed to process account {}: {}", pubkey, e);
                failed_count += 1;
            }
        }
    }

    info!(
        "Tree metadata sync completed. Synced: {}, Failed: {}",
        synced_count, failed_count
    );

    Ok(())
}

pub async fn process_tree_account(
    db: &DatabaseConnection,
    pubkey: Pubkey,
    account: &mut Account,
    slot: u64,
) -> Result<bool, PhotonApiError> {
    if let Ok(data) = process_v1_state_account(account) {
        if !check_tree_owner(&data.owner) {
            debug!(
                "Skipping V1 state tree {} - owner {} does not match expected owner",
                pubkey, data.owner
            );
            return Ok(false);
        }

        info!(
            "DEBUG: Parsed as V1 state tree: {} (queue={}, owner={})",
            pubkey, data.queue_pubkey, data.owner
        );
        upsert_tree_metadata(db, pubkey, TreeType::StateV1, &data, slot).await?;
        info!(
            "Synced V1 state tree {} with height {}, root_history_capacity {}, seq {}, next_idx {}",
            pubkey, data.height, data.root_history_capacity, data.sequence_number, data.next_index
        );
        return Ok(true);
    }

    if let Ok(data) = process_v1_address_account(account) {
        if !check_tree_owner(&data.owner) {
            debug!(
                "Skipping V1 address tree {} - owner {} does not match expected owner",
                pubkey, data.owner
            );
            return Ok(false);
        }

        upsert_tree_metadata(db, pubkey, TreeType::AddressV1, &data, slot).await?;
        info!("Synced V1 address tree {} with height {}, root_history_capacity {}, seq {}, next_idx {}",
            pubkey, data.height, data.root_history_capacity, data.sequence_number, data.next_index);
        return Ok(true);
    }

    let light_pubkey = light_compressed_account::pubkey::Pubkey::new_from_array(pubkey.to_bytes());
    if let Ok(tree_account) =
        BatchedMerkleTreeAccount::state_from_bytes(&mut account.data.clone(), &light_pubkey)
    {
        let metadata = tree_account.get_metadata();
        let data = TreeAccountData {
            queue_pubkey: Pubkey::new_from_array(metadata.metadata.associated_queue.to_bytes()),
            root_history_capacity: metadata.root_history_capacity as usize,
            height: tree_account.height,
            sequence_number: metadata.sequence_number,
            next_index: metadata.next_index,
            owner: Pubkey::new_from_array(metadata.metadata.access_metadata.owner.to_bytes()),
        };

        if !check_tree_owner(&data.owner) {
            debug!(
                "Skipping V2 state tree {} - owner {} does not match expected owner",
                pubkey, data.owner
            );
            return Ok(false);
        }

        info!(
            "DEBUG: Parsed as V2 state tree: {} (queue={}, owner={})",
            pubkey, data.queue_pubkey, data.owner
        );
        upsert_tree_metadata(db, pubkey, TreeType::StateV2, &data, slot).await?;

        info!(
            "Synced V2 state tree {} with root_history_capacity {}",
            pubkey, data.root_history_capacity
        );
        return Ok(true);
    }

    if let Ok(tree_account) =
        BatchedMerkleTreeAccount::address_from_bytes(&mut account.data.clone(), &light_pubkey)
    {
        let metadata = tree_account.get_metadata();
        let data = TreeAccountData {
            queue_pubkey: pubkey, // For V2 address trees, queue == tree
            root_history_capacity: metadata.root_history_capacity as usize,
            height: tree_account.height,
            sequence_number: metadata.sequence_number,
            next_index: metadata.next_index,
            owner: Pubkey::new_from_array(metadata.metadata.access_metadata.owner.to_bytes()),
        };

        if !check_tree_owner(&data.owner) {
            debug!(
                "Skipping V2 address tree {} - owner {} does not match expected owner",
                pubkey, data.owner
            );
            return Ok(false);
        }

        upsert_tree_metadata(db, pubkey, TreeType::AddressV2, &data, slot).await?;

        info!(
            "Synced V2 address tree {} with root_history_capacity {}",
            pubkey, data.root_history_capacity
        );
        return Ok(true);
    }

    debug!("Account {} is not a recognized tree type", pubkey);
    Ok(false)
}

fn process_v1_state_account(account: &Account) -> Result<TreeAccountData, PhotonApiError> {
    let tree_account = StateMerkleTreeAccount::from_account_bytes(&account.data).map_err(|e| {
        PhotonApiError::UnexpectedError(format!("Failed to deserialize state tree account: {}", e))
    })?;

    let merkle_tree = tree_account.tree().map_err(|e| {
        PhotonApiError::UnexpectedError(format!("Failed to parse concurrent merkle tree: {}", e))
    })?;

    Ok(TreeAccountData {
        queue_pubkey: Pubkey::new_from_array(tree_account.metadata.associated_queue.to_bytes()),
        root_history_capacity: merkle_tree.roots.capacity(),
        height: merkle_tree.height as u32,
        sequence_number: merkle_tree.sequence_number() as u64,
        next_index: merkle_tree.next_index() as u64,
        owner: Pubkey::new_from_array(tree_account.metadata.access_metadata.owner.to_bytes()),
    })
}

fn process_v1_address_account(account: &Account) -> Result<TreeAccountData, PhotonApiError> {
    let tree_account =
        AddressMerkleTreeAccount::from_account_bytes(&account.data).map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to deserialize address tree account: {}",
                e
            ))
        })?;

    let indexed_tree = tree_account.tree().map_err(|e| {
        PhotonApiError::UnexpectedError(format!("Failed to parse indexed merkle tree: {}", e))
    })?;

    Ok(TreeAccountData {
        queue_pubkey: Pubkey::new_from_array(tree_account.metadata.associated_queue.to_bytes()),
        root_history_capacity: indexed_tree.merkle_tree.roots.capacity(),
        height: indexed_tree.merkle_tree.height as u32,
        sequence_number: indexed_tree.merkle_tree.sequence_number() as u64,
        next_index: indexed_tree.merkle_tree.next_index() as u64,
        owner: Pubkey::new_from_array(tree_account.metadata.access_metadata.owner.to_bytes()),
    })
}

pub async fn upsert_tree_metadata<C>(
    db: &C,
    tree_pubkey: Pubkey,
    tree_type: TreeType,
    data: &TreeAccountData,
    slot: u64,
) -> Result<(), PhotonApiError>
where
    C: ConnectionTrait,
{
    let tree_bytes = tree_pubkey.to_bytes().to_vec();

    let model = tree_metadata::ActiveModel {
        tree_pubkey: Set(tree_bytes),
        queue_pubkey: Set(data.queue_pubkey.to_bytes().to_vec()),
        tree_type: Set(tree_type as i32),
        height: Set(data.height as i32),
        root_history_capacity: Set(data.root_history_capacity as i64),
        sequence_number: Set(data.sequence_number as i64),
        next_index: Set(data.next_index as i64),
        last_synced_slot: Set(slot as i64),
    };

    TreeMetadata::insert(model)
        .on_conflict(
            sea_orm::sea_query::OnConflict::column(tree_metadata::Column::TreePubkey)
                .update_columns([
                    tree_metadata::Column::QueuePubkey,
                    tree_metadata::Column::TreeType,
                    tree_metadata::Column::SequenceNumber,
                    tree_metadata::Column::NextIndex,
                    tree_metadata::Column::LastSyncedSlot,
                ])
                .to_owned(),
        )
        .exec(db)
        .await?;

    debug!("Upserted tree metadata for {}", tree_pubkey);

    Ok(())
}
