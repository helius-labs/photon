use borsh::BorshDeserialize;
use log::{debug, info, warn};
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;

use crate::api::error::PhotonApiError;
use crate::dao::generated::{prelude::*, tree_metadata};
use crate::ingester::parser::get_compression_program_id;
use account_compression::utils::check_discriminator::check_discriminator;
use account_compression::{AddressMerkleTreeAccount, StateMerkleTreeAccount};
use light_batched_merkle_tree::merkle_tree::BatchedMerkleTreeAccount;
use light_compressed_account::TreeType;
use light_concurrent_merkle_tree::light_hasher::Poseidon;
use light_concurrent_merkle_tree::zero_copy::ConcurrentMerkleTreeZeroCopy;
use light_indexed_merkle_tree::zero_copy::IndexedMerkleTreeZeroCopy;
use std::mem;

pub async fn sync_tree_metadata(
    rpc_client: &RpcClient,
    db: &DatabaseConnection,
) -> Result<(), PhotonApiError> {
    info!("Starting tree metadata sync from on-chain...");

    let compression_program = get_compression_program_id();
    let program_id = Pubkey::from(compression_program.to_bytes());
    info!("Fetching all accounts for program: {}", program_id);

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
        match process_tree_account(db, pubkey, &mut account).await {
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
) -> Result<bool, PhotonApiError> {
    if let Ok((queue_pubkey, root_history_capacity, height, sequence_number, next_index)) =
        process_v1_state_account(account)
    {
        upsert_tree_metadata(
            db,
            pubkey,
            root_history_capacity as i64,
            height as i32,
            TreeType::StateV1 as i32,
            sequence_number,
            next_index,
            queue_pubkey,
        )
        .await?;
        info!(
            "Synced V1 state tree {} with height {}, root_history_capacity {}, seq {}, next_idx {}",
            pubkey, height, root_history_capacity, sequence_number, next_index
        );
        return Ok(true);
    }

    if let Ok((queue_pubkey, root_history_capacity, height, sequence_number, next_index)) =
        process_v1_address_account(account)
    {
        upsert_tree_metadata(
            db,
            pubkey,
            root_history_capacity as i64,
            height as i32,
            TreeType::AddressV1 as i32,
            sequence_number,
            next_index,
            queue_pubkey,
        )
        .await?;
        info!("Synced V1 address tree {} with height {}, root_history_capacity {}, seq {}, next_idx {}",
            pubkey, height, root_history_capacity, sequence_number, next_index);
        return Ok(true);
    }

    let light_pubkey = light_compressed_account::pubkey::Pubkey::new_from_array(pubkey.to_bytes());
    if let Ok(tree_account) =
        BatchedMerkleTreeAccount::state_from_bytes(&mut account.data.clone(), &light_pubkey)
    {
        let metadata = tree_account.get_metadata();

        upsert_tree_metadata(
            db,
            pubkey,
            metadata.root_history_capacity as i64,
            tree_account.height as i32,
            TreeType::StateV2 as i32,
            metadata.sequence_number,
            metadata.next_index,
            Pubkey::new_from_array(metadata.metadata.associated_queue.to_bytes()),
        )
        .await?;

        info!(
            "Synced V2 state tree {} with root_history_capacity {}",
            pubkey, metadata.root_history_capacity
        );
        return Ok(true);
    }

    if let Ok(tree_account) =
        BatchedMerkleTreeAccount::address_from_bytes(&mut account.data.clone(), &light_pubkey)
    {
        let metadata = tree_account.get_metadata();

        upsert_tree_metadata(
            db,
            pubkey,
            metadata.root_history_capacity as i64,
            tree_account.height as i32,
            TreeType::AddressV2 as i32,
            metadata.sequence_number,
            metadata.next_index,
            pubkey, // For V2 address trees, queue == tree
        )
        .await?;

        info!(
            "Synced V2 address tree {} with root_history_capacity {}",
            pubkey, metadata.root_history_capacity
        );
        return Ok(true);
    }

    debug!("Account {} is not a recognized tree type", pubkey);
    Ok(false)
}

fn process_v1_state_account(
    account: &Account,
) -> Result<(Pubkey, usize, u32, u64, u64), PhotonApiError> {
    check_discriminator::<StateMerkleTreeAccount>(&account.data).map_err(|_| {
        PhotonApiError::UnexpectedError("Invalid state merkle tree discriminator".to_string())
    })?;

    let tree_account =
        StateMerkleTreeAccount::deserialize(&mut &account.data[8..]).map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to deserialize state tree account: {}",
                e
            ))
        })?;

    let tree_data = &account.data[8 + mem::size_of::<StateMerkleTreeAccount>()..];
    let merkle_tree = ConcurrentMerkleTreeZeroCopy::<Poseidon, 26>::from_bytes_zero_copy(tree_data)
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to parse concurrent merkle tree: {}",
                e
            ))
        })?;

    let root_history_capacity = merkle_tree.roots.capacity();
    let height = merkle_tree.height as u32;
    let sequence_number = merkle_tree.sequence_number() as u64;
    let next_index = merkle_tree.next_index() as u64;

    let queue_pubkey = Pubkey::new_from_array(tree_account.metadata.associated_queue.to_bytes());

    Ok((
        queue_pubkey,
        root_history_capacity,
        height,
        sequence_number,
        next_index,
    ))
}

fn process_v1_address_account(
    account: &Account,
) -> Result<(Pubkey, usize, u32, u64, u64), PhotonApiError> {
    check_discriminator::<AddressMerkleTreeAccount>(&account.data).map_err(|_| {
        PhotonApiError::UnexpectedError("Invalid address merkle tree discriminator".to_string())
    })?;

    let tree_account =
        AddressMerkleTreeAccount::deserialize(&mut &account.data[8..]).map_err(|e| {
            PhotonApiError::UnexpectedError(format!(
                "Failed to deserialize address tree account: {}",
                e
            ))
        })?;

    let tree_data = &account.data[8 + mem::size_of::<AddressMerkleTreeAccount>()..];
    let indexed_tree =
        IndexedMerkleTreeZeroCopy::<Poseidon, usize, 26, 16>::from_bytes_zero_copy(tree_data)
            .map_err(|e| {
                PhotonApiError::UnexpectedError(format!(
                    "Failed to parse indexed merkle tree: {}",
                    e
                ))
            })?;

    let root_history_capacity = indexed_tree.merkle_tree.roots.capacity();
    let height = indexed_tree.merkle_tree.height as u32;
    let sequence_number = indexed_tree.merkle_tree.sequence_number() as u64;
    let next_index = indexed_tree.merkle_tree.next_index() as u64;
    let queue_pubkey = Pubkey::new_from_array(tree_account.metadata.associated_queue.to_bytes());

    Ok((
        queue_pubkey,
        root_history_capacity,
        height,
        sequence_number,
        next_index,
    ))
}

pub async fn upsert_tree_metadata(
    db: &DatabaseConnection,
    tree_pubkey: Pubkey,
    root_history_capacity: i64,
    height: i32,
    tree_type: i32,
    sequence_number: u64,
    next_index: u64,
    queue_pubkey: Pubkey,
) -> Result<(), PhotonApiError> {
    let tree_bytes = tree_pubkey.to_bytes().to_vec();

    // Check if exists
    let existing = TreeMetadata::find()
        .filter(tree_metadata::Column::TreePubkey.eq(tree_bytes.clone()))
        .one(db)
        .await?;

    let model = tree_metadata::ActiveModel {
        tree_pubkey: Set(tree_bytes),
        queue_pubkey: Set(queue_pubkey.to_bytes().to_vec()),
        tree_type: Set(tree_type),
        height: Set(height),
        root_history_capacity: Set(root_history_capacity),
        sequence_number: Set(sequence_number as i64),
        next_index: Set(next_index as i64),
        last_synced_slot: Set(0),
    };

    if existing.is_some() {
        model.update(db).await?;
        debug!("Updated tree metadata for {}", tree_pubkey);
    } else {
        model.insert(db).await?;
        debug!("Inserted new tree metadata for {}", tree_pubkey);
    }

    Ok(())
}
