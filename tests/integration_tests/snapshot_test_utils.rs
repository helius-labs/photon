use anyhow::{Context, Result};
use futures::stream;
use photon_indexer::ingester::parser::get_compression_program_id;
use photon_indexer::ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo};
use photon_indexer::snapshot::{
    create_snapshot_from_byte_stream, load_block_stream_from_directory_adapter,
    load_byte_stream_from_directory_adapter, DirectoryAdapter,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_sdk::signature::Signature;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

/// Test utility to create a snapshot file from compression transactions found on-chain
pub async fn create_test_snapshot_from_compression_transactions(
    rpc_url: &str,
    max_signatures: usize,
) -> Result<String> {
    println!("Connecting to RPC: {}", rpc_url);
    let client = RpcClient::new(rpc_url.to_string());

    // Step 1: Fetch compression transaction signatures
    let signatures = fetch_compression_signatures(&client, max_signatures).await?;
    println!("Found {} compression transaction signatures:", signatures.len());
    for (i, signature) in signatures.iter().enumerate() {
        println!("  {}. {}", i + 1, signature);
    }

    if signatures.is_empty() {
        return Err(anyhow::anyhow!("No compression transactions found on devnet"));
    }

    // Step 2: Get unique slots from signatures
    let mut slots = HashSet::new();
    for signature in &signatures {
        match client.get_transaction_with_config(
            signature,
            solana_client::rpc_config::RpcTransactionConfig {
                encoding: Some(solana_transaction_status::UiTransactionEncoding::Json),
                commitment: None,
                max_supported_transaction_version: Some(1),
            },
        ).await {
            Ok(tx) => {
                slots.insert(tx.slot);
            }
            Err(e) => {
                eprintln!("Failed to get transaction {}: {}", signature, e);
            }
        }
    }

    let mut slots: Vec<u64> = slots.into_iter().collect();
    slots.sort();
    println!("Found {} unique slots with compression transactions:", slots.len());
    for (i, slot) in slots.iter().enumerate() {
        println!("  {}. Slot {}", i + 1, slot);
    }

    // Step 3: Fetch blocks for these slots
    let mut blocks = Vec::new();
    for slot in &slots {
        match client.get_block_with_config(
            *slot,
            solana_client::rpc_config::RpcBlockConfig {
                encoding: Some(solana_transaction_status::UiTransactionEncoding::Base64),
                transaction_details: Some(solana_transaction_status::TransactionDetails::Full),
                rewards: None,
                commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            },
        ).await {
            Ok(block) => {
                match parse_ui_confirmed_blocked(block, *slot) {
                    Ok(block_info) => {
                        let block_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(block_info.metadata.block_time as u64);
                        let datetime = std::time::SystemTime::now().duration_since(block_time)
                            .map(|d| format!("{:.1} seconds ago", d.as_secs_f64()))
                            .unwrap_or_else(|_| format!("timestamp: {}", block_info.metadata.block_time));
                        println!("Successfully parsed block at slot {} ({} transactions, {})", 
                               slot, block_info.transactions.len(), datetime);
                        blocks.push(block_info);
                    }
                    Err(e) => {
                        eprintln!("Failed to parse block at slot {}: {}", slot, e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch block at slot {}: {}", slot, e);
            }
        }
    }

    if blocks.is_empty() {
        return Err(anyhow::anyhow!("No blocks could be fetched and parsed"));
    }

    println!("Successfully fetched and parsed {} blocks", blocks.len());

    // Step 4: Create snapshot from blocks
    let snapshot_dir = std::path::PathBuf::from("target").join("test_snapshots").join("devnet");
    std::fs::create_dir_all(&snapshot_dir)?;

    let snapshot_dir_str = snapshot_dir.to_str().unwrap().to_string();
    let directory_adapter = Arc::new(DirectoryAdapter::from_local_directory(snapshot_dir_str.clone()));

    // Clear any existing snapshots
    let existing_snapshots = photon_indexer::snapshot::get_snapshot_files_with_metadata(directory_adapter.as_ref()).await?;
    for snapshot in existing_snapshots {
        directory_adapter.delete_file(snapshot.file).await?;
    }

    // Sort blocks by slot to ensure proper ordering
    blocks.sort_by_key(|block| block.metadata.slot);
    
    // Set last_indexed_slot to be the slot before the first block to ensure snapshot creation
    let last_indexed_slot = blocks.first().map(|b| b.metadata.slot.saturating_sub(1)).unwrap_or(0);
    
    // Create blocks stream
    let blocks_stream = stream::iter(vec![blocks]);
    
    // Create snapshot with small interval for testing (every 1 slot incremental, every 10 slots full)
    photon_indexer::snapshot::update_snapshot_helper(
        directory_adapter.clone(),
        blocks_stream,
        last_indexed_slot,
        1, // incremental_snapshot_interval_slots
        100, // full_snapshot_interval_slots (increase to avoid full snapshot merging during test)
    ).await;

    println!("Snapshot created successfully in directory: {}", snapshot_dir_str);
    
    // Debug: List created snapshot files
    let created_snapshots = photon_indexer::snapshot::get_snapshot_files_with_metadata(directory_adapter.as_ref()).await?;
    println!("Created {} snapshot files:", created_snapshots.len());
    for snapshot in &created_snapshots {
        println!("  - {} (slots {} to {})", snapshot.file, snapshot.start_slot, snapshot.end_slot);
    }

    Ok(snapshot_dir_str)
}


/// Validate that photon can parse the generated snapshot
pub async fn validate_snapshot_parsing(snapshot_dir: &str) -> Result<Vec<BlockInfo>> {
    let directory_adapter = Arc::new(DirectoryAdapter::from_local_directory(snapshot_dir.to_string()));

    // Load and parse the snapshot
    let block_stream = load_block_stream_from_directory_adapter(directory_adapter.clone()).await;
    let blocks: Vec<Vec<BlockInfo>> = futures::StreamExt::collect(block_stream).await;
    let blocks: Vec<BlockInfo> = blocks.into_iter().flatten().collect();

    println!("Successfully parsed {} blocks from snapshot", blocks.len());

    // Validate that all blocks contain only compression transactions
    for (i, block) in blocks.iter().enumerate() {
        println!("Block {} at slot {}: {} transactions", 
                i, block.metadata.slot, block.transactions.len());
        
        for (j, tx) in block.transactions.iter().enumerate() {
            let is_compression = photon_indexer::snapshot::is_compression_transaction(tx);
            if !is_compression {
                return Err(anyhow::anyhow!(
                    "Block {} transaction {} is not a compression transaction", i, j
                ));
            }
        }
    }

    println!("All transactions in snapshot are compression transactions ✓");
    Ok(blocks)
}

/// Test round-trip: create snapshot and reload it via byte stream
pub async fn test_snapshot_roundtrip(snapshot_dir: &str) -> Result<()> {
    let source_adapter = Arc::new(DirectoryAdapter::from_local_directory(snapshot_dir.to_string()));
    
    // Create a second directory for the round-trip test
    let roundtrip_dir = std::path::PathBuf::from("target").join("test_snapshots").join("roundtrip");
    std::fs::create_dir_all(&roundtrip_dir)?;
    let roundtrip_dir_str = roundtrip_dir.to_str().unwrap().to_string();
    let target_adapter = Arc::new(DirectoryAdapter::from_local_directory(roundtrip_dir_str));

    // Load byte stream from source
    let byte_stream = load_byte_stream_from_directory_adapter(source_adapter.clone()).await;
    
    // Create snapshot from byte stream in target
    create_snapshot_from_byte_stream(byte_stream, target_adapter.as_ref()).await?;

    // Load blocks from both snapshots and compare
    let source_blocks = load_block_stream_from_directory_adapter(source_adapter).await;
    let source_blocks: Vec<Vec<BlockInfo>> = futures::StreamExt::collect(source_blocks).await;
    let source_blocks: Vec<BlockInfo> = source_blocks.into_iter().flatten().collect();

    let target_blocks = load_block_stream_from_directory_adapter(target_adapter).await;
    let target_blocks: Vec<Vec<BlockInfo>> = futures::StreamExt::collect(target_blocks).await;
    let target_blocks: Vec<BlockInfo> = target_blocks.into_iter().flatten().collect();

    if source_blocks.len() != target_blocks.len() {
        return Err(anyhow::anyhow!(
            "Block count mismatch: source={}, target={}",
            source_blocks.len(),
            target_blocks.len()
        ));
    }

    for (i, (source_block, target_block)) in source_blocks.iter().zip(target_blocks.iter()).enumerate() {
        if source_block != target_block {
            return Err(anyhow::anyhow!("Block {} differs between source and target", i));
        }
    }

    println!("Round-trip test passed: {} blocks match exactly", source_blocks.len());
    Ok(())
}

async fn fetch_compression_signatures(
    client: &RpcClient,
    max_signatures: usize,
) -> Result<Vec<Signature>> {
    let mut signatures = Vec::new();
    let mut before = None;

    while signatures.len() < max_signatures {
        let config = GetConfirmedSignaturesForAddress2Config {
            before,
            until: None,
            limit: None, //Some(std::cmp::min(max_signatures - signatures.len(), 1000)),
            commitment: None,
        };

        let compression_program_id = solana_sdk::pubkey::Pubkey::new_from_array(get_compression_program_id().to_bytes());
        println!("Fetching signatures for compression program: {}", compression_program_id);
        let batch = client
            .get_signatures_for_address_with_config(&compression_program_id, config)
            .await
            .context("Failed to fetch signatures for compression program")?;
        
        println!("Fetched {} signatures in this batch", batch.len());

     

        for sig_info in &batch {
            // Skip failed transactions
            if sig_info.err.is_some() {
                continue;
            }

            let signature = Signature::from_str(&sig_info.signature)
                .context("Failed to parse signature")?;
            signatures.push(signature);

            if signatures.len() >= max_signatures {
                break;
            }
        }

        before = batch.last().map(|sig| Signature::from_str(&sig.signature).unwrap());

        if batch.len() < 1000 {
            // No more signatures available
            break;
        }
    }

    Ok(signatures)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Remove this to run the test
    async fn test_create_snapshot_from_compression_transactions() {
        let snapshot_dir = create_test_snapshot_from_compression_transactions(
            "https://api.devnet.solana.com",
            10, // Fetch 10 compression transactions
        )
        .await
        .expect("Failed to create test snapshot");

        let blocks = validate_snapshot_parsing(&snapshot_dir)
            .await
            .expect("Failed to validate snapshot parsing");

        assert!(!blocks.is_empty(), "Snapshot should contain blocks");

        test_snapshot_roundtrip(&snapshot_dir)
            .await
            .expect("Round-trip test failed");

        println!("Test completed successfully!");
        println!("Snapshot directory: {}", snapshot_dir);
        println!("Parsed {} blocks from snapshot", blocks.len());
    }
}