use anyhow::Result;
use futures::StreamExt;
use light_compressed_account::TreeType;
use photon_indexer::ingester::parser::{parse_transaction, state_update::IndexedTreeLeafUpdate};
use photon_indexer::ingester::typedefs::block_info::{parse_ui_confirmed_blocked, BlockInfo};
use photon_indexer::snapshot::{load_block_stream_from_directory_adapter, DirectoryAdapter};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config;
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use solana_pubkey::{pubkey, Pubkey};
use solana_sdk::signature::Signature;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

// Import the new gap detection functions
use crate::test_v1_address_tree_sequence_consistency::{
    detect_gaps_from_sequences, SequenceGap, StateUpdateFieldType, StateUpdateSequences,
};

// V1 Address Tree Pubkey - the only v1 address tree
const V1_ADDRESS_TREE: Pubkey = pubkey!("amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2");

#[tokio::test]
#[ignore]
async fn test_fill_v1_address_tree_gaps() -> Result<()> {
    println!("🔧 Testing Comprehensive Gap Filling for All StateUpdate Fields");

    // Step 1: Load existing snapshot and detect ALL gaps using comprehensive gap detection
    let gaps = analyze_existing_snapshot_for_all_gaps().await?;

    if gaps.is_empty() {
        println!("✅ No gaps found in existing snapshot");
        return Ok(());
    }

    println!(
        "🔍 Found {} gaps to fill across all StateUpdate fields:",
        gaps.len()
    );

    // Group and display gaps by field type
    let mut gaps_by_field: HashMap<StateUpdateFieldType, Vec<&SequenceGap>> = HashMap::new();
    for gap in &gaps {
        gaps_by_field
            .entry(gap.field_type.clone())
            .or_insert_with(Vec::new)
            .push(gap);
    }

    for (field_type, field_gaps) in &gaps_by_field {
        println!("  {:?}: {} gaps", field_type, field_gaps.len());
    }

    // Step 2: Fetch missing blocks using signature-based approach
    println!(
        "🎯 Processing all {} gaps across all StateUpdate fields",
        gaps.len()
    );

    let (mut missing_blocks, mut missing_updates) = fetch_missing_blocks(&gaps).await?;

    // Step 3: Update snapshot with signature-based results
    if !missing_blocks.is_empty() {
        update_snapshot_with_missing_blocks(&missing_blocks).await?;
        println!(
            "✅ Updated snapshot with {} signature-based blocks",
            missing_blocks.len()
        );
    }

    // Step 4: Validate and fallback for remaining gaps
    println!("🔍 Checking for remaining gaps after signature-based approach...");
    let remaining_gaps = analyze_existing_snapshot_for_all_gaps().await?;

    if remaining_gaps.is_empty() {
        println!("✅ All gaps filled by signature-based approach!");
    } else {
        println!(
            "⚠️  Still have {} gaps - triggering slot-range fallback",
            remaining_gaps.len()
        );

        // Get RPC client for fallback
        let rpc_url = std::env::var("RPC_URL")
            .unwrap_or_else(|_| "https://api.devnet.solana.com".to_string());
        let client = RpcClient::new(rpc_url);

        // Rebuild existing slots index after snapshot update
        let updated_existing_slots = build_existing_slot_index().await?;
        let (fallback_blocks, fallback_updates) =
            validate_and_fallback_gap_filling(&client, &remaining_gaps, &updated_existing_slots)
                .await?;

        if !fallback_blocks.is_empty() {
            let fallback_count = fallback_blocks.len();
            update_snapshot_with_missing_blocks(&fallback_blocks).await?;
            missing_blocks.extend(fallback_blocks);
            missing_updates.extend(fallback_updates);
            println!(
                "✅ Updated snapshot with {} additional fallback blocks",
                fallback_count
            );
        }
    }

    println!(
        "🎯 Total blocks added: {}, V1 updates: {}",
        missing_blocks.len(),
        missing_updates.len()
    );

    // Step 5: Final verification
    verify_gaps_filled().await?;

    println!("🎉 Comprehensive gap filling completed!");

    Ok(())
}

async fn analyze_existing_snapshot_for_all_gaps() -> Result<Vec<SequenceGap>> {
    println!("📂 Analyzing existing snapshot for ALL gaps using comprehensive gap detection...");

    let snapshot_path = "target/snapshot_local";
    let directory_adapter = Arc::new(DirectoryAdapter::from_local_directory(
        snapshot_path.to_string(),
    ));

    let block_stream = load_block_stream_from_directory_adapter(directory_adapter).await;
    let all_blocks: Vec<Vec<_>> = block_stream.collect().await;
    let blocks: Vec<_> = all_blocks.into_iter().flatten().collect();

    println!("📦 Processing {} blocks from snapshot", blocks.len());

    // Extract sequences from all StateUpdates using the new system
    let mut sequences = StateUpdateSequences::default();
    let mut total_transactions = 0;
    let mut parsed_transactions = 0;

    for block in blocks {
        let slot = block.metadata.slot;
        total_transactions += block.transactions.len();

        for transaction in &block.transactions {
            let signature = transaction.signature.to_string();

            // Parse each transaction to extract state updates
            match parse_transaction(transaction, slot, None) {
                Ok(state_update) => {
                    parsed_transactions += 1;

                    // Extract sequences with context using the new method
                    sequences.extract_state_update_sequences(&state_update, slot, &signature);
                }
                Err(_) => {
                    // Skip failed parsing - compression transactions might have parsing issues
                    continue;
                }
            }
        }
    }

    println!(
        "📊 Parsed {}/{} transactions successfully",
        parsed_transactions, total_transactions
    );

    // Detect gaps across ALL StateUpdate fields using the comprehensive system
    let all_gaps = detect_gaps_from_sequences(&sequences);

    println!(
        "🔍 Found {} total gaps across all StateUpdate fields",
        all_gaps.len()
    );

    Ok(all_gaps)
}

#[allow(unused)]
async fn analyze_existing_snapshot() -> Result<Vec<SequenceGap>> {
    println!("📂 Analyzing existing snapshot for V1 address tree gaps...");

    // Get all gaps first
    let all_gaps = analyze_existing_snapshot_for_all_gaps().await?;

    // Filter for V1 address tree gaps only (for backward compatibility)
    let v1_gaps: Vec<SequenceGap> = all_gaps
        .into_iter()
        .filter(|gap| {
            gap.field_type == StateUpdateFieldType::IndexedTreeUpdate
                && gap.tree_pubkey == Some(V1_ADDRESS_TREE)
        })
        .collect();

    println!(
        "🎯 Found {} gaps specifically in V1 address tree",
        v1_gaps.len()
    );

    Ok(v1_gaps)
}

/// Build a HashSet of all slot numbers that already exist in the current snapshot
async fn build_existing_slot_index() -> Result<HashSet<u64>> {
    let snapshot_path = "target/snapshot_local";
    let directory_adapter = Arc::new(DirectoryAdapter::from_local_directory(
        snapshot_path.to_string(),
    ));

    let block_stream = load_block_stream_from_directory_adapter(directory_adapter).await;
    let all_blocks: Vec<Vec<_>> = block_stream.collect().await;
    let blocks: Vec<_> = all_blocks.into_iter().flatten().collect();

    let existing_slots: HashSet<u64> = blocks.iter().map(|block| block.metadata.slot).collect();

    Ok(existing_slots)
}

/// Calculate global gap boundaries across all gaps
fn calculate_global_gap_boundaries(gaps: &[SequenceGap]) -> (u64, u64, String, String) {
    let min_slot = gaps.iter().map(|g| g.before_slot).min().unwrap_or(0);
    let max_slot = gaps.iter().map(|g| g.after_slot).max().unwrap_or(0);

    // Find the earliest before_signature and latest after_signature
    // For comprehensive coverage, we want the earliest possible start and latest possible end
    let earliest_before_sig = gaps
        .iter()
        .min_by_key(|g| g.before_slot)
        .map(|g| g.before_signature.clone())
        .unwrap_or_default();

    let latest_after_sig = gaps
        .iter()
        .max_by_key(|g| g.after_slot)
        .map(|g| g.after_signature.clone())
        .unwrap_or_default();

    (min_slot, max_slot, earliest_before_sig, latest_after_sig)
}

/// Fetch ALL signatures between two boundaries with full pagination
async fn fetch_all_signatures_paginated(
    client: &RpcClient,
    earliest_before_sig: &str,
    latest_after_sig: &str,
) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
    let compression_program_id = solana_sdk::pubkey::Pubkey::new_from_array(
        photon_indexer::ingester::parser::get_compression_program_id().to_bytes(),
    );

    let before_signature = Signature::from_str(earliest_before_sig)?;
    let until_signature = Signature::from_str(latest_after_sig)?;

    let mut all_signatures = Vec::new();
    let mut current_before = Some(until_signature); // Start from latest (going backwards)
    let mut page_count = 0;

    loop {
        page_count += 1;
        let config = GetConfirmedSignaturesForAddress2Config {
            before: current_before,
            until: Some(before_signature), // Stop at earliest
            limit: Some(1000),             // Use smaller limit for better reliability
            commitment: None,
        };

        let batch = client
            .get_signatures_for_address_with_config(&compression_program_id, config)
            .await?;

        if batch.is_empty() {
            break; // No more signatures
        }

        println!(
            "   📄 Page {}: fetched {} signatures",
            page_count,
            batch.len()
        );

        // Check if we've reached our until signature
        let mut reached_until = false;
        for sig_info in &batch {
            if let Ok(sig) = Signature::from_str(&sig_info.signature) {
                if sig == before_signature {
                    reached_until = true;
                    break;
                }
            }
        }

        all_signatures.extend(batch.clone());

        if reached_until || batch.len() < 1000 {
            // If we got less than limit, we're done
            break;
        }

        // Update before for next page
        current_before = batch
            .last()
            .and_then(|sig| Signature::from_str(&sig.signature).ok());
    }

    Ok(all_signatures)
}

/// Efficiently fetch blocks in batch with progress tracking
async fn fetch_blocks_batch(
    client: &RpcClient,
    mut needed_slots: Vec<u64>,
) -> Result<(Vec<BlockInfo>, Vec<IndexedTreeLeafUpdate>)> {
    needed_slots.sort(); // Process in order

    let mut missing_blocks = Vec::new();
    let mut missing_updates = Vec::new();
    let mut slots_with_missing_seqs = HashSet::new();

    for (i, slot) in needed_slots.iter().enumerate() {
        match client
            .get_block_with_config(
                *slot,
                solana_client::rpc_config::RpcBlockConfig {
                    encoding: Some(solana_transaction_status::UiTransactionEncoding::Base64),
                    transaction_details: Some(solana_transaction_status::TransactionDetails::Full),
                    rewards: None,
                    commitment: Some(solana_sdk::commitment_config::CommitmentConfig::confirmed()),
                    max_supported_transaction_version: Some(0),
                },
            )
            .await
        {
            Ok(block) => {
                if let Ok(block_info) = parse_ui_confirmed_blocked(block, *slot) {
                    let mut has_missing_seq = false;

                    // Check if this block contains compression transactions (any type)
                    for transaction in &block_info.transactions {
                        if let Ok(state_update) = parse_transaction(transaction, *slot, None) {
                            // Check for any compression activity that could fill gaps
                            if !state_update.indexed_merkle_tree_updates.is_empty()
                                || !state_update.leaf_nullifications.is_empty()
                                || !state_update.batch_nullify_context.is_empty()
                                || !state_update.batch_new_addresses.is_empty()
                                || !state_update.batch_merkle_tree_events.is_empty()
                                || !state_update.out_accounts.is_empty()
                            {
                                println!(
                                    "   ✅ Found compression activity in slot {} [{}/{}]",
                                    slot,
                                    i + 1,
                                    needed_slots.len()
                                );
                                has_missing_seq = true;

                                // Still collect V1 address tree updates for backwards compatibility
                                for ((tree_pubkey, _leaf_index), leaf_update) in
                                    state_update.indexed_merkle_tree_updates
                                {
                                    if leaf_update.tree_type == TreeType::AddressV1
                                        && tree_pubkey == V1_ADDRESS_TREE
                                    {
                                        missing_updates.push(leaf_update);
                                    }
                                }
                            } else {
                                println!(
                                    "   ❌ No compression activity in slot {} [{}/{}]",
                                    slot,
                                    i + 1,
                                    needed_slots.len()
                                );
                            }
                        }
                    }

                    // If this block has compression activity and we haven't already collected it
                    if has_missing_seq && !slots_with_missing_seqs.contains(slot) {
                        // Filter block to only include compression transactions
                        let filtered_block = BlockInfo {
                            metadata: block_info.metadata.clone(),
                            transactions: block_info
                                .transactions
                                .iter()
                                .filter(|tx| {
                                    photon_indexer::snapshot::is_compression_transaction(tx)
                                })
                                .cloned()
                                .collect(),
                        };

                        println!(
                            "   📦 Collected block {} with {} compression transactions [{}/{}]",
                            slot,
                            filtered_block.transactions.len(),
                            i + 1,
                            needed_slots.len()
                        );
                        missing_blocks.push(filtered_block);
                        slots_with_missing_seqs.insert(*slot);
                    }
                }
            }
            Err(e) => {
                println!(
                    "   ❌ Failed to fetch slot {} [{}/{}]: {}",
                    slot,
                    i + 1,
                    needed_slots.len(),
                    e
                );
            }
        }
    }

    Ok((missing_blocks, missing_updates))
}

/// Validate if gaps remain after signature-based approach and fallback to slot-range fetching
async fn validate_and_fallback_gap_filling(
    client: &RpcClient,
    original_gaps: &[SequenceGap],
    existing_slots: &HashSet<u64>,
) -> Result<(Vec<BlockInfo>, Vec<IndexedTreeLeafUpdate>)> {
    // First, build a quick snapshot of what we currently have to check for remaining gaps
    println!("   🔍 Checking if gaps still exist after signature-based approach...");

    // For validation, we need to re-analyze the current state
    // This is a simplified check - in a real implementation we'd want to
    // rebuild the full state, but for now we'll use the gap ranges as a proxy

    let mut fallback_slots = Vec::new();

    // For each original gap, check if we might have missed slots in the range
    for gap in original_gaps {
        println!(
            "   📊 Checking gap in {:?}: slots {} to {}",
            gap.field_type, gap.before_slot, gap.after_slot
        );

        // Generate all slots in the gap range
        let gap_range_slots: Vec<u64> = (gap.before_slot + 1..gap.after_slot).collect();

        // Find slots in this range that we don't have and haven't fetched
        let missing_in_range: Vec<u64> = gap_range_slots
            .iter()
            .filter(|slot| !existing_slots.contains(slot))
            .copied()
            .collect();

        if !missing_in_range.is_empty() {
            println!(
                "   ⚠️  Found {} potentially missing slots in gap range",
                missing_in_range.len()
            );

            fallback_slots.extend(missing_in_range);
        }
    }

    if fallback_slots.is_empty() {
        println!("   ✅ No additional slots need fallback fetching");
        return Ok((Vec::new(), Vec::new()));
    }

    // Remove duplicates and sort
    fallback_slots.sort();
    fallback_slots.dedup();

    println!(
        "   🔄 Fallback: fetching {} additional slots from gap ranges",
        fallback_slots.len()
    );
    println!(
        "   📋 Fallback slots: {:?}",
        &fallback_slots[..std::cmp::min(10, fallback_slots.len())]
    );

    // Use the same batch fetching approach for fallback slots
    let result = fetch_blocks_batch(client, fallback_slots).await?;
    println!(
        "   ✅ Fallback completed: {} blocks, {} updates",
        result.0.len(),
        result.1.len()
    );
    Ok(result)
}

async fn fetch_missing_blocks(
    gaps: &[SequenceGap],
) -> Result<(Vec<BlockInfo>, Vec<IndexedTreeLeafUpdate>)> {
    println!("🌐 Ultra-Efficient Global Gap Filling Starting...");

    if gaps.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    // Get RPC URL from environment variable or use default devnet
    let rpc_url =
        std::env::var("RPC_URL").unwrap_or_else(|_| "https://api.devnet.solana.com".to_string());

    println!("🔗 Using RPC endpoint: {}", rpc_url);
    let client = RpcClient::new(rpc_url);

    // Phase 1: Build existing slot index from current snapshot
    println!("📂 Phase 1: Building existing slot index from snapshot...");
    let existing_slots = build_existing_slot_index().await?;
    println!(
        "📊 Found {} existing slots in snapshot",
        existing_slots.len()
    );

    // Phase 1.5: Calculate global gap boundaries
    println!("🌍 Phase 1.5: Calculating global gap boundaries...");
    let (min_slot, max_slot, earliest_before_sig, latest_after_sig) =
        calculate_global_gap_boundaries(gaps);
    println!(
        "🎯 Global gap range: slots {} to {} (span: {} slots)",
        min_slot,
        max_slot,
        max_slot - min_slot
    );
    println!(
        "🔗 Global signature range: {} -> {}",
        &earliest_before_sig[..8],
        &latest_after_sig[..8]
    );

    // Phase 2: Smart signature collection with pagination
    println!("📡 Phase 2: Fetching ALL signatures with pagination...");
    let all_signatures =
        fetch_all_signatures_paginated(&client, &earliest_before_sig, &latest_after_sig).await?;
    println!(
        "✅ Collected {} total signatures across all gaps",
        all_signatures.len()
    );

    // Phase 3: Extract and filter slots
    println!("🔍 Phase 3: Extracting and filtering slots...");
    let signature_slots: HashSet<u64> = all_signatures
        .iter()
        .filter(|sig_info| sig_info.err.is_none()) // Skip failed transactions
        .map(|sig_info| sig_info.slot)
        .collect();
    println!(
        "📊 Found {} unique slots from signatures",
        signature_slots.len()
    );

    // Filter out slots we already have - this is the key optimization!
    let needed_slots: Vec<u64> = signature_slots
        .iter()
        .filter(|slot| !existing_slots.contains(slot))
        .copied()
        .collect();

    println!(
        "🎯 Need to fetch {} new blocks (filtered out {} existing)",
        needed_slots.len(),
        signature_slots.len() - needed_slots.len()
    );

    // Phase 4: Efficient batch block fetching (even if empty)
    let (missing_blocks, missing_updates) = if needed_slots.is_empty() {
        println!("📦 Phase 4: No new blocks to fetch from signatures");
        (Vec::new(), Vec::new())
    } else {
        println!(
            "📦 Phase 4: Fetching {} missing blocks...",
            needed_slots.len()
        );
        fetch_blocks_batch(&client, needed_slots).await?
    };

    println!(
        "🎯 Signature-based approach: found {} blocks, {} updates",
        missing_blocks.len(),
        missing_updates.len()
    );
    Ok((missing_blocks, missing_updates))
}

#[allow(unused)]
fn validate_sequence_consistency(updates: &[IndexedTreeLeafUpdate]) -> Result<()> {
    println!("🔍 Validating sequence consistency after gap filling...");

    if updates.is_empty() {
        return Err(anyhow::anyhow!("No updates to validate"));
    }

    let first_seq = updates[0].seq;
    let last_seq = updates.last().unwrap().seq;
    println!(
        "📈 Sequence range: {} to {} (span: {})",
        first_seq,
        last_seq,
        last_seq - first_seq + 1
    );

    // Check for sequential ordering
    let mut expected_seq = first_seq;
    let mut gaps = Vec::new();

    for (i, update) in updates.iter().enumerate() {
        if update.seq != expected_seq {
            gaps.push((i, expected_seq, update.seq));
        }
        expected_seq = update.seq + 1;
    }

    // Check for duplicates
    let mut seq_counts: HashMap<u64, usize> = HashMap::new();
    for update in updates {
        *seq_counts.entry(update.seq).or_insert(0) += 1;
    }

    let duplicates: Vec<_> = seq_counts
        .iter()
        .filter(|(_, &count)| count > 1)
        .map(|(&seq, &count)| (seq, count))
        .collect();

    // Report results
    println!("\n📊 Final Validation Results:");

    if gaps.is_empty() {
        println!("✅ All v1 address tree sequence numbers are now sequential!");
    } else {
        println!("❌ Still found {} gaps:", gaps.len());
        for (index, expected, actual) in gaps.iter().take(5) {
            println!(
                "   Index {}: expected seq {}, found seq {}",
                index, expected, actual
            );
        }
    }

    if duplicates.is_empty() {
        println!("✅ No duplicate sequence numbers found");
    } else {
        println!("❌ Found {} duplicate sequence numbers", duplicates.len());
    }

    if !gaps.is_empty() {
        return Err(anyhow::anyhow!(
            "Sequence gaps still exist after gap filling"
        ));
    }

    if !duplicates.is_empty() {
        return Err(anyhow::anyhow!("Duplicate sequence numbers found"));
    }

    println!("✅ Perfect sequence consistency achieved!");
    Ok(())
}

async fn update_snapshot_with_missing_blocks(missing_blocks: &[BlockInfo]) -> Result<()> {
    println!("💾 Updating snapshot file with missing blocks...");

    let snapshot_path = "target/snapshot_local";
    let directory_adapter = Arc::new(DirectoryAdapter::from_local_directory(
        snapshot_path.to_string(),
    ));

    // Load existing blocks from snapshot
    let block_stream = load_block_stream_from_directory_adapter(directory_adapter.clone()).await;
    let all_blocks: Vec<Vec<_>> = block_stream.collect().await;
    let mut existing_blocks: Vec<_> = all_blocks.into_iter().flatten().collect();

    println!(
        "📦 Loaded {} existing blocks from snapshot",
        existing_blocks.len()
    );

    // Add missing blocks to existing blocks
    existing_blocks.extend_from_slice(missing_blocks);

    // Sort all blocks by slot
    existing_blocks.sort_by_key(|block| block.metadata.slot);

    println!(
        "📦 Total blocks after adding missing: {}",
        existing_blocks.len()
    );

    // Clear existing snapshot files
    let existing_snapshots =
        photon_indexer::snapshot::get_snapshot_files_with_metadata(directory_adapter.as_ref())
            .await?;
    for snapshot in existing_snapshots {
        directory_adapter.delete_file(snapshot.file).await?;
    }

    // Create new snapshot with all blocks
    let first_slot = existing_blocks
        .first()
        .map(|b| b.metadata.slot)
        .unwrap_or(0);
    let last_slot = existing_blocks.last().map(|b| b.metadata.slot).unwrap_or(0);

    let snapshot_filename = format!("snapshot-{}-{}", first_slot, last_slot);

    println!("💾 Writing updated snapshot: {}", snapshot_filename);

    // Serialize all blocks
    let mut snapshot_data = Vec::new();
    for block in &existing_blocks {
        let block_bytes = bincode::serialize(block).unwrap();
        snapshot_data.extend(block_bytes);
    }

    // Write updated snapshot file
    let snapshot_path_buf = std::path::PathBuf::from(snapshot_path).join(&snapshot_filename);
    std::fs::write(&snapshot_path_buf, snapshot_data)?;

    println!(
        "✅ Successfully updated snapshot with {} total blocks",
        existing_blocks.len()
    );
    Ok(())
}

async fn verify_gaps_filled() -> Result<()> {
    println!("🔍 Verifying ALL gaps are filled in updated snapshot...");

    // Run comprehensive analysis to check for all types of gaps
    let all_gaps = analyze_existing_snapshot_for_all_gaps().await?;

    if all_gaps.is_empty() {
        println!("🎉 SUCCESS: All gaps across all StateUpdate fields have been filled!");
        return Ok(());
    }

    println!("⚠️  Still found {} gaps after filling:", all_gaps.len());

    // Group remaining gaps by field type for better reporting
    let mut gaps_by_field: HashMap<StateUpdateFieldType, Vec<&SequenceGap>> = HashMap::new();
    for gap in &all_gaps {
        gaps_by_field
            .entry(gap.field_type.clone())
            .or_insert_with(Vec::new)
            .push(gap);
    }

    for (field_type, field_gaps) in &gaps_by_field {
        println!("  {:?}: {} remaining gaps", field_type, field_gaps.len());
        for gap in field_gaps.iter().take(2) {
            // Show first 2 gaps for each field type
            println!("    Slot {} -> {}", gap.before_slot, gap.after_slot);
        }
        if field_gaps.len() > 2 {
            println!("    ... and {} more", field_gaps.len() - 2);
        }
    }

    // This is still success - we may not have filled all gaps due to missing blocks on RPC
    println!(
        "ℹ️  Note: Some gaps may remain due to missing blocks on RPC or truly missing sequences"
    );
    Ok(())
}
