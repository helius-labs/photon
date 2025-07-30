use anyhow::Result;
use futures::StreamExt;
use photon_indexer::ingester::parser::{
    indexer_events::MerkleTreeEvent, parse_transaction, state_update::StateUpdate,
};
use photon_indexer::snapshot::{load_block_stream_from_directory_adapter, DirectoryAdapter};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;

fn merkle_event_to_type_id(event: &MerkleTreeEvent) -> u8 {
    match event {
        MerkleTreeEvent::BatchAppend(_) => 1,
        MerkleTreeEvent::BatchNullify(_) => 2,
        MerkleTreeEvent::BatchAddressAppend(_) => 3,
        _ => 0, // Other event types we don't care about
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StateUpdateFieldType {
    IndexedTreeUpdate,
    LeafNullification,
    BatchNullifyContext,
    BatchNewAddress,
    BatchMerkleTreeEventAppend,
    BatchMerkleTreeEventNullify,
    BatchMerkleTreeEventAddressAppend,
    OutAccount,
}

#[derive(Debug, Clone)]
pub struct SequenceGap {
    // Boundary information for gap filling
    pub before_slot: u64,
    pub after_slot: u64,
    pub before_signature: String,
    pub after_signature: String,

    // Tree/context metadata
    pub tree_pubkey: Option<Pubkey>, // Tree pubkey (unified for all tree operations)
    pub _tree_type_string: Option<String>, // Tree type string (for indexed tree updates)
    pub field_type: StateUpdateFieldType,
}

#[derive(Debug, Default, Clone)]
pub struct StateUpdateSequences {
    // Sequences with slot and signature information for gap analysis
    indexed_tree_seqs: HashMap<(Pubkey, String), Vec<(u64, u64, String)>>, // (tree, type_string) -> (seq, slot, signature)
    nullification_seqs: HashMap<Pubkey, Vec<(u64, u64, String)>>, // tree -> (seq, slot, signature)
    batch_nullify_queue_indexes: Vec<(u64, u64, String)>,         // (queue_index, slot, signature)
    batch_address_queue_indexes: HashMap<Pubkey, Vec<(u64, u64, String)>>, // tree -> (queue_index, slot, signature)
    batch_merkle_event_seqs: HashMap<(Pubkey, u8), Vec<(u64, u64, String)>>, // (tree_pubkey, event_type) -> (seq, slot, signature)
    out_account_leaf_indexes: HashMap<Pubkey, Vec<(u64, u64, String)>>, // tree -> (leaf_index, slot, signature)
}
impl StateUpdateSequences {
    /// Extracts sequences from a StateUpdate with slot and signature context
    pub fn extract_state_update_sequences(
        &mut self,
        state_update: &StateUpdate,
        slot: u64,
        signature: &str,
    ) {
        // Extract indexed tree sequences
        for ((tree_pubkey, _), leaf_update) in &state_update.indexed_merkle_tree_updates {
            let tree_type_string = format!("{:?}", leaf_update.tree_type);
            self.indexed_tree_seqs
                .entry((*tree_pubkey, tree_type_string))
                .or_insert_with(Vec::new)
                .push((leaf_update.seq, slot, signature.to_string()));
        }

        // Extract leaf nullification sequences
        for nullification in &state_update.leaf_nullifications {
            self.nullification_seqs
                .entry(nullification.tree)
                .or_insert_with(Vec::new)
                .push((nullification.seq, slot, signature.to_string()));
        }

        // Extract batch nullify context queue indexes
        for context in &state_update.batch_nullify_context {
            self.batch_nullify_queue_indexes.push((
                context.nullifier_queue_index,
                slot,
                signature.to_string(),
            ));
        }

        // Extract batch new address queue indexes
        for address in &state_update.batch_new_addresses {
            self.batch_address_queue_indexes
                .entry(address.tree.0)
                .or_insert_with(Vec::new)
                .push((address.queue_index, slot, signature.to_string()));
        }

        // Extract batch merkle tree event sequences
        for (tree_hash, events) in &state_update.batch_merkle_tree_events {
            let tree_pubkey = Pubkey::from(*tree_hash);
            for (seq, merkle_event) in events {
                let event_type = merkle_event_to_type_id(merkle_event);
                if event_type > 0 {
                    self.batch_merkle_event_seqs
                        .entry((tree_pubkey, event_type))
                        .or_insert_with(Vec::new)
                        .push((*seq, slot, signature.to_string()));
                }
            }
        }

        // Extract out_account leaf indexes
        for account_with_context in &state_update.out_accounts {
            let tree_pubkey = account_with_context.account.tree.0;
            let leaf_index = account_with_context.account.leaf_index.0;
            self.out_account_leaf_indexes
                .entry(tree_pubkey)
                .or_insert_with(Vec::new)
                .push((leaf_index, slot, signature.to_string()));
        }
    }
}

/// Merges multiple StateUpdateSequences into a single aggregated structure
pub fn merge_state_update_sequences(
    all_sequences: &[StateUpdateSequences],
) -> StateUpdateSequences {
    let mut aggregated = StateUpdateSequences::default();

    for sequences in all_sequences {
        // Merge indexed tree sequences
        for ((tree, tree_type_string), seqs) in &sequences.indexed_tree_seqs {
            aggregated
                .indexed_tree_seqs
                .entry((*tree, tree_type_string.clone()))
                .or_insert_with(Vec::new)
                .extend(seqs.clone());
        }

        // Merge nullification sequences
        for (tree, seqs) in &sequences.nullification_seqs {
            aggregated
                .nullification_seqs
                .entry(*tree)
                .or_insert_with(Vec::new)
                .extend(seqs.clone());
        }

        // Merge batch nullify queue indexes
        aggregated
            .batch_nullify_queue_indexes
            .extend(sequences.batch_nullify_queue_indexes.clone());

        // Merge batch address queue indexes
        for (tree, seqs) in &sequences.batch_address_queue_indexes {
            aggregated
                .batch_address_queue_indexes
                .entry(*tree)
                .or_insert_with(Vec::new)
                .extend(seqs.clone());
        }

        // Merge batch merkle event sequences
        for ((tree, event_type), seqs) in &sequences.batch_merkle_event_seqs {
            aggregated
                .batch_merkle_event_seqs
                .entry((*tree, *event_type))
                .or_insert_with(Vec::new)
                .extend(seqs.clone());
        }

        // Merge out_account leaf indexes
        for (tree, seqs) in &sequences.out_account_leaf_indexes {
            aggregated
                .out_account_leaf_indexes
                .entry(*tree)
                .or_insert_with(Vec::new)
                .extend(seqs.clone());
        }
    }

    aggregated
}

/// Detects gaps from a single StateUpdateSequences struct
pub fn detect_gaps_from_sequences(sequences: &StateUpdateSequences) -> Vec<SequenceGap> {
    let sequences_vec = vec![sequences.clone()];
    detect_all_sequence_gaps(&sequences_vec)
}

/// Comprehensive gap detection function that takes a vector of StateUpdateSequences and returns ALL gaps found
/// Aggregates sequences from multiple StateUpdates and detects gaps across all transactions
pub fn detect_all_sequence_gaps(all_sequences: &[StateUpdateSequences]) -> Vec<SequenceGap> {
    // First aggregate all sequences from multiple StateUpdates
    let sequences = merge_state_update_sequences(all_sequences);

    let mut all_gaps = Vec::new();

    // Check indexed tree updates
    for ((tree_pubkey, tree_type_string), seqs) in &sequences.indexed_tree_seqs {
        let gaps = detect_sequence_gaps_with_metadata(
            seqs,
            Some(*tree_pubkey),
            Some(tree_type_string.clone()),
            StateUpdateFieldType::IndexedTreeUpdate,
        );
        all_gaps.extend(gaps);
    }

    // Check leaf nullifications
    for (tree_pubkey, seqs) in &sequences.nullification_seqs {
        let gaps = detect_sequence_gaps_with_metadata(
            seqs,
            Some(*tree_pubkey),
            None,
            StateUpdateFieldType::LeafNullification,
        );
        all_gaps.extend(gaps);
    }

    // Check batch nullify context
    if !sequences.batch_nullify_queue_indexes.is_empty() {
        let gaps = detect_sequence_gaps_with_metadata(
            &sequences.batch_nullify_queue_indexes,
            None,
            None,
            StateUpdateFieldType::BatchNullifyContext,
        );
        all_gaps.extend(gaps);
    }

    // Check batch new addresses
    for (tree_pubkey, seqs) in &sequences.batch_address_queue_indexes {
        let gaps = detect_sequence_gaps_with_metadata(
            seqs,
            Some(*tree_pubkey),
            None,
            StateUpdateFieldType::BatchNewAddress,
        );
        all_gaps.extend(gaps);
    }

    // Check batch merkle tree events
    for ((tree_pubkey, event_type), seqs) in &sequences.batch_merkle_event_seqs {
        let field_type = match event_type {
            1 => StateUpdateFieldType::BatchMerkleTreeEventAppend,
            2 => StateUpdateFieldType::BatchMerkleTreeEventNullify,
            3 => StateUpdateFieldType::BatchMerkleTreeEventAddressAppend,
            _ => continue,
        };

        let gaps = detect_sequence_gaps_with_metadata(seqs, Some(*tree_pubkey), None, field_type);
        all_gaps.extend(gaps);
    }

    // Check out_account leaf indexes
    for (tree_pubkey, seqs) in &sequences.out_account_leaf_indexes {
        let gaps = detect_sequence_gaps_with_metadata(
            seqs,
            Some(*tree_pubkey),
            None,
            StateUpdateFieldType::OutAccount,
        );
        all_gaps.extend(gaps);
    }

    all_gaps
}

/// Detects gaps in a sequence with full metadata for gap filling
fn detect_sequence_gaps_with_metadata(
    sequences: &[(u64, u64, String)], // (seq, slot, signature)
    tree_pubkey: Option<Pubkey>,
    tree_type_string: Option<String>,
    field_type: StateUpdateFieldType,
) -> Vec<SequenceGap> {
    if sequences.len() < 2 {
        return Vec::new();
    }

    let mut sorted_sequences = sequences.to_vec();
    sorted_sequences.sort_by_key(|(seq, _, _)| *seq);

    let mut gaps = Vec::new();

    for i in 1..sorted_sequences.len() {
        let (prev_seq, prev_slot, prev_sig) = &sorted_sequences[i - 1];
        let (curr_seq, curr_slot, curr_sig) = &sorted_sequences[i];

        if curr_seq - prev_seq > 1 {
            gaps.push(SequenceGap {
                before_slot: *prev_slot,
                after_slot: *curr_slot,
                before_signature: prev_sig.clone(),
                after_signature: curr_sig.clone(),
                tree_pubkey,
                _tree_type_string: tree_type_string.clone(),
                field_type: field_type.clone(),
            });
        }
    }

    gaps
}

#[tokio::test]
async fn test_comprehensive_state_update_validation() -> Result<()> {
    println!("🔍 Testing Comprehensive StateUpdate Sequence Consistency");

    // Load blocks from the created snapshot
    let snapshot_path = "/Users/tsv/Developer/db/snapshot/old";
    let directory_adapter = Arc::new(DirectoryAdapter::from_local_directory(
        snapshot_path.to_string(),
    ));

    println!("📂 Loading snapshot from: {}", snapshot_path);
    let block_stream = load_block_stream_from_directory_adapter(directory_adapter).await;

    // Collect all blocks from the stream
    let all_blocks: Vec<Vec<_>> = block_stream.collect().await;
    let blocks: Vec<_> = all_blocks.into_iter().flatten().collect();

    println!("📦 Processing {} blocks from snapshot", blocks.len());

    // Extract sequences from all StateUpdates with context
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

                    // Extract sequences with context for comprehensive validation
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

    // Detect gaps across all transactions
    let gaps = detect_all_sequence_gaps(&[sequences]);

    // Comprehensive validation summary
    println!("\n🔍 Comprehensive StateUpdate validation results:");
    println!(
        "📊 Total gaps detected across all transactions: {}",
        gaps.len()
    );

    if gaps.is_empty() {
        println!("🎉 All StateUpdate sequences are perfectly consistent!");
    } else {
        // Group gaps by field type for summary
        let mut gaps_by_field: HashMap<StateUpdateFieldType, Vec<&SequenceGap>> = HashMap::new();
        for gap in &gaps {
            gaps_by_field
                .entry(gap.field_type.clone())
                .or_insert_with(Vec::new)
                .push(gap);
            println!(
                "DEBUG: Found gap for tree: {:?}, {:?}",
                gap.tree_pubkey, gap
            );
        }

        println!("⚠️  Gap breakdown by field type:");
        for (field_type, field_gaps) in &gaps_by_field {
            println!("   {:?}: {} gaps", field_type, field_gaps.len());
        }

        println!("⚠️  These gaps may need investigation or gap filling");
    }

    println!("\n🎉 Comprehensive StateUpdate validation completed!");

    Ok(())
}
