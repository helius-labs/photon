use crate::ingester::parser::{
    indexer_events::MerkleTreeEvent,
    state_update::StateUpdate,
    tree_info::{TreeTypeSeq, QUEUE_TREE_MAPPING},
};
use lazy_static::lazy_static;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Mutex;
use tracing::debug;

// Global sequence state tracker to maintain latest observed sequences
lazy_static! {
    pub static ref SEQUENCE_STATE: Mutex<HashMap<String, TreeTypeSeq>> = Mutex::new(HashMap::new());
}

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

    pub tree_pubkey: Option<Pubkey>,
    pub field_type: StateUpdateFieldType,
}

#[derive(Debug, Default, Clone)]
pub struct SequenceEntry {
    pub sequence: u64,
    pub slot: u64,
    pub signature: String,
}

#[derive(Debug, Default, Clone)]
pub struct StateUpdateSequences {
    // Sequences with slot and signature information for gap analysis
    indexed_tree_seqs: HashMap<(Pubkey, u64), Vec<SequenceEntry>>, // (tree, tree_type_id) -> entries
    nullification_seqs: HashMap<Pubkey, Vec<SequenceEntry>>,       // tree -> entries
    batch_nullify_queue_indexes: HashMap<Pubkey, Vec<SequenceEntry>>, // tree -> queue_index entries
    batch_address_queue_indexes: HashMap<Pubkey, Vec<SequenceEntry>>, // tree -> queue_index entries
    batch_merkle_event_seqs: HashMap<(Pubkey, u8), Vec<SequenceEntry>>, // (tree_pubkey, event_type) -> entries
    out_account_leaf_indexes: HashMap<Pubkey, Vec<SequenceEntry>>, // tree -> leaf_index entries
}

/// Updates the global sequence state with the latest observed sequences
pub fn update_sequence_state(sequences: &StateUpdateSequences) {
    let mut state = match SEQUENCE_STATE.lock() {
        Ok(state) => state,
        Err(e) => {
            debug!("Failed to acquire sequence state lock: {}", e);
            return;
        }
    };

    for ((tree_pubkey, _tree_type_id), entries) in &sequences.indexed_tree_seqs {
        if let Some(max_entry) = entries.iter().max_by_key(|e| e.sequence) {
            let tree_str = tree_pubkey.to_string();
            if let Some(info) = QUEUE_TREE_MAPPING.get(&tree_str) {
                match info.tree_type {
                    light_compressed_account::TreeType::AddressV1 => {
                        state.insert(tree_str, TreeTypeSeq::AddressV1(max_entry.clone()));
                    }
                    _ => {
                        // Other tree types not handled in indexed_tree_seqs
                    }
                }
            }
        }
    }

    // Update nullification sequences
    for (tree_pubkey, entries) in &sequences.nullification_seqs {
        if let Some(max_entry) = entries.iter().max_by_key(|e| e.sequence) {
            let tree_str = tree_pubkey.to_string();
            state.insert(tree_str, TreeTypeSeq::StateV1(max_entry.clone()));
        }
    }

    // Update batch address queue indexes
    for (tree_pubkey, entries) in &sequences.batch_address_queue_indexes {
        if let Some(max_entry) = entries.iter().max_by_key(|e| e.sequence) {
            let tree_str = tree_pubkey.to_string();
            debug!(
                "Updating batch_address_queue_indexes for tree: {}, sequence: {}",
                tree_str, max_entry.sequence
            );
            let input_queue_entry = if let Some(current_seq) = state.get(&tree_str) {
                if let TreeTypeSeq::AddressV2(input_queue_entry, _) = current_seq {
                    input_queue_entry.clone()
                } else {
                    SequenceEntry::default()
                }
            } else {
                SequenceEntry::default()
            };
            state.insert(
                tree_str,
                TreeTypeSeq::AddressV2(input_queue_entry, max_entry.clone()),
            );
        }
    }

    // Update out account leaf indexes for StateV2 trees
    for (tree_pubkey, entries) in &sequences.out_account_leaf_indexes {
        if let Some(max_entry) = entries.iter().max_by_key(|e| e.sequence) {
            let tree_str = tree_pubkey.to_string();
            if let Some(info) = QUEUE_TREE_MAPPING.get(&tree_str) {
                match info.tree_type {
                    light_compressed_account::TreeType::StateV2 => {
                        let mut seq_context = if let Some(current_seq) = state.get(&tree_str) {
                            if let TreeTypeSeq::StateV2(seq_context) = current_seq {
                                seq_context.clone()
                            } else {
                                crate::ingester::parser::tree_info::StateV2SeqWithContext::default()
                            }
                        } else {
                            crate::ingester::parser::tree_info::StateV2SeqWithContext::default()
                        };
                        seq_context.output_queue_entry = Some(max_entry.clone());
                        state.insert(tree_str, TreeTypeSeq::StateV2(seq_context));
                    }
                    light_compressed_account::TreeType::StateV1 => {
                        state.insert(tree_str, TreeTypeSeq::StateV1(max_entry.clone()));
                    }
                    _ => {}
                }
            }
        }
    }
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
            self.indexed_tree_seqs
                .entry((*tree_pubkey, leaf_update.tree_type as u64))
                .or_insert_with(Vec::new)
                .push(SequenceEntry {
                    sequence: leaf_update.seq,
                    slot,
                    signature: signature.to_string(),
                });
        }

        // Extract leaf nullification sequences
        for nullification in &state_update.leaf_nullifications {
            self.nullification_seqs
                .entry(nullification.tree)
                .or_insert_with(Vec::new)
                .push(SequenceEntry {
                    sequence: nullification.seq,
                    slot,
                    signature: signature.to_string(),
                });
        }

        // Extract batch nullify context queue indexes
        for context in &state_update.batch_nullify_context {
            let tree = Pubkey::new_from_array(context.tree_pubkey.to_bytes());
            self.batch_nullify_queue_indexes
                .entry(tree)
                .or_insert_with(Vec::new)
                .push(SequenceEntry {
                    sequence: context.nullifier_queue_index,
                    slot,
                    signature: signature.to_string(),
                });
        }

        // Extract batch new address queue indexes
        for address in &state_update.batch_new_addresses {
            let tree_str = address.tree.0.to_string();
            debug!(
                "Extracting batch_new_address for tree: {}, queue_index: {}",
                tree_str, address.queue_index
            );

            // Check if this is an AddressV1 tree incorrectly in batch operations
            if let Some(info) = QUEUE_TREE_MAPPING.get(&tree_str) {
                if info.tree_type == light_compressed_account::TreeType::AddressV1 {
                    tracing::error!(
                        "AddressV1 tree {tree_str} found in batch_new_addresses - this should not happen! \
                        queue_index: {}, slot: {}, signature: {}",
                        address.queue_index, slot, signature
                    );
                    // Skip this invalid data
                    continue;
                }
            }

            self.batch_address_queue_indexes
                .entry(address.tree.0)
                .or_insert_with(Vec::new)
                .push(SequenceEntry {
                    sequence: address.queue_index,
                    slot,
                    signature: signature.to_string(),
                });
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
                        .push(SequenceEntry {
                            sequence: *seq,
                            slot,
                            signature: signature.to_string(),
                        });
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
                .push(SequenceEntry {
                    sequence: leaf_index,
                    slot,
                    signature: signature.to_string(),
                });
        }
    }
}

/// Comprehensive gap detection function that takes a vector of StateUpdateSequences and returns ALL gaps found
/// Aggregates sequences from multiple StateUpdates and detects gaps across all transactions
pub fn detect_all_sequence_gaps(sequences: &StateUpdateSequences) -> Vec<SequenceGap> {
    let mut all_gaps = Vec::new();

    // Check indexed tree updates
    for ((tree_pubkey, tree_type_id), seqs) in &sequences.indexed_tree_seqs {
        debug!(
            "Processing indexed_tree_seqs - tree: {}, tree_type_id: {}",
            tree_pubkey, tree_type_id
        );
        let gaps = detect_sequence_gaps_with_metadata(
            seqs,
            Some(*tree_pubkey),
            None,
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
    for (tree_pubkey, entries) in &sequences.batch_nullify_queue_indexes {
        if !entries.is_empty() {
            let gaps = detect_sequence_gaps_with_metadata(
                entries,
                Some(*tree_pubkey),
                None,
                StateUpdateFieldType::BatchNullifyContext,
            );
            all_gaps.extend(gaps);
        }
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
    sequences: &[SequenceEntry],
    tree_pubkey: Option<Pubkey>,
    queue_pubkey: Option<Pubkey>,
    field_type: StateUpdateFieldType,
) -> Vec<SequenceGap> {
    if sequences.len() < 2 {
        return Vec::new();
    }

    let mut sorted_sequences = sequences.to_vec();
    sorted_sequences.sort_by_key(|entry| entry.sequence);
    let mut gaps = Vec::new();

    let start_seq = get_current_sequence_state(tree_pubkey, queue_pubkey, &field_type);
    let (unpacked_start_seq, start_entry) = extract_sequence_info(&start_seq, &field_type);

    // Skip gap detection for tree initialization (when unpacked_start_seq == 0)
    // because there's no previous sequence to compare against
    // Also skip if unpacked_start_seq is u64::MAX (no state found)
    if unpacked_start_seq > 0
        && unpacked_start_seq != u64::MAX
        && sorted_sequences[0].sequence > unpacked_start_seq.saturating_add(1)
    {
        let (before_slot, before_signature) = if let Some(entry) = start_entry {
            (entry.slot, entry.signature)
        } else {
            (0, String::new())
        };

        gaps.push(SequenceGap {
            before_slot,
            after_slot: sorted_sequences[0].slot,
            before_signature,
            after_signature: sorted_sequences[0].signature.clone(),
            tree_pubkey,
            field_type: field_type.clone(),
        });
    }

    for i in 1..sorted_sequences.len() {
        let prev_entry = &sorted_sequences[i - 1];
        let curr_entry = &sorted_sequences[i];

        if curr_entry.sequence - prev_entry.sequence > 1 {
            gaps.push(SequenceGap {
                before_slot: prev_entry.slot,
                after_slot: curr_entry.slot,
                before_signature: prev_entry.signature.clone(),
                after_signature: curr_entry.signature.clone(),
                tree_pubkey,
                field_type: field_type.clone(),
            });
        }
    }

    gaps
}

/// Gets the current sequence state from the global state tracker
fn get_current_sequence_state(
    tree_pubkey: Option<Pubkey>,
    queue_pubkey: Option<Pubkey>,
    field_type: &StateUpdateFieldType,
) -> TreeTypeSeq {
    let state = match SEQUENCE_STATE.lock() {
        Ok(state) => state,
        Err(e) => {
            debug!("Failed to acquire sequence state lock: {}", e);
            return TreeTypeSeq::default();
        }
    };

    if let Some(tree) = tree_pubkey {
        let tree_str = tree.to_string();
        if let Some(current_seq) = state.get(&tree_str) {
            debug!(
                "Using current sequence state for tree {}: {:?}",
                tree_str, current_seq
            );
            current_seq.clone()
        } else {
            debug!("No current sequence state found for tree {}", tree_str);
            TreeTypeSeq::default()
        }
    } else if let Some(queue_pubkey) = queue_pubkey {
        let queue_str = queue_pubkey.to_string();
        if let Some(current_seq) = state.get(&queue_str) {
            current_seq.clone()
        } else {
            debug!("No current sequence state found for queue {}", queue_str);
            TreeTypeSeq::default()
        }
    } else {
        debug!(
            "No tree/queue pubkey provided for field_type: {:?}",
            field_type
        );
        TreeTypeSeq::default()
    }
}

/// Extracts sequence information based on field type and tree type
///
/// Returns `(sequence_number, optional_entry)` where:
/// - `u64::MAX` indicates invalid state - tree type mismatch or unexpected configuration.
///   Gap detection will be skipped entirely for these cases.
/// - `0` indicates valid initial state - the expected tree type exists but the specific
///   sequence entry hasn't been initialized yet. Gap detection remains active.
/// - Any other value represents an actual sequence number from existing state.
///
/// This distinction is important because:
/// - Invalid configurations (u64::MAX) should not trigger false-positive gap alerts
/// - Valid but uninitialized sequences (0) should still detect gaps if the first
///   observed sequence is > 1
fn extract_sequence_info(
    start_seq: &TreeTypeSeq,
    field_type: &StateUpdateFieldType,
) -> (u64, Option<SequenceEntry>) {
    match field_type {
        StateUpdateFieldType::IndexedTreeUpdate => match start_seq {
            TreeTypeSeq::AddressV1(entry) => {
                debug!("IndexedTreeUpdate with AddressV1, seq: {}", entry.sequence);
                (entry.sequence, Some(entry.clone()))
            }
            _ => {
                debug!(
                    "IndexedTreeUpdate with unsupported tree type: {:?}",
                    start_seq
                );
                (u64::MAX, None)
            }
        },
        StateUpdateFieldType::BatchMerkleTreeEventAddressAppend
        | StateUpdateFieldType::BatchNewAddress => {
            if let TreeTypeSeq::AddressV2(_, entry) = start_seq {
                (entry.sequence, Some(entry.clone()))
            } else {
                debug!(
                    "Expected AddressV2 for {:?}, got {:?}",
                    field_type, start_seq
                );
                (u64::MAX, None)
            }
        }
        StateUpdateFieldType::BatchMerkleTreeEventAppend
        | StateUpdateFieldType::BatchMerkleTreeEventNullify => {
            if let TreeTypeSeq::StateV2(seq_context) = start_seq {
                if let Some(entry) = &seq_context.batch_event_entry {
                    (entry.sequence, Some(entry.clone()))
                } else {
                    (0, None)
                }
            } else {
                debug!("Expected StateV2 for {:?}, got {:?}", field_type, start_seq);
                (u64::MAX, None)
            }
        }
        StateUpdateFieldType::LeafNullification => {
            if let TreeTypeSeq::StateV1(entry) = start_seq {
                (entry.sequence, Some(entry.clone()))
            } else {
                debug!(
                    "Expected StateV1 for LeafNullification, got {:?}",
                    start_seq
                );
                (u64::MAX, None)
            }
        }
        StateUpdateFieldType::OutAccount => match start_seq {
            TreeTypeSeq::StateV1(entry) => (entry.sequence, Some(entry.clone())),
            TreeTypeSeq::StateV2(seq_context) => {
                if let Some(entry) = &seq_context.output_queue_entry {
                    (entry.sequence, Some(entry.clone()))
                } else {
                    (0, None)
                }
            }
            _ => {
                debug!("Expected StateV1/V2 for OutAccount, got {:?}", start_seq);
                (u64::MAX, None)
            }
        },
        StateUpdateFieldType::BatchNullifyContext => {
            if let TreeTypeSeq::StateV2(seq_context) = start_seq {
                if let Some(entry) = &seq_context.input_queue_entry {
                    (entry.sequence, Some(entry.clone()))
                } else {
                    (0, None)
                }
            } else {
                debug!(
                    "Expected StateV2 for BatchNullifyContext, got {:?}",
                    start_seq
                );
                (u64::MAX, None)
            }
        }
    }
}
