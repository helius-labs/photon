use crate::ingester::parser::{
    indexer_events::MerkleTreeEvent,
    state_update::StateUpdate,
    tree_info::{TreeTypeSeq, QUEUE_TREE_MAPPING},
};
use lazy_static::lazy_static;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Mutex;

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

    // Tree/context metadata
    pub tree_pubkey: Option<Pubkey>, // Tree pubkey (unified for all tree operations)
    //  pub tree_type_string: Option<String>, // Tree type string (for indexed tree updates)
    pub field_type: StateUpdateFieldType,
}

#[derive(Debug, Clone)]
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
    let mut state = SEQUENCE_STATE.lock().unwrap();

    // Update indexed tree sequences
    for ((tree_pubkey, _tree_type_id), entries) in &sequences.indexed_tree_seqs {
        if let Some(max_entry) = entries.iter().max_by_key(|e| e.sequence) {
            let tree_str = tree_pubkey.to_string();
            // Check the actual tree type from the mapping
            if let Some(info) = QUEUE_TREE_MAPPING.get(&tree_str) {
                match info.tree_type {
                    light_compressed_account::TreeType::AddressV1 => {
                        state.insert(tree_str, TreeTypeSeq::AddressV1(max_entry.clone()));
                    }
                    light_compressed_account::TreeType::StateV1 => {
                        state.insert(tree_str, TreeTypeSeq::StateV1(max_entry.clone()));
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
            println!(
                "DEBUG: Updating batch_address_queue_indexes for tree: {}, sequence: {}",
                tree_str, max_entry.sequence
            );
            let input_queue_entry = if let Some(current_seq) = state.get(&tree_str) {
                if let TreeTypeSeq::AddressV2(input_queue_entry, _) = current_seq {
                    input_queue_entry.clone()
                } else {
                    SequenceEntry {
                        sequence: 0,
                        slot: 0,
                        signature: String::new(),
                    }
                }
            } else {
                SequenceEntry {
                    sequence: 0,
                    slot: 0,
                    signature: String::new(),
                }
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
                    _ => {
                        state.insert(tree_str, TreeTypeSeq::StateV1(max_entry.clone()));
                    }
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
            println!(
                "DEBUG: Extracting batch_new_address for tree: {}, queue_index: {}",
                tree_str, address.queue_index
            );

            // Check if this is an AddressV1 tree incorrectly in batch operations
            if let Some(info) = QUEUE_TREE_MAPPING.get(&tree_str) {
                if info.tree_type == light_compressed_account::TreeType::AddressV1 {
                    println!("ERROR: AddressV1 tree {} found in batch_new_addresses - this should not happen!", tree_str);
                    println!(
                        "  queue_index: {}, slot: {}, signature: {}",
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

/// Detects gaps from a single StateUpdateSequences struct
pub fn detect_gaps_from_sequences(sequences: &StateUpdateSequences) -> Vec<SequenceGap> {
    detect_all_sequence_gaps(sequences)
}

/// Comprehensive gap detection function that takes a vector of StateUpdateSequences and returns ALL gaps found
/// Aggregates sequences from multiple StateUpdates and detects gaps across all transactions
pub fn detect_all_sequence_gaps(sequences: &StateUpdateSequences) -> Vec<SequenceGap> {
    let mut all_gaps = Vec::new();

    // Check indexed tree updates
    for ((tree_pubkey, tree_type_id), seqs) in &sequences.indexed_tree_seqs {
        println!(
            "DEBUG: Processing indexed_tree_seqs - tree: {}, tree_type_id: {}",
            tree_pubkey, tree_type_id
        );
        let gaps = detect_sequence_gaps_with_metadata(
            seqs,
            Some(*tree_pubkey),
            None, // TODO: use queue pubkey if we only have queue pubkey such as for outputs of batched trees
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
    if field_type == StateUpdateFieldType::BatchNullifyContext {
        // For batch nullify context, we don't have tree or queue pubkey, so we can't detect gaps
        return Vec::new();
    }
    if sequences.len() < 2 {
        return Vec::new();
    }

    let mut sorted_sequences = sequences.to_vec();
    sorted_sequences.sort_by_key(|entry| entry.sequence);
    let mut gaps = Vec::new();
    let start_seq = if let Some(tree) = tree_pubkey {
        let tree_str = tree.to_string();

        // First check current sequence state, fall back to initial mapping
        let state = SEQUENCE_STATE.lock().unwrap();
        if let Some(current_seq) = state.get(&tree_str) {
            println!(
                "DEBUG: Using current sequence state for tree {}: {:?}",
                tree_str, current_seq
            );
            current_seq.clone()
        } else if let Some(info) = QUEUE_TREE_MAPPING.get(&tree_str) {
            println!(
                "DEBUG: Using initial mapping for tree {}: {:?}",
                tree_str, info.seq
            );
            info.seq.clone()
        } else {
            println!("Tree {} not found in QUEUE_TREE_MAPPING", tree_str);
            println!(
                "Available keys: {:?}",
                QUEUE_TREE_MAPPING.keys().collect::<Vec<_>>()
            );
            unimplemented!("Tree not found in mapping");
        }
    } else if let Some(queue_pubkey) = queue_pubkey {
        let queue_str = queue_pubkey.to_string();
        let state = SEQUENCE_STATE.lock().unwrap();
        if let Some(current_seq) = state.get(&queue_str) {
            current_seq.clone()
        } else {
            QUEUE_TREE_MAPPING
                .get(&queue_str)
                .map(|info| info.seq.clone())
                .unwrap()
        }
    } else {
        println!("field_type: {:?}", field_type);
        println!(
            "tree_pubkey: {:?}, queue_pubkey: {:?}",
            tree_pubkey, queue_pubkey
        );
        unimplemented!("No tree or queue pubkey provided for gap detection");
    };

    let (unpacked_start_seq, start_entry) = match field_type {
        StateUpdateFieldType::IndexedTreeUpdate => match start_seq {
            TreeTypeSeq::AddressV1(entry) => {
                println!(
                    "DEBUG: IndexedTreeUpdate with AddressV1, seq: {}",
                    entry.sequence
                );
                (entry.sequence, Some(entry))
            }
            _ => {
                println!(
                    "DEBUG: IndexedTreeUpdate with unsupported tree type: {:?}",
                    start_seq
                );
                unimplemented!("Unsupported tree type for gap detection");
            }
        },
        StateUpdateFieldType::BatchMerkleTreeEventAddressAppend => {
            if let TreeTypeSeq::AddressV2(_, entry) = start_seq {
                (entry.sequence, Some(entry))
            } else {
                unimplemented!("Unsupported tree type for gap detection");
            }
        }
        StateUpdateFieldType::BatchNewAddress => {
            if let TreeTypeSeq::AddressV2(_, entry) = start_seq {
                (entry.sequence, Some(entry))
            } else {
                unimplemented!("Unsupported tree type for gap detection");
            }
        }
        StateUpdateFieldType::BatchMerkleTreeEventAppend => {
            if let TreeTypeSeq::StateV2(seq_context) = start_seq {
                if let Some(entry) = &seq_context.batch_event_entry {
                    (entry.sequence, Some(entry.clone()))
                } else {
                    (0, None)
                }
            } else {
                unimplemented!("Unsupported tree type for gap detection");
            }
        }
        StateUpdateFieldType::BatchMerkleTreeEventNullify => {
            if let TreeTypeSeq::StateV2(seq_context) = start_seq {
                if let Some(entry) = &seq_context.batch_event_entry {
                    (entry.sequence, Some(entry.clone()))
                } else {
                    (0, None)
                }
            } else {
                unimplemented!("Unsupported tree type for gap detection");
            }
        }
        StateUpdateFieldType::LeafNullification => {
            if let TreeTypeSeq::StateV1(entry) = start_seq {
                (entry.sequence, Some(entry))
            } else {
                unimplemented!("Unsupported tree type for gap detection");
            }
        }
        StateUpdateFieldType::OutAccount => {
            if let TreeTypeSeq::StateV1(entry) = start_seq {
                (entry.sequence, Some(entry))
            } else if let TreeTypeSeq::StateV2(seq_context) = start_seq {
                if let Some(entry) = &seq_context.output_queue_entry {
                    (entry.sequence, Some(entry.clone()))
                } else {
                    (0, None)
                }
            } else {
                unimplemented!("Unsupported tree type for gap detection");
            }
        }
        StateUpdateFieldType::BatchNullifyContext => {
            if let TreeTypeSeq::StateV2(seq_context) = start_seq {
                if let Some(entry) = &seq_context.input_queue_entry {
                    (entry.sequence, Some(entry.clone()))
                } else {
                    (0, None)
                }
            } else {
                unimplemented!("Unsupported tree type for gap detection");
            }
        }
    };

    // Skip gap detection for tree initialization (when unpacked_start_seq == 0)
    // because there's no previous sequence to compare against
    if unpacked_start_seq > 0 && sorted_sequences[0].sequence > unpacked_start_seq + 1 {
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
