use crate::ingester::gap::treetype_seq::TreeTypeSeq;
use crate::ingester::gap::{
    get_current_sequence_state, SequenceEntry, SequenceGap, StateUpdateFieldType, SEQUENCE_STATE,
};
use crate::ingester::parser::indexer_events::MerkleTreeEvent;
use crate::ingester::parser::state_update::StateUpdate;
use crate::ingester::parser::tree_info::QUEUE_TREE_MAPPING;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use tracing::debug;

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

            // Check if this tree should not be in batch operations
            if let Some(info) = QUEUE_TREE_MAPPING.get(&tree_str) {
                // batch_new_addresses should only contain AddressV2 trees
                if info.tree_type != light_compressed_account::TreeType::AddressV2 {
                    tracing::error!(
                        "{:?} wrong tree {tree_str} found in batch_new_addresses \
                        Only AddressV2 trees should be in batch new address operations. \
                        queue_index: {}, slot: {}, signature: {}",
                        info.tree_type,
                        address.queue_index,
                        slot,
                        signature
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

    /// Updates the global sequence state with the latest observed sequences
    pub fn update_sequence_state(&self) {
        let current_state = match SEQUENCE_STATE.read() {
            Ok(state) => state,
            Err(e) => {
                debug!("Failed to acquire read lock for sequence state: {}", e);
                return;
            }
        };

        let mut updates: HashMap<String, TreeTypeSeq> = HashMap::new();

        // Process indexed tree sequences
        for ((tree_pubkey, _tree_type_id), entries) in &self.indexed_tree_seqs {
            if let Some(max_entry) = entries.iter().max_by_key(|e| e.sequence) {
                let tree_str = tree_pubkey.to_string();
                if let Some(info) = QUEUE_TREE_MAPPING.get(&tree_str) {
                    match info.tree_type {
                        light_compressed_account::TreeType::AddressV1 => {
                            updates.insert(tree_str, TreeTypeSeq::AddressV1(max_entry.clone()));
                        }
                        tree_type => {
                            tracing::error!(
                                "Unhandled tree type {:?} for tree {} in indexed_tree_seqs",
                                tree_type,
                                tree_str
                            );
                        }
                    }
                }
            }
        }

        // Process nullification sequences
        for (tree_pubkey, entries) in &self.nullification_seqs {
            if let Some(max_entry) = entries.iter().max_by_key(|e| e.sequence) {
                let tree_str = tree_pubkey.to_string();
                updates.insert(tree_str, TreeTypeSeq::StateV1(max_entry.clone()));
            }
        }

        // Process batch address queue indexes (AddressV2)
        for (tree_pubkey, entries) in &self.batch_address_queue_indexes {
            if let Some(max_entry) = entries.iter().max_by_key(|e| e.sequence) {
                let tree_str = tree_pubkey.to_string();
                debug!(
                    "Updating batch_address_queue_indexes for tree: {}, sequence: {}",
                    tree_str, max_entry.sequence
                );

                updates.insert(
                    tree_str.clone(),
                    TreeTypeSeq::new_address_v2_with_output(
                        current_state.get(&tree_str),
                        max_entry.clone(),
                    ),
                );
            }
        }

        // Process out account leaf indexes
        for (tree_pubkey, entries) in &self.out_account_leaf_indexes {
            if let Some(max_entry) = entries.iter().max_by_key(|e| e.sequence) {
                let tree_str = tree_pubkey.to_string();
                if let Some(info) = QUEUE_TREE_MAPPING.get(&tree_str) {
                    match info.tree_type {
                        light_compressed_account::TreeType::StateV2 => {
                            updates.insert(
                                tree_str.clone(),
                                TreeTypeSeq::new_state_v2_with_output(
                                    current_state.get(&tree_str),
                                    max_entry.clone(),
                                ),
                            );
                        }
                        light_compressed_account::TreeType::StateV1 => {
                            updates.insert(tree_str, TreeTypeSeq::StateV1(max_entry.clone()));
                        }
                        tree_type => {
                            tracing::error!(
                                "Unhandled tree type {:?} for tree {} in out_account_leaf_indexes",
                                tree_type,
                                tree_str
                            );
                        }
                    }
                }
            }
        }

        // Drop read lock before acquiring write lock
        drop(current_state);

        // Apply all updates atomically
        if !updates.is_empty() {
            match SEQUENCE_STATE.write() {
                Ok(mut state) => {
                    for (key, value) in updates {
                        state.insert(key, value);
                    }
                }
                Err(e) => {
                    debug!("Failed to acquire write lock for sequence state: {}", e);
                }
            }
        }
    }

    /// Comprehensive gap detection function that takes a vector of StateUpdateSequences and returns ALL gaps found
    /// Aggregates sequences from multiple StateUpdates and detects gaps across all transactions
    pub fn detect_all_sequence_gaps(&self) -> Vec<SequenceGap> {
        let mut all_gaps = Vec::new();

        // Check indexed tree updates
        for ((tree_pubkey, tree_type_id), seqs) in &self.indexed_tree_seqs {
            debug!(
                "Processing indexed_tree_seqs - tree: {}, tree_type_id: {}",
                tree_pubkey, tree_type_id
            );
            let gaps = StateUpdateSequences::detect_sequence_gaps_with_metadata(
                seqs,
                Some(*tree_pubkey),
                None,
                StateUpdateFieldType::IndexedTreeUpdate,
            );
            all_gaps.extend(gaps);
        }

        // Check leaf nullifications
        for (tree_pubkey, seqs) in &self.nullification_seqs {
            let gaps = StateUpdateSequences::detect_sequence_gaps_with_metadata(
                seqs,
                Some(*tree_pubkey),
                None,
                StateUpdateFieldType::LeafNullification,
            );
            all_gaps.extend(gaps);
        }

        // Check batch nullify context
        for (tree_pubkey, entries) in &self.batch_nullify_queue_indexes {
            if !entries.is_empty() {
                let gaps = StateUpdateSequences::detect_sequence_gaps_with_metadata(
                    entries,
                    Some(*tree_pubkey),
                    None,
                    StateUpdateFieldType::BatchNullifyContext,
                );
                all_gaps.extend(gaps);
            }
        }

        // Check batch new addresses
        for (tree_pubkey, seqs) in &self.batch_address_queue_indexes {
            let gaps = StateUpdateSequences::detect_sequence_gaps_with_metadata(
                seqs,
                Some(*tree_pubkey),
                None,
                StateUpdateFieldType::BatchNewAddress,
            );
            all_gaps.extend(gaps);
        }

        // Check batch merkle tree events
        for ((tree_pubkey, event_type), seqs) in &self.batch_merkle_event_seqs {
            let field_type = match event_type {
                1 => StateUpdateFieldType::BatchMerkleTreeEventAppend,
                2 => StateUpdateFieldType::BatchMerkleTreeEventNullify,
                3 => StateUpdateFieldType::BatchMerkleTreeEventAddressAppend,
                _ => continue,
            };

            let gaps = StateUpdateSequences::detect_sequence_gaps_with_metadata(
                seqs,
                Some(*tree_pubkey),
                None,
                field_type,
            );
            all_gaps.extend(gaps);
        }

        // Check out_account leaf indexes
        for (tree_pubkey, seqs) in &self.out_account_leaf_indexes {
            let gaps = StateUpdateSequences::detect_sequence_gaps_with_metadata(
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
        let (unpacked_start_seq, start_entry) = start_seq.extract_sequence_info(&field_type);

        // Skip gap detection for tree initialization (when unpacked_start_seq == 0)
        // because there's no previous sequence to compare against
        // Also skip if unpacked_start_seq is u64::MAX (no state found)
        if unpacked_start_seq > 0 && unpacked_start_seq != u64::MAX {
            // Check for any missing sequences between global state and the minimum sequence in this block
            let min_seq_in_block = sorted_sequences[0].sequence;
            
            // Check if there's a gap between the global state and the sequences in this block
            // A gap exists if the minimum sequence in the block is more than 1 away from global state
            // AND the missing sequences are not present anywhere in this block
            if min_seq_in_block > unpacked_start_seq.saturating_add(1) {
                // Check if ALL missing sequences are present in this block
                let mut has_real_gap = false;
                for missing_seq in (unpacked_start_seq + 1)..min_seq_in_block {
                    let found = sorted_sequences.iter().any(|e| e.sequence == missing_seq);
                    if !found {
                        has_real_gap = true;
                        break;
                    }
                }
                
                if has_real_gap {
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
            }
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
}

fn merkle_event_to_type_id(event: &MerkleTreeEvent) -> u8 {
    match event {
        MerkleTreeEvent::BatchAppend(_) => 1,
        MerkleTreeEvent::BatchNullify(_) => 2,
        MerkleTreeEvent::BatchAddressAppend(_) => 3,
        _ => 0, // Other event types we don't care about
    }
}
