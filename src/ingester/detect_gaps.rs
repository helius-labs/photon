use crate::ingester::parser::{
     indexer_events::MerkleTreeEvent, state_update::StateUpdate, tree_info::{TreeTypeSeq, QUEUE_TREE_MAPPING}
};
use solana_pubkey::Pubkey;
use std::collections::HashMap;

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


#[derive(Debug, Default, Clone)]
pub struct StateUpdateSequences {
    // Sequences with slot and signature information for gap analysis
    indexed_tree_seqs: HashMap<(Pubkey, u64), Vec<(u64, u64, String)>>, // (tree, tree_type_id) -> (seq, slot, signature)
    nullification_seqs: HashMap<Pubkey, Vec<(u64, u64, String)>>, // tree -> (seq, slot, signature)
    batch_nullify_queue_indexes: Vec<(u64, u64, String)>, // (queue_index, slot, signature)
    batch_address_queue_indexes: HashMap<Pubkey, Vec<(u64, u64, String)>>, // tree -> (queue_index, slot, signature)
    batch_merkle_event_seqs: HashMap<(Pubkey, u8), Vec<(u64, u64, String)>>, // (tree_pubkey, event_type) -> (seq, slot, signature)
    out_account_leaf_indexes: HashMap<Pubkey, Vec<(u64, u64, String)>>, // tree -> (leaf_index, slot, signature)
}

impl StateUpdateSequences {
/// Extracts sequences from a StateUpdate with slot and signature context
pub fn extract_state_update_sequences(&mut self, state_update: &StateUpdate, slot: u64, signature: &str) {
    
    // Extract indexed tree sequences
    for ((tree_pubkey, _), leaf_update) in &state_update.indexed_merkle_tree_updates {
        self.indexed_tree_seqs
            .entry((*tree_pubkey, leaf_update.tree_type as u64))
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
        self.batch_nullify_queue_indexes.push((context.nullifier_queue_index, slot, signature.to_string()));
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


/// Detects gaps from a single StateUpdateSequences struct
pub fn detect_gaps_from_sequences(sequences: &StateUpdateSequences) -> Vec<SequenceGap> {
    detect_all_sequence_gaps(sequences)
}

/// Comprehensive gap detection function that takes a vector of StateUpdateSequences and returns ALL gaps found
/// Aggregates sequences from multiple StateUpdates and detects gaps across all transactions
pub fn detect_all_sequence_gaps(sequences: &StateUpdateSequences) -> Vec<SequenceGap> {  

    let mut all_gaps = Vec::new();
    
    // Check indexed tree updates
    for ((tree_pubkey, _tree_type_id), seqs) in &sequences.indexed_tree_seqs {
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
        
        let gaps = detect_sequence_gaps_with_metadata(
            seqs,
            Some(*tree_pubkey),
            None,
            field_type,
        );
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
    queue_pubkey: Option<Pubkey>,
    field_type: StateUpdateFieldType,
) -> Vec<SequenceGap> {
    if sequences.len() < 2 {
        return Vec::new();
    }
    
    let mut sorted_sequences = sequences.to_vec();
    sorted_sequences.sort_by_key(|(seq, _, _)| *seq);
    
    let mut gaps = Vec::new();
    let start_seq =  if let Some(tree) = tree_pubkey {
        QUEUE_TREE_MAPPING
            .get(&tree.to_string())
            .map(|info| info.seq)
            .unwrap()
    } else if let Some(queue_pubkey) = queue_pubkey {
        QUEUE_TREE_MAPPING
            .get(&queue_pubkey.to_string())
            .map(|info| info.seq)
            .unwrap()
    } else {
       unimplemented!("No tree or queue pubkey provided for gap detection");
    };

    let unpacked_start_seq = match field_type {
        StateUpdateFieldType::IndexedTreeUpdate => {
          if let TreeTypeSeq::AddressV1(seq) = start_seq {
              seq
          } else {
              unimplemented!("Unsupported tree type for gap detection");
          }
        },
        StateUpdateFieldType::BatchMerkleTreeEventAddressAppend => {
          if let TreeTypeSeq::AddressV2(_,seq ) = start_seq {
           seq
          } else {
              unimplemented!("Unsupported tree type for gap detection");
          }
        },StateUpdateFieldType::BatchNewAddress => {
          if let TreeTypeSeq::AddressV2(seq,_ ) = start_seq {
           seq
          } else {
              unimplemented!("Unsupported tree type for gap detection");
          }
        },
         StateUpdateFieldType::BatchMerkleTreeEventAppend => {
          if let TreeTypeSeq::StateV2(seq) = start_seq {
              seq.batch_event_seq
          } else {
              unimplemented!("Unsupported tree type for gap detection");
          }
        },
         StateUpdateFieldType::BatchMerkleTreeEventNullify => {
          if let TreeTypeSeq::StateV2(seq) = start_seq {
              seq.batch_event_seq
          } else {
              unimplemented!("Unsupported tree type for gap detection");
          }
        },
           StateUpdateFieldType::LeafNullification => {
          if let TreeTypeSeq::StateV1(seq) = start_seq {
              seq
          } else {
              unimplemented!("Unsupported tree type for gap detection");
          }
        },
           StateUpdateFieldType::OutAccount => {
          if let TreeTypeSeq::StateV1(seq) = start_seq {
              seq
          } else if let TreeTypeSeq::StateV2(seq) = start_seq {
              seq.output_queue_index
          } else {
              unimplemented!("Unsupported tree type for gap detection");
          }
        },
           StateUpdateFieldType::BatchNullifyContext => {
           if let TreeTypeSeq::StateV2(seq) = start_seq {
              seq.input_queue_index
          } else {
              unimplemented!("Unsupported tree type for gap detection");
          }
        },
    };

    if sorted_sequences[0].0 > unpacked_start_seq {
        gaps.push(SequenceGap {
            before_slot: 0, // No previous slot available
            after_slot: sorted_sequences[0].1,
            before_signature: String::new(), // No previous signature available
            after_signature: sorted_sequences[0].2.clone(),
            tree_pubkey,
            field_type: field_type.clone(),
        });
    }
    for i in 1..sorted_sequences.len() {
        let (prev_seq, prev_slot, prev_sig) = &sorted_sequences[i-1];
        let (curr_seq, curr_slot, curr_sig) = &sorted_sequences[i];
        
        if curr_seq - prev_seq > 1 {
            gaps.push(SequenceGap {
                before_slot: *prev_slot,
                after_slot: *curr_slot,
                before_signature: prev_sig.clone(),
                after_signature: curr_sig.clone(),
                tree_pubkey,
                field_type: field_type.clone(),
            });
        }
    }
    
    gaps
}