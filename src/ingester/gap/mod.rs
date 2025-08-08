use lazy_static::lazy_static;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::RwLock;
use tracing::{debug, info};

mod rewind;
mod sequences;
mod treetype_seq;

use crate::ingester::gap::treetype_seq::TreeTypeSeq;

pub use rewind::{RewindCommand, RewindController};
pub use sequences::StateUpdateSequences;

// Global sequence state tracker to maintain latest observed sequences
lazy_static! {
    pub static ref SEQUENCE_STATE: RwLock<HashMap<String, TreeTypeSeq>> =
        RwLock::new(HashMap::new());
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

/// Clears the global sequence state - used after rewind to re-learn sequences
pub fn clear_sequence_state() {
    match SEQUENCE_STATE.write() {
        Ok(mut state) => {
            state.clear();
            info!("Cleared sequence state after rewind");
        }
        Err(e) => {
            debug!("Failed to acquire write lock to clear sequence state: {}", e);
        }
    }
}

/// Gets the current sequence state from the global state tracker
pub fn get_current_sequence_state(
    tree_pubkey: Option<Pubkey>,
    queue_pubkey: Option<Pubkey>,
    field_type: &StateUpdateFieldType,
) -> TreeTypeSeq {
    let state = match SEQUENCE_STATE.read() {
        Ok(state) => state,
        Err(e) => {
            debug!("Failed to acquire sequence state read lock: {}", e);
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
