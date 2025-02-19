use std::collections::{HashMap, HashSet};
use borsh::{BorshDeserialize, BorshSerialize};
use light_batched_merkle_tree::event::{BatchAppendEvent, BatchNullifyEvent};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use crate::common::typedefs::account::AccountV2;
use crate::common::typedefs::hash::Hash;
use super::indexer_events::{MerkleTreeSequenceNumber, RawIndexedElement};

#[derive(BorshDeserialize, BorshSerialize, Debug, Clone, PartialEq, Eq)]
pub struct PathNode {
    pub node: [u8; 32],
    pub index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrichedPathNode {
    pub node: PathNode,
    pub slot: u64,
    pub tree: [u8; 32],
    pub seq: u64,
    pub level: usize,
    pub tree_depth: usize,
    pub leaf_index: Option<u32>,
}

pub struct PathUpdate {
    pub tree: [u8; 32],
    pub path: Vec<PathNode>,
    pub seq: u64,
}

#[derive(Hash, Eq, Clone, PartialEq, Debug)]
pub struct Transaction {
    pub signature: Signature,
    pub slot: u64,
    pub uses_compression: bool,
    pub error: Option<String>,
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub struct AccountTransaction {
    pub hash: Hash,
    pub signature: Signature,
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub struct LeafNullification {
    pub tree: Pubkey,
    pub leaf_index: u64,
    pub seq: u64,
    pub signature: Signature,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct IndexedTreeLeafUpdate {
    pub tree: Pubkey,
    pub leaf: RawIndexedElement,
    pub hash: [u8; 32],
    pub seq: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct AccountContext {
    pub tx_hash: Hash,
    pub account: Hash,
    pub nullifier: Hash,
    pub nullifier_queue_index: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct InputContext {
    pub accounts: Vec<AccountContext>,
    pub in_seq_numbers: Vec<MerkleTreeSequenceNumber>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
/// Representation of state update of the compression system that is optimal for simple persistence.
pub struct StateUpdate {
    pub in_accounts: HashSet<Hash>,
    pub in_seq_numbers: Vec<MerkleTreeSequenceNumber>,
    pub out_accounts: Vec<AccountV2>,
    pub account_transactions: HashSet<AccountTransaction>,
    pub transactions: HashSet<Transaction>,
    pub leaf_nullifications: HashSet<LeafNullification>,
    pub indexed_merkle_tree_updates: HashMap<(Pubkey, u64), IndexedTreeLeafUpdate>,

    pub batch_append: Vec<BatchAppendEvent>,
    pub batch_nullify: Vec<BatchNullifyEvent>,

    // TODO: switch to hashset
    pub input_context: Vec<InputContext>,
}

impl StateUpdate {
    pub fn new() -> Self {
        StateUpdate::default()
    }

    pub fn merge_updates(updates: Vec<StateUpdate>) -> StateUpdate {
        let mut merged = StateUpdate::default();
        // TODO: remove assert after tx_hash and in_seq_numbers are associated with in_accounts
        // assert!(updates.iter().filter(|update| update.tx_hash != Hash::default()).count() <= 1);
        for update in updates {
            // legacy
            merged.in_seq_numbers.extend(update.in_seq_numbers);
            merged.in_accounts.extend(update.in_accounts);
            merged.out_accounts.extend(update.out_accounts);
            merged
                .account_transactions
                .extend(update.account_transactions);
            merged.transactions.extend(update.transactions);
            merged
                .leaf_nullifications
                .extend(update.leaf_nullifications);

            for (key, value) in update.indexed_merkle_tree_updates {
                // Insert only if the seq is higher.
                if let Some(existing) = merged.indexed_merkle_tree_updates.get_mut(&key) {
                    if value.seq > existing.seq {
                        *existing = value;
                    }
                } else {
                    merged.indexed_merkle_tree_updates.insert(key, value);
                }
            }

            // batch updates
            merged.input_context.extend(update.input_context);
            merged.batch_append.extend(update.batch_append);
            merged.batch_nullify.extend(update.batch_nullify);
        }
        merged
    }
}
