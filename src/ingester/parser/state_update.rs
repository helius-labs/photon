use super::{indexer_events::RawIndexedElement, merkle_tree_events_parser::IndexedBatchEvents};
use crate::common::typedefs::account::AccountWithContext;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use borsh::{BorshDeserialize, BorshSerialize};
use jsonrpsee_core::Serialize;
use light_compressed_account::indexer_event::event::BatchNullifyContext;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::collections::{HashMap, HashSet};
use utoipa::ToSchema;

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AddressQueueUpdate {
    pub tree: SerializablePubkey,
    pub address: [u8; 32],
    pub queue_index: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
/// Representation of state update of the compression system that is optimal for simple persistence.
pub struct StateUpdate {
    pub in_accounts: HashSet<Hash>,
    pub out_accounts: Vec<AccountWithContext>,
    pub account_transactions: HashSet<AccountTransaction>,
    pub transactions: HashSet<Transaction>,
    pub leaf_nullifications: HashSet<LeafNullification>,
    pub indexed_merkle_tree_updates: HashMap<(Pubkey, u64), IndexedTreeLeafUpdate>,
    pub batch_events: IndexedBatchEvents,
    pub input_context: Vec<BatchNullifyContext>,
    pub addresses: Vec<AddressQueueUpdate>,
}

impl StateUpdate {
    pub fn new() -> Self {
        StateUpdate::default()
    }

    pub fn merge_updates(updates: Vec<StateUpdate>) -> StateUpdate {
        let mut merged = StateUpdate::default();

        for update in updates {
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

            merged.addresses.extend(update.addresses);

            merged.input_context.extend(update.input_context);

            for (key, events) in update.batch_events {
                if let Some(existing_events) = merged.batch_events.get_mut(&key) {
                    existing_events.extend(events);
                } else {
                    merged.batch_events.insert(key, events);
                }
            }
        }

        merged
    }
}
