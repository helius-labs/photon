use std::collections::{HashMap, HashSet};

use solana_sdk::signature::Signature;

use crate::common::typedefs::account::Account;

use crate::common::typedefs::hash::Hash;

use crate::ingester::parser::indexer_events::PathNode;

#[derive(Debug, Clone)]
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
}

#[derive(Hash, PartialEq, Eq)]
pub struct AccountTransaction {
    pub hash: Hash,
    pub signature: Signature,
    pub slot: u64,
}

#[derive(Default)]
/// Representation of state update of the compression system that is optimal for simple persistance.
pub struct StateUpdate {
    pub in_accounts: HashSet<Hash>,
    pub out_accounts: Vec<Account>,
    pub path_nodes: HashMap<([u8; 32], u32), EnrichedPathNode>,
    pub account_transactions: HashSet<AccountTransaction>,
}

impl StateUpdate {
    pub fn new() -> Self {
        StateUpdate::default()
    }

    pub fn prune_redundant_updates(&mut self) {
        // NOTE: For snapshot verification, we might need to persist accounts data until accounts are
        //       removed from the tree through the nullifier crank.
        self.out_accounts
            .retain(|a| !self.in_accounts.contains(&a.hash));
    }

    pub fn merge_updates(updates: Vec<StateUpdate>) -> StateUpdate {
        let mut merged = StateUpdate::default();
        for update in updates {
            merged.in_accounts.extend(update.in_accounts);
            merged.out_accounts.extend(update.out_accounts);
            merged
                .account_transactions
                .extend(update.account_transactions);

            for (key, node) in update.path_nodes {
                if let Some(existing) = merged.path_nodes.get_mut(&key) {
                    if (*existing).seq < node.seq {
                        *existing = node;
                    }
                } else {
                    merged.path_nodes.insert(key, node);
                }
            }
        }
        merged
    }
}
