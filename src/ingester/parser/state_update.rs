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

pub struct AccountTransaction {
    pub hash: Hash,
    pub signature: Signature,
    pub closure: bool,
    pub slot: u64,
}

#[derive(Default)]
/// Representation of state update of the compression system that is optimal for simple persistance.
pub struct StateUpdate {
    pub in_accounts: Vec<Account>,
    pub out_accounts: Vec<Account>,
    pub path_nodes: Vec<EnrichedPathNode>,
    pub account_transactions: Vec<AccountTransaction>,
}

impl StateUpdate {
    pub fn new() -> Self {
        StateUpdate::default()
    }

    pub fn prune_redundant_updates(&mut self) {
        let in_account_set: HashSet<Hash> =
            self.in_accounts.iter().map(|a| a.hash.clone()).collect();
        // NOTE: For snapshot verification, we might need to persist accounts data until accounts are
        //       removed from the tree through the nullifier crank.
        self.out_accounts
            .retain(|a| !in_account_set.contains(&a.hash));

        // TODO: Use seq instead of slot.

        // Assuming the type of `path_nodes` and fields inside `node` such as `tree`, `node.index`, and `seq`.
        let mut latest_nodes: HashMap<([u8; 32], u32), EnrichedPathNode> = HashMap::new();

        for node in self.path_nodes.drain(..) {
            let key = (node.tree, node.node.index);
            if let Some(existing) = latest_nodes.get_mut(&key) {
                if (*existing).seq < node.seq {
                    *existing = node;
                }
            } else {
                latest_nodes.insert(key, node);
            }
        }
        self.path_nodes = latest_nodes.into_values().collect();
    }

    pub fn merge_updates(updates: Vec<StateUpdate>) -> StateUpdate {
        let mut merged = StateUpdate::default();
        for update in updates {
            merged.in_accounts.extend(update.in_accounts);
            merged.out_accounts.extend(update.out_accounts);
            merged.path_nodes.extend(update.path_nodes);
            merged
                .account_transactions
                .extend(update.account_transactions);
        }
        merged
    }
}
