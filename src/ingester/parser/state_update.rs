use std::collections::HashSet;

use solana_sdk::pubkey::Pubkey;

use crate::ingester::parser::indexer_events::{CompressedAccount, PathNode};

#[derive(Debug, Clone)]
pub struct EnrichedAccount {
    pub account: CompressedAccount,
    pub tree: Pubkey,
    pub seq: Option<u64>,
    pub hash: [u8; 32],
    pub slot: u64,
    pub leaf_index: Option<u32>,
}

pub struct EnrichedPathNode {
    pub node: PathNode,
    pub slot: u64,
    pub tree: [u8; 32],
    pub seq: u64,
    pub level: usize,
    pub tree_depth: usize,
}

pub struct PathUpdate {
    pub tree: [u8; 32],
    pub path: Vec<PathNode>,
    pub seq: u64,
}

#[derive(Default)]
/// Representation of state update of the compression system that is optimal for simple persistance.
pub struct StateUpdate {
    pub in_accounts: Vec<EnrichedAccount>,
    pub out_accounts: Vec<EnrichedAccount>,
    pub path_nodes: Vec<EnrichedPathNode>,
}

impl StateUpdate {
    pub fn new() -> Self {
        StateUpdate::default()
    }

    pub fn prune_redundant_updates(&mut self) {
        let in_account_set: HashSet<[u8; 32]> = self.in_accounts.iter().map(|a| a.hash).collect();
        // NOTE: For snapshot verification, we might need to persist accounts data until accounts are
        //       removed from the tree through the nullifier crank.
        self.out_accounts
            .retain(|a| !in_account_set.contains(&a.hash));

        // TODO: Use seq instead of slot.
        let mut seen_path_nodes = HashSet::new();

        self.path_nodes.sort_by(|a, b| a.slot.cmp(&b.seq));
        self.path_nodes = self
            .path_nodes
            .drain(..)
            .rev()
            .filter(|p| seen_path_nodes.insert((p.tree, p.node.index)))
            .rev()
            .collect();
    }

    pub fn merge_updates(updates: Vec<StateUpdate>) -> StateUpdate {
        let mut merged = StateUpdate::default();
        for update in updates {
            merged.in_accounts.extend(update.in_accounts);
            merged.out_accounts.extend(update.out_accounts);
            merged.path_nodes.extend(update.path_nodes);
        }
        merged.prune_redundant_updates();
        merged
    }
}
