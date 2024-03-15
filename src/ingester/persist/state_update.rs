use std::collections::HashSet;

use light_merkle_tree_event::{ChangelogEvent, Changelogs, PathNode};
use psp_compressed_pda::utxo::Utxo;

use crate::ingester::{error::IngesterError, parser::bundle::EventBundle};

pub struct UtxoWithSlot {
    pub utxo: Utxo,
    pub slot: i64,
}

pub struct EnrichedUtxo {
    pub utxo: UtxoWithSlot,
    pub tree: [u8; 32],
    pub seq: i64,
}

pub struct EnrichedPathNode {
    pub node: PathNode,
    pub slot: i64,
    pub tree: [u8; 32],
    pub seq: i64,
    pub level: usize,
}

struct PathUpdate {
    tree: [u8; 32],
    path: Vec<PathNode>,
    seq: i64,
}

#[derive(Default)]
/// Representation of state update of the compression system that is optimal for simple persistance.
pub struct StateUpdate {
    pub in_utxos: Vec<UtxoWithSlot>,
    pub out_utxos: Vec<EnrichedUtxo>,
    pub path_nodes: Vec<EnrichedPathNode>,
}

impl StateUpdate {
    pub fn prune_redundant_updates(&mut self) {
        let in_utxo_hash_set: HashSet<[u8; 32]> =
            self.in_utxos.iter().map(|u| u.utxo.hash()).collect();
        // NOTE: For snapshot verification, we might need to persist UTXO data until UTXOs are
        //       removed from the tree through the nullifier crank.
        self.out_utxos
            .retain(|u| !in_utxo_hash_set.contains(&u.utxo.utxo.hash()));

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
            merged.in_utxos.extend(update.in_utxos);
            merged.out_utxos.extend(update.out_utxos);
            merged.path_nodes.extend(update.path_nodes);
        }
        merged.prune_redundant_updates();
        merged
    }
}

impl TryFrom<EventBundle> for StateUpdate {
    type Error = IngesterError;

    fn try_from(e: EventBundle) -> Result<StateUpdate, IngesterError> {
        match e {
            EventBundle::PublicTransactionEvent(e) => {
                let mut state_update = StateUpdate::default();
                state_update
                    .in_utxos
                    .extend(e.in_utxos.into_iter().map(|utxo| UtxoWithSlot {
                        utxo,
                        slot: e.slot as i64,
                    }));

                let path_updates = extract_path_updates(e.changelogs);

                if e.out_utxos.len() != path_updates.len() {
                    return Err(IngesterError::MalformedEvent {
                        msg: format!(
                            "Number of path updates did not match the number of output UTXOs (txn: {})",
                            e.transaction,
                        ),
                    });
                }

                for (out_utxo, path) in e.out_utxos.into_iter().zip(path_updates.iter()) {
                    state_update.out_utxos.push(EnrichedUtxo {
                        utxo: UtxoWithSlot {
                            utxo: out_utxo,
                            slot: e.slot as i64,
                        },
                        tree: path.tree,
                        seq: path.seq,
                    });
                }

                state_update
                    .path_nodes
                    .extend(path_updates.into_iter().flat_map(|p| {
                        p.path
                            .into_iter()
                            .enumerate()
                            .map(move |(i, node)| EnrichedPathNode {
                                node,
                                slot: e.slot as i64,
                                tree: p.tree,
                                seq: p.seq,
                                level: i,
                            })
                    }));
                state_update.prune_redundant_updates();
                Ok(state_update)
            }
        }
    }
}

fn extract_path_updates(changelogs: Changelogs) -> Vec<PathUpdate> {
    changelogs
        .changelogs
        .iter()
        .flat_map(|cl| match cl {
            ChangelogEvent::V1(cl) => {
                let tree_id = cl.id.clone();
                cl.paths.iter().map(move |p| PathUpdate {
                    tree: tree_id,
                    path: p
                        .iter()
                        .map(|node| PathNode {
                            node: node.node.clone(),
                            index: node.index,
                        })
                        .collect(),
                    seq: cl.seq as i64,
                })
            }
        })
        .collect::<Vec<_>>()
}
