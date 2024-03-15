use light_merkle_tree_event::{ChangelogEvent, Changelogs, PathNode};
use psp_compressed_pda::utxo::Utxo;
use solana_sdk::clock::Slot;

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
}

#[derive(Default)]
pub struct MergedEventBundle {
    pub in_utxos: Vec<UtxoWithSlot>,
    pub out_utxos: Vec<EnrichedUtxo>,
    pub path_nodes: Vec<EnrichedPathNode>,
}

struct PathUpdate {
    tree: [u8; 32],
    path: Vec<PathNode>,
    seq: i64,
}

pub async fn consolidate_event_bundle(
    events: Vec<EventBundle>,
) -> Result<MergedEventBundle, IngesterError> {
    let mut merged_bundle = MergedEventBundle::default();

    for event in events {
        match event {
            EventBundle::PublicTransactionEvent(e) => {
                for in_utxo in e.in_utxos {
                    merged_bundle.in_utxos.push(UtxoWithSlot {
                        utxo: in_utxo,
                        slot: e.slot as i64,
                    });
                }

                let path_updates = extract_path_updates(e.changelogs, e.slot);

                if e.out_utxos.len() != path_updates.len() {
                    return Err(IngesterError::MalformedEvent {
                        msg: format!(
                            "Number of path updates did not match the number of output UTXOs (txn: {})",
                            e.transaction,
                        ),
                    });
                }

                // TODO: Do batch inserts here
                for i in 0..e.out_utxos.len() {
                    let out_utxo = &e.out_utxos[i];
                    let path = &path_updates[i];
                    let tree = path.tree;
                    let seq = path.seq;
                    merged_bundle.out_utxos.push(EnrichedUtxo {
                        utxo: UtxoWithSlot {
                            utxo: out_utxo.clone(),
                            slot: e.slot as i64,
                        },
                        tree,
                        seq,
                    });
                }

                merged_bundle
                    .path_nodes
                    .extend(path_updates.iter().flat_map(|p| {
                        p.path.iter().map(move |node| EnrichedPathNode {
                            // PathNode does not support cloning.
                            node: PathNode {
                                node: node.node.clone(),
                                index: node.index,
                            },
                            slot: e.slot as i64,
                            tree: p.tree,
                            seq: p.seq,
                        })
                    }));
            }
        }
    }
    Ok(merged_bundle)
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
