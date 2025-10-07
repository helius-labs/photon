mod address;
mod append;
pub mod helpers;
mod nullify;
pub mod sequence;

use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::MerkleTreeEvent;
use crate::ingester::parser::merkle_tree_events_parser::BatchMerkleTreeEvents;
use log::debug;
use sea_orm::DatabaseTransaction;
use solana_pubkey::Pubkey;

use self::address::persist_batch_address_append_event;
use self::append::persist_batch_append_event;
use self::helpers::{deduplicate_events, persist_leaf_nodes_chunked, ZKP_BATCH_SIZE};
use self::nullify::persist_batch_nullify_event;
use self::sequence::should_process_event;

/// We need to find the events of the same tree:
/// - order them by sequence number and execute them in order
///     HashMap<pubkey, Vec<Event(BatchAppendEvent, seq)>>
/// - execute a single function call to persist all changed nodes
pub async fn persist_batch_events(
    txn: &DatabaseTransaction,
    mut events: BatchMerkleTreeEvents,
    tree_info_cache: &std::collections::HashMap<
        solana_pubkey::Pubkey,
        crate::ingester::parser::tree_info::TreeInfo,
    >,
) -> Result<(), IngesterError> {
    for (tree_pubkey, events) in events.iter_mut() {
        let solana_pubkey = Pubkey::from(*tree_pubkey);
        let tree_info = tree_info_cache.get(&solana_pubkey)
            .ok_or_else(|| IngesterError::ParserError(format!(
                "Tree metadata not found for tree {}. Tree metadata must be synced before indexing.",
                bs58::encode(tree_pubkey).into_string()
            )))?;
        let tree_height = tree_info.height;
        // Sort by sequence
        events.sort_by(|a, b| a.0.cmp(&b.0));

        let tree_name = bs58::encode(tree_pubkey).into_string();
        let sequences_before: Vec<u64> = events.iter().map(|(seq, _)| *seq).collect();
        debug!(
            "Processing tree: {:?} with {} events, sequences: {:?}",
            tree_name,
            events.len(),
            sequences_before
        );

        // Deduplicate events with the same sequence number.
        // It's safety measure for (almost) impossible case when we have two events with the same sequence number.
        // This should never happen in practice, but we handle it gracefully just in case.
        // The only valid scenario for duplicate sequences is snapshot with duplicate transactions.
        // In that case we just process the first event and ignore the rest.
        deduplicate_events(events);

        // Process each event in sequence
        for (_event_seq, event) in events.iter() {
            // Batch size is 500 for batched State Merkle trees.
            let mut leaf_nodes = Vec::with_capacity(ZKP_BATCH_SIZE);
            // Check if we should process this event (idempotency check)
            let should_process = match event {
                MerkleTreeEvent::BatchNullify(e)
                | MerkleTreeEvent::BatchAppend(e)
                | MerkleTreeEvent::BatchAddressAppend(e) => should_process_event(txn, e).await?,
                _ => return Err(IngesterError::InvalidEvent),
            };

            if !should_process {
                debug!(
                    "Skipping already processed event with sequence {}",
                    _event_seq
                );
                continue;
            }

            match event {
                MerkleTreeEvent::BatchNullify(batch_nullify_event) => {
                    persist_batch_nullify_event(txn, batch_nullify_event, &mut leaf_nodes).await
                }
                MerkleTreeEvent::BatchAppend(batch_append_event) => {
                    persist_batch_append_event(txn, batch_append_event, &mut leaf_nodes).await
                }
                MerkleTreeEvent::BatchAddressAppend(batch_address_append_event) => {
                    persist_batch_address_append_event(
                        txn,
                        batch_address_append_event,
                        tree_info_cache,
                    )
                    .await
                }
                _ => Err(IngesterError::InvalidEvent),
            }?;

            // Persist leaf nodes, chunking if necessary
            persist_leaf_nodes_chunked(txn, leaf_nodes, tree_height).await?;
        }
    }
    Ok(())
}
