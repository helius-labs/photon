use borsh::BorshDeserialize;
use log::info;
use solana_sdk::pubkey::Pubkey;

use crate::ingester::parser::{
    indexer_events::CompressedAccountWithMerkleContext, state_update::EnrichedAccount,
};

use super::{error::IngesterError, typedefs::block_info::TransactionInfo};

use self::{
    indexer_events::{Changelogs, PublicTransactionEvent},
    state_update::StateUpdate,
};

pub mod indexer_events;
pub mod state_update;

use solana_program::pubkey;

const ACCOUNT_COMPRESSION_PROGRAM_ID: Pubkey =
    pubkey!("5QPEJ5zDsVou9FQS3KCauKswM3VwBEBu4dpL9xTqkWwN");
const NOOP_PROGRAM_ID: Pubkey = pubkey!("noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV");

pub fn parse_transaction(tx: &TransactionInfo, slot: u64) -> Result<StateUpdate, IngesterError> {
    let mut state_updates = Vec::new();
    let mut logged_transaction = false;

    for instruction_group in tx.clone().instruction_groups {
        let mut ordered_intructions = Vec::new();
        ordered_intructions.push(instruction_group.outer_instruction);
        ordered_intructions.extend(instruction_group.inner_instructions);

        for (index, instruction) in ordered_intructions.iter().enumerate() {
            if ordered_intructions.len() - index > 2 {
                let next_instruction = &ordered_intructions[index + 1];
                let next_next_instruction = &ordered_intructions[index + 2];
                // We need to check if the account compression instruction contains a noop account to determine
                // if the instruction emits a noop event. If it doesn't then we want avoid indexing
                // the following noop instruction because it'll contain either irrelevant or malicious data.
                if ACCOUNT_COMPRESSION_PROGRAM_ID == instruction.program_id
                    && instruction.accounts.contains(&NOOP_PROGRAM_ID)
                    && next_instruction.program_id == NOOP_PROGRAM_ID
                    && next_next_instruction.program_id == NOOP_PROGRAM_ID
                {
                    if !logged_transaction {
                        info!(
                            "Indexing transaction with slot {} and id {}",
                            slot, tx.signature
                        );
                        logged_transaction = true;
                    }
                    let changelogs = Changelogs::deserialize(&mut next_instruction.data.as_slice())
                        .map_err(|e| {
                            IngesterError::ParserError(format!(
                                "Failed to deserialize Changelogs: {}",
                                e
                            ))
                        })?;

                    let public_transaction_event = PublicTransactionEvent::deserialize(
                        &mut next_next_instruction.data.as_slice(),
                    )
                    .map_err(|e| {
                        IngesterError::ParserError(format!(
                            "Failed to deserialize PublicTransactionEvent: {}",
                            e
                        ))
                    })?;
                    let state_update =
                        parse_public_transaction_event(slot, public_transaction_event, changelogs)?;
                    state_updates.push(state_update);
                }
            }
        }
    }
    Ok(StateUpdate::merge_updates(state_updates))
}

fn parse_public_transaction_event(
    slot: u64,
    transaction_event: PublicTransactionEvent,
    changelogs: Changelogs,
) -> Result<StateUpdate, IngesterError> {
    let PublicTransactionEvent {
        input_compressed_account_hashes,
        output_compressed_account_hashes,
        input_compressed_accounts,
        output_compressed_accounts,
        pubkey_array,
        ..
    } = transaction_event;

    let state_update = StateUpdate::new();

    for (account, hash) in input_compressed_accounts
        .drain(..)
        .zip(input_compressed_account_hashes)
    {
        let CompressedAccountWithMerkleContext {
            compressed_account,
            merkle_tree_pubkey_index,
            nullifier_queue_pubkey_index,
            leaf_index,
        } = account;

        let account_with_slot = EnrichedAccount {
            account: compressed_account,
            tree: pubkey_array[merkle_tree_pubkey_index as usize],
            seq: None,
            slot: slot as i64,
        };
        state_update.in_accounts.push(account_with_slot);
    }

    todo!()
}

// impl TryFrom<EventBundle> for StateUpdate {
//     type Error = IngesterError;

//     fn try_from(e: EventBundle) -> Result<StateUpdate, IngesterError> {
//         match e {
//             EventBundle::PublicTransactionEvent(e) => {
//                 let mut state_update = StateUpdate::default();
//                 state_update
//                     .in_accounts
//                     .extend(
//                         e.in_accounts
//                             .into_iter()
//                             .map(|utxoaccount| AccountWithSlot {
//                                 utxo,
//                                 slot: e.slot as i64,
//                             }),
//                     );

//                 let path_updates = extract_path_updates(e.changelogs);

//                 if e.out_utxos.len() != path_updates.len() {
//                     return Err(IngesterError::MalformedEvent {
//                         msg: format!(
//                             "Number of path updates did not match the number of output UTXOs (txn: {})",
//                             e.transaction,
//                         ),
//                     });
//                 }

//                 for (out_utxo, path) in e.out_utxos.into_iter().zip(path_updates.iter()) {
//                     state_update.out_utxos.push(EnrichedUtxo {
//                         utxo: UtxoWithSlot {
//                             utxo: out_utxo,
//                             slot: e.slot as i64,
//                         },
//                         tree: path.tree,
//                         seq: path.seq,
//                     });
//                 }

//                 state_update
//                     .path_nodes
//                     .extend(path_updates.into_iter().flat_map(|p| {
//                         let tree_height = p.path.len();
//                         p.path
//                             .into_iter()
//                             .enumerate()
//                             .map(move |(i, node)| EnrichedPathNode {
//                                 node: PathNode {
//                                     node: node.node,
//                                     index: node.index,
//                                 },
//                                 slot: e.slot as i64,
//                                 tree: p.tree,
//                                 seq: p.seq,
//                                 level: i,
//                                 tree_depth: tree_height,
//                             })
//                     }));
//                 state_update.prune_redundant_updates();
//                 Ok(state_update)
//             }
//         }
//     }
// }

// fn extract_path_updates(changelogs: Changelogs) -> Vec<PathUpdate> {
//     changelogs
//         .changelogs
//         .iter()
//         .flat_map(|cl| match cl {
//             ChangelogEvent::V1(cl) => {
//                 let tree_id = cl.id;
//                 cl.paths.iter().map(move |p| PathUpdate {
//                     tree: tree_id,
//                     path: p
//                         .iter()
//                         .map(|node| PathNode {
//                             node: node.node,
//                             index: node.index,
//                         })
//                         .collect(),
//                     seq: cl.seq as i64,
//                 })
//             }
//         })
//         .collect::<Vec<_>>()
// }
