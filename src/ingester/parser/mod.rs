use borsh::BorshDeserialize;
use byteorder::{ByteOrder, LittleEndian};
use log::debug;
use solana_sdk::{pubkey::Pubkey, signature::Signature};

use crate::{
    common::typedefs::{
        account::Account, account::AccountData, bs64_string::Base64String, hash::Hash,
        serializable_pubkey::SerializablePubkey,
    },
    ingester::parser::{
        indexer_events::{CompressedAccountWithMerkleContext, PathNode},
        state_update::EnrichedPathNode,
    },
};

use super::{error::IngesterError, typedefs::block_info::TransactionInfo};

use self::{
    indexer_events::{ChangelogEvent, Changelogs, CompressedAccount, PublicTransactionEvent},
    state_update::{AccountTransaction, PathUpdate, StateUpdate},
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
                        debug!(
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
                    let state_update = parse_public_transaction_event(
                        tx.signature,
                        slot,
                        public_transaction_event,
                        changelogs,
                    )?;
                    state_updates.push(state_update);
                }
            }
        }
    }
    Ok(StateUpdate::merge_updates(state_updates))
}

fn parse_account_data(
    compressed_account: CompressedAccount,
    hash: [u8; 32],
    tree: Pubkey,
    leaf_index: u32,
    slot: u64,
    seq: Option<u64>,
) -> Account {
    let CompressedAccount {
        owner,
        lamports,
        address,
        data,
    } = compressed_account;

    let data = data.map(|d| AccountData {
        discriminator: LittleEndian::read_u64(&d.discriminator),
        data: Base64String(d.data),
        data_hash: Hash::from(d.data_hash),
    });

    Account {
        owner: owner.into(),
        lamports,
        address: address.map(SerializablePubkey::from),
        data,
        hash: hash.into(),
        slot_updated: slot,
        leaf_index,
        tree: SerializablePubkey::from(tree),
        seq,
    }
}

fn parse_public_transaction_event(
    tx: Signature,
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

    let mut state_update = StateUpdate::new();

    for (account, hash) in input_compressed_accounts
        .iter()
        .zip(input_compressed_account_hashes)
    {
        let CompressedAccountWithMerkleContext {
            compressed_account,
            merkle_tree_pubkey_index,
            leaf_index,
            ..
        } = account.clone();

        let enriched_account = parse_account_data(
            compressed_account,
            hash,
            pubkey_array[merkle_tree_pubkey_index as usize],
            leaf_index,
            slot,
            None,
        );

        state_update.in_accounts.push(enriched_account);
    }
    let path_updates = extract_path_updates(changelogs);

    if output_compressed_accounts.len() != path_updates.len() {
        return Err(IngesterError::MalformedEvent {
            msg: format!(
                "Number of path updates did not match the number of output accounts (txn: {})",
                tx,
            ),
        });
    }

    for (((out_account, path), hash), leaf_index) in output_compressed_accounts
        .into_iter()
        .zip(path_updates.iter())
        .zip(output_compressed_account_hashes)
        .zip(transaction_event.output_leaf_indices.iter())
    {
        let enriched_account = parse_account_data(
            out_account,
            hash,
            path.tree.into(),
            *leaf_index,
            slot,
            Some(path.seq),
        );
        state_update.out_accounts.push(enriched_account);
    }

    for ((path_index, path), leaf_index) in path_updates
        .into_iter()
        .enumerate()
        .zip(transaction_event.output_leaf_indices)
    {
        for (i, node) in path.path.iter().enumerate() {
            state_update.path_nodes.insert(
                (path.tree, node.index),
                EnrichedPathNode {
                    node: node.clone(),
                    slot,
                    tree: path.tree,
                    seq: path.seq + path_index as u64,
                    level: i,
                    tree_depth: path.path.len(),
                    leaf_index: if i == 0 { Some(leaf_index) } else { None },
                },
            );
        }
    }

    state_update
        .account_transactions
        .extend(state_update.in_accounts.iter().map(|a| AccountTransaction {
            hash: a.hash.clone(),
            signature: tx,
            slot,
        }));

    state_update
        .account_transactions
        .extend(
            state_update
                .out_accounts
                .iter()
                .map(|a| AccountTransaction {
                    hash: a.hash.clone(),
                    signature: tx,
                    slot,
                }),
        );

    Ok(state_update)
}

fn extract_path_updates(changelogs: Changelogs) -> Vec<PathUpdate> {
    changelogs
        .changelogs
        .iter()
        .flat_map(|cl| match cl {
            ChangelogEvent::V1(cl) => {
                let tree_id = cl.id;
                cl.paths.iter().map(move |p| PathUpdate {
                    tree: tree_id,
                    path: p
                        .iter()
                        .map(|node| PathNode {
                            node: node.node,
                            index: node.index,
                        })
                        .collect(),
                    seq: cl.seq,
                })
            }
        })
        .collect::<Vec<_>>()
}
