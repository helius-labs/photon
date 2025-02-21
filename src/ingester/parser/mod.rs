use borsh::BorshDeserialize;
use indexer_events::{IndexedMerkleTreeEvent, MerkleTreeEvent, NullifierEvent};
use lazy_static::lazy_static;
use light_batched_merkle_tree::event::BatchAppendEvent;
use log::info;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use state_update::{IndexedTreeLeafUpdate, LeafNullification};
use std::collections::HashMap;
use std::str::FromStr;

use light_batched_merkle_tree::event::{
    BATCH_ADDRESS_APPEND_EVENT_DISCRIMINATOR, BATCH_APPEND_EVENT_DISCRIMINATOR,
    BATCH_NULLIFY_EVENT_DISCRIMINATOR,
};

use super::{error::IngesterError, typedefs::block_info::TransactionInfo};

use self::{
    indexer_events::PublicTransactionEvent,
    state_update::{AccountTransaction, StateUpdate, Transaction},
};

mod batch_event_parser;
pub mod indexer_events;
pub mod state_update;

use crate::common::typedefs::account::AccountWithContext;
use crate::ingester::parser::batch_event_parser::{
    parse_batch_public_transaction_event, parse_public_transaction_event_v2,
};
use crate::ingester::typedefs::block_info::Instruction;
use solana_program::pubkey;

pub const ACCOUNT_COMPRESSION_PROGRAM_ID: Pubkey =
    pubkey!("compr6CUsB5m2jS4Y3831ztGSTnDpnKJTKS95d64XVq");
const SYSTEM_PROGRAM: Pubkey = pubkey!("11111111111111111111111111111111");
const NOOP_PROGRAM_ID: Pubkey = pubkey!("noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV");
const VOTE_PROGRAM_ID: Pubkey = pubkey!("Vote111111111111111111111111111111111111111");

// TODO: add a table which stores tree metadata: tree_pubkey | queue_pubkey | type | ...
lazy_static! {
    pub static ref QUEUE_TREE_MAPPING: HashMap<String, String> = {
        let mut m = HashMap::new();
        m.insert(
            "6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU".to_string(), // queue
            "HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu".to_string(), // tree
        );
        m
    };
}

fn queue_to_tree(queue: &str) -> Option<Pubkey> {
    QUEUE_TREE_MAPPING
        .get(queue)
        .map(|x| Pubkey::from_str(x.as_str()).unwrap())
}

pub fn parse_transaction(tx: &TransactionInfo, slot: u64) -> Result<StateUpdate, IngesterError> {
    let mut state_updates = Vec::new();
    let mut is_compression_transaction = false;

    for instruction_group in tx.clone().instruction_groups {
        let mut ordered_instructions = Vec::new();
        ordered_instructions.push(instruction_group.outer_instruction.clone());
        ordered_instructions.extend(instruction_group.inner_instructions.clone());

        let mut vec_accounts = Vec::<Vec<Pubkey>>::new();
        let mut vec_instructions_data = Vec::new();
        vec_instructions_data.push(instruction_group.outer_instruction.data);
        vec_accounts.push(instruction_group.outer_instruction.accounts.clone());

        instruction_group
            .inner_instructions
            .iter()
            .find_map(|inner_instruction| {
                vec_instructions_data.push(inner_instruction.data.clone());
                vec_accounts.push(inner_instruction.accounts.clone());
                None::<PublicTransactionEvent>
            });

        if let Some(event) = parse_public_transaction_event_v2(&vec_instructions_data, vec_accounts)
        {
            let state_update = parse_batch_public_transaction_event(tx.signature, slot, event)?;
            is_compression_transaction = true;
            state_updates.push(state_update);
        } else {
            parse_legacy_instructions(
                &ordered_instructions,
                tx,
                slot,
                &mut state_updates,
                &mut is_compression_transaction,
            )?;
        }
    }

    let mut state_update = StateUpdate::merge_updates(state_updates.clone());
    if !is_voting_transaction(tx) || is_compression_transaction {
        state_update.transactions.insert(Transaction {
            signature: tx.signature,
            slot,
            uses_compression: is_compression_transaction,
            error: tx.error.clone(),
        });
    }

    Ok(state_update)
}

fn parse_legacy_public_transaction_event(
    tx: &TransactionInfo,
    slot: u64,
    instruction: &Instruction,
    next_instruction: &Instruction,
    next_next_instruction: &Instruction,
) -> Result<Option<StateUpdate>, IngesterError> {
    if ACCOUNT_COMPRESSION_PROGRAM_ID == instruction.program_id
        && next_instruction.program_id == SYSTEM_PROGRAM
        && next_next_instruction.program_id == NOOP_PROGRAM_ID
        && tx.error.is_none()
    {
        info!(
            "Indexing transaction with slot {} and id {}",
            slot, tx.signature
        );

        let public_transaction_event =
            PublicTransactionEvent::deserialize(&mut next_next_instruction.data.as_slice())
                .map_err(|e| {
                    IngesterError::ParserError(format!(
                        "Failed to deserialize PublicTransactionEvent: {}",
                        e
                    ))
                })?;

        parse_public_transaction_event(tx.signature, slot, public_transaction_event).map(Some)
    } else {
        Ok(None)
    }
}

fn parse_batch_merkle_tree_event(
    instruction: &Instruction,
    next_instruction: &Instruction,
    tx: &TransactionInfo,
) -> Result<Option<StateUpdate>, IngesterError> {
    if ACCOUNT_COMPRESSION_PROGRAM_ID == instruction.program_id
        && next_instruction.program_id == NOOP_PROGRAM_ID
        && tx.error.is_none()
    {
        info!("Parsing tx with signature: {}", tx.signature);

        // Try to parse as batch append/nullify event first
        if let Ok(batch_event) =
            BatchAppendEvent::deserialize(&mut next_instruction.data.as_slice())
        {
            let mut state_update = StateUpdate::new();

            match batch_event.discriminator {
                BATCH_APPEND_EVENT_DISCRIMINATOR => {
                    state_update.batch_append.push(batch_event);
                }
                BATCH_NULLIFY_EVENT_DISCRIMINATOR => {
                    state_update.batch_nullify.push(batch_event);
                }
                BATCH_ADDRESS_APPEND_EVENT_DISCRIMINATOR => {
                    // TODO: implement address append
                }
                _ => unimplemented!(),
            }

            return Ok(Some(state_update));
        }

        // If not batch event, try legacy events
        parse_legacy_merkle_tree_events(tx.signature, next_instruction).map(Some)
    } else {
        Ok(None)
    }
}

fn parse_legacy_merkle_tree_events(
    signature: Signature,
    instruction: &Instruction,
) -> Result<StateUpdate, IngesterError> {
    let merkle_tree_event = MerkleTreeEvent::deserialize(&mut instruction.data.as_slice())
        .map_err(|e| {
            IngesterError::ParserError(format!("Failed to deserialize MerkleTreeEvent: {}", e))
        })?;

    match merkle_tree_event {
        MerkleTreeEvent::V2(nullifier_event) => parse_nullifier_event(signature, nullifier_event),
        MerkleTreeEvent::V3(indexed_merkle_tree_event) => {
            parse_indexed_merkle_tree_update(indexed_merkle_tree_event)
        }
        _ => Err(IngesterError::ParserError(
            "Expected nullifier event or merkle tree update".to_string(),
        )),
    }
}

fn parse_legacy_instructions(
    ordered_instructions: &[Instruction],
    tx: &TransactionInfo,
    slot: u64,
    state_updates: &mut Vec<StateUpdate>,
    is_compression_transaction: &mut bool,
) -> Result<(), IngesterError> {
    for (index, _) in ordered_instructions.iter().enumerate() {
        if ordered_instructions.len() - index > 3 {
            if let Some(state_update) = parse_legacy_public_transaction_event(
                tx,
                slot,
                &ordered_instructions[index],
                &ordered_instructions[index + 1],
                &ordered_instructions[index + 2],
            )? {
                *is_compression_transaction = true;
                state_updates.push(state_update);
            }
        }

        if ordered_instructions.len() - index > 1 {
            if let Some(state_update) = parse_batch_merkle_tree_event(
                &ordered_instructions[index],
                &ordered_instructions[index + 1],
                tx,
            )? {
                *is_compression_transaction = true;
                state_updates.push(state_update);
            }
        }
    }

    Ok(())
}

fn is_voting_transaction(tx: &TransactionInfo) -> bool {
    tx.instruction_groups
        .iter()
        .any(|group| group.outer_instruction.program_id == VOTE_PROGRAM_ID)
}

fn parse_indexed_merkle_tree_update(
    indexed_merkle_tree_event: IndexedMerkleTreeEvent,
) -> Result<StateUpdate, IngesterError> {
    let IndexedMerkleTreeEvent {
        id,
        updates,
        mut seq,
    } = indexed_merkle_tree_event;
    let mut state_update = StateUpdate::new();

    for update in updates {
        for (leaf, hash) in [
            (update.new_low_element, update.new_low_element_hash),
            (update.new_high_element, update.new_high_element_hash),
        ]
        .iter()
        {
            let indexed_tree_leaf_update = IndexedTreeLeafUpdate {
                tree: Pubkey::from(id),
                hash: *hash,
                leaf: *leaf,
                seq,
            };
            seq += 1;
            state_update.indexed_merkle_tree_updates.insert(
                (indexed_tree_leaf_update.tree, leaf.index as u64),
                indexed_tree_leaf_update,
            );
        }
    }

    Ok(state_update)
}

fn parse_nullifier_event(
    tx: Signature,
    nullifier_event: NullifierEvent,
) -> Result<StateUpdate, IngesterError> {
    let NullifierEvent {
        id,
        nullified_leaves_indices,
        seq,
    } = nullifier_event;

    let mut state_update = StateUpdate::new();

    for (i, leaf_index) in nullified_leaves_indices.iter().enumerate() {
        let leaf_nullification: LeafNullification = {
            LeafNullification {
                tree: Pubkey::from(id),
                leaf_index: *leaf_index,
                seq: seq + i as u64,
                signature: tx,
            }
        };
        state_update.leaf_nullifications.insert(leaf_nullification);
    }

    Ok(state_update)
}

fn parse_public_transaction_event(
    tx: Signature,
    slot: u64,
    transaction_event: PublicTransactionEvent,
) -> Result<StateUpdate, IngesterError> {
    let PublicTransactionEvent {
        input_compressed_account_hashes,
        output_compressed_account_hashes,
        output_compressed_accounts,
        pubkey_array,
        sequence_numbers,
        ..
    } = transaction_event;

    let mut state_update = StateUpdate::new();

    let mut has_batched_instructions = false;
    for seq in sequence_numbers.iter() {
        if queue_to_tree(&seq.pubkey.to_string()).is_some() {
            has_batched_instructions = true;
            break;
        }
    }

    let mut tree_to_seq_number = HashMap::new();
    if has_batched_instructions {
        for seq in sequence_numbers.iter() {
            if let Some(tree) = queue_to_tree(&seq.pubkey.to_string()) {
                tree_to_seq_number.insert(tree, seq.seq);
            }
        }
    } else {
        tree_to_seq_number = sequence_numbers
            .iter()
            .map(|seq| (seq.pubkey, seq.seq))
            .collect::<HashMap<_, _>>();
    }

    for hash in input_compressed_account_hashes {
        state_update.in_accounts.insert(hash.into());
    }

    for ((out_account, hash), leaf_index) in output_compressed_accounts
        .into_iter()
        .zip(output_compressed_account_hashes)
        .zip(transaction_event.output_leaf_indices.iter())
    {
        let mut tree = pubkey_array[out_account.merkle_tree_index as usize];
        let mut queue = queue_to_tree(&tree.to_string());
        if let Some(q) = queue {
            // swap tree and q
            let temp = tree;
            tree = q;
            queue = Some(temp);
        };

        let mut seq = None;
        if queue.is_none() {
            seq = Some(*tree_to_seq_number.get(&tree).ok_or_else(|| {
                IngesterError::ParserError("Missing sequence number".to_string())
            })?);
        }
        let enriched_account = AccountWithContext::new(
            out_account.compressed_account,
            hash,
            tree,
            queue,
            *leaf_index,
            slot,
            seq,
            queue.is_some(),
            false,
            None,
            None,
        );

        if queue.is_none() {
            let seq = tree_to_seq_number
                .get_mut(&tree)
                .ok_or_else(|| IngesterError::ParserError("Missing sequence number".to_string()))?;
            *seq += 1;
        }

        state_update.out_accounts.push(enriched_account);
    }

    state_update
        .account_transactions
        .extend(
            state_update
                .in_accounts
                .iter()
                .map(|hash| AccountTransaction {
                    hash: hash.clone(),
                    signature: tx,
                }),
        );

    state_update
        .account_transactions
        .extend(
            state_update
                .out_accounts
                .iter()
                .map(|a| AccountTransaction {
                    hash: a.account.hash.clone(),
                    signature: tx,
                }),
        );

    Ok(state_update)
}
