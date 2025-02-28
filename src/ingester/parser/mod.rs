use merkle_tree_events_parser::parse_merkle_tree_event;
use solana_sdk::pubkey::Pubkey;
use tx_event_parser::parse_legacy_public_transaction_event;
use tx_event_parser_v2::create_state_update;

use super::{error::IngesterError, typedefs::block_info::TransactionInfo};

use self::state_update::{StateUpdate, Transaction};

pub mod indexer_events;
pub mod merkle_tree_events_parser;
pub mod state_update;
pub mod tree_info;
mod tx_event_parser;
pub mod tx_event_parser_v2;

use crate::ingester::parser::tx_event_parser_v2::parse_public_transaction_event_v2;
use solana_program::pubkey;

pub const ACCOUNT_COMPRESSION_PROGRAM_ID: Pubkey =
    pubkey!("compr6CUsB5m2jS4Y3831ztGSTnDpnKJTKS95d64XVq");
const SYSTEM_PROGRAM: Pubkey = pubkey!("11111111111111111111111111111111");
const NOOP_PROGRAM_ID: Pubkey = pubkey!("noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV");
const VOTE_PROGRAM_ID: Pubkey = pubkey!("Vote111111111111111111111111111111111111111");

pub fn parse_transaction(tx: &TransactionInfo, slot: u64) -> Result<StateUpdate, IngesterError> {
    let mut state_updates = Vec::new();
    let mut is_compression_transaction = false;

    for instruction_group in tx.clone().instruction_groups {
        let mut ordered_instructions = Vec::new();
        ordered_instructions.push(instruction_group.outer_instruction.clone());
        ordered_instructions.extend(instruction_group.inner_instructions.clone());

        let mut vec_accounts = Vec::<Vec<Pubkey>>::new();
        let mut vec_instructions_data = Vec::new();
        let mut program_ids = Vec::new();

        ordered_instructions.iter().for_each(|inner_instruction| {
            vec_instructions_data.push(inner_instruction.data.clone());
            vec_accounts.push(inner_instruction.accounts.clone());
            program_ids.push(inner_instruction.program_id);
        });

        if let Some(event) =
            parse_public_transaction_event_v2(&program_ids, &vec_instructions_data, vec_accounts)
        {
            let state_update = create_state_update(tx.signature, slot, event)?;
            is_compression_transaction = true;
            state_updates.push(state_update);
        } else {
            for (index, _) in ordered_instructions.iter().enumerate() {
                if ordered_instructions.len() - index > 2 {
                    if let Some(state_update) = parse_legacy_public_transaction_event(
                        tx,
                        slot,
                        &ordered_instructions[index],
                        &ordered_instructions[index + 1],
                        &ordered_instructions[index + 2],
                    )? {
                        is_compression_transaction = true;
                        state_updates.push(state_update);
                    }
                } else if ordered_instructions.len() - index > 1 {
                    if let Some(state_update) = parse_merkle_tree_event(
                        &ordered_instructions[index],
                        &ordered_instructions[index + 1],
                        tx,
                    )? {
                        is_compression_transaction = true;
                        state_updates.push(state_update);
                    }
                }
            }
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

fn is_voting_transaction(tx: &TransactionInfo) -> bool {
    tx.instruction_groups
        .iter()
        .any(|group| group.outer_instruction.program_id == VOTE_PROGRAM_ID)
}
