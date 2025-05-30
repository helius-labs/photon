use merkle_tree_events_parser::parse_merkle_tree_event;
use solana_pubkey::Pubkey;
use std::sync::OnceLock;
use tx_event_parser::parse_public_transaction_event_v1;
use tx_event_parser_v2::create_state_update_v2;

use super::{error::IngesterError, typedefs::block_info::TransactionInfo};

use self::state_update::{StateUpdate, Transaction};

pub mod indexer_events;
pub mod merkle_tree_events_parser;
pub mod state_update;
pub mod tree_info;
mod tx_event_parser;
pub mod tx_event_parser_v2;

use crate::ingester::parser::tx_event_parser_v2::parse_public_transaction_event_v2;
use solana_pubkey::pubkey;

static ACCOUNT_COMPRESSION_PROGRAM_ID: OnceLock<Pubkey> = OnceLock::new();
pub fn get_compression_program_id() -> Pubkey {
    *ACCOUNT_COMPRESSION_PROGRAM_ID
        .get_or_init(|| pubkey!("compr6CUsB5m2jS4Y3831ztGSTnDpnKJTKS95d64XVq"))
}
pub fn set_compression_program_id(program_id_str: &str) -> Result<(), String> {
    match program_id_str.parse::<Pubkey>() {
        Ok(pubkey) => match ACCOUNT_COMPRESSION_PROGRAM_ID.set(pubkey) {
            Ok(_) => Ok(()),
            Err(_) => Err("Compression program ID has already been set".to_string()),
        },
        Err(err) => Err(format!("Invalid compression program ID: {}", err)),
    }
}

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
            let state_update = create_state_update_v2(tx.signature, slot, event)?;
            is_compression_transaction = true;
            state_updates.push(state_update);
        } else {
            for (index, instruction) in ordered_instructions.iter().enumerate() {
                if ordered_instructions.len() - index > 1 {
                    if get_compression_program_id() == instruction.program_id {
                        // Look for a NOOP_PROGRAM_ID instruction after one or two SYSTEM_PROGRAM instructions
                        // We handle up to two system program instructions in the case where we also have to pay a tree rollover fee
                        let mut noop_instruction_index = None;
                        let mut system_program_count = 0;
                        let mut all_intermediate_are_system = true;

                        // Search for the NOOP instruction, ensuring we find at least one SYSTEM_PROGRAM but no more than two
                        for i in (index + 1)..ordered_instructions.len() {
                            let current_instruction = &ordered_instructions[i];

                            if current_instruction.program_id == NOOP_PROGRAM_ID {
                                noop_instruction_index = Some(i);
                                break;
                            } else if current_instruction.program_id == SYSTEM_PROGRAM {
                                system_program_count += 1;
                                if system_program_count > 2 {
                                    all_intermediate_are_system = false;
                                    break;
                                }
                            } else {
                                all_intermediate_are_system = false;
                                break;
                            }
                        }

                        // If we found a NOOP instruction, exactly one or two SYSTEM_PROGRAM instructions, and all intermediates were valid
                        if let Some(noop_index) = noop_instruction_index {
                            if system_program_count >= 1
                                && system_program_count <= 2
                                && all_intermediate_are_system
                            {
                                if let Some(state_update) = parse_public_transaction_event_v1(
                                    tx,
                                    slot,
                                    instruction,
                                    &ordered_instructions[noop_index],
                                )? {
                                    is_compression_transaction = true;
                                    state_updates.push(state_update);
                                }
                            }
                        }
                    }
                }

                if ordered_instructions.len() - index > 1 {
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
