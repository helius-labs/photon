use solana_sdk::pubkey::Pubkey;

use super::{error::IngesterError, typedefs::block_info::TransactionInfo};

use self::{
    indexer_events::PublicTransactionEvent,
    state_update::{StateUpdate, Transaction},
};

mod batch_event_parser;
pub mod indexer_events;
mod legacy;
pub mod state_update;
mod tx_event_parser;

use crate::ingester::parser::batch_event_parser::{
    parse_batch_public_transaction_event, parse_public_transaction_event_v2,
};
use crate::ingester::parser::legacy::parse_legacy_instructions;
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
            log::info!("batched state update {:?}", state_update);
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

fn is_voting_transaction(tx: &TransactionInfo) -> bool {
    tx.instruction_groups
        .iter()
        .any(|group| group.outer_instruction.program_id == VOTE_PROGRAM_ID)
}
