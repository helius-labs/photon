use borsh::BorshDeserialize;
use light_merkle_tree_event::Changelogs;
use log::info;
use psp_compressed_pda::event::PublicTransactionEvent;
use solana_sdk::pubkey::Pubkey;

use crate::{error::IngesterError, transaction_info::TransactionInfo};

use self::bundle::EventBundle;

pub mod bundle;
use solana_program::pubkey;

const ACCOUNT_COMPRESSION_PROGRAM_ID: Pubkey =
    pubkey!("5QPEJ5zDsVou9FQS3KCauKswM3VwBEBu4dpL9xTqkWwN");
const NOOP_PROGRAM_ID: Pubkey = pubkey!("noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV");

pub fn parse_transaction(tx: TransactionInfo) -> Result<Vec<EventBundle>, IngesterError> {
    info!("Parsing transaction: {}", tx);
    let mut event_bundles = Vec::new();
    for instruction_group in tx.instruction_groups {
        let mut ordered_intructions = Vec::new();
        ordered_intructions.push(instruction_group.outer_instruction);
        ordered_intructions.extend(instruction_group.inner_instructions);

        for (index, instruction) in ordered_intructions.iter().enumerate() {
            if index < ordered_intructions.len() - 2 {
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
                    let changelogs = Changelogs::deserialize(&mut next_instruction.data.as_slice())
                        .map_err(|e| {
                            IngesterError::ParserError(format!(
                                "Failed to deserialize Changelogs: {}",
                                e
                            ))
                        })?;
                    event_bundles.push(EventBundle::LegacyChangeLogEvent(changelogs));

                    let public_transaction_event = PublicTransactionEvent::deserialize(
                        &mut next_next_instruction.data.as_slice(),
                    )
                    .map_err(|e| {
                        IngesterError::ParserError(format!(
                            "Failed to deserialize PublicTransactionEvent: {}",
                            e
                        ))
                    })?;

                    event_bundles.push(EventBundle::LegacyPublicStateTransaction(
                        public_transaction_event.into(),
                    ));
                }
            }
        }
    }
    Ok(event_bundles)
}
