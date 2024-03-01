use account_compression::Changelogs;
use borsh::BorshDeserialize;
use light_verifier_sdk::public_transaction::PublicTransactionEvent;
use log::info;
use solana_sdk::pubkey::Pubkey;

use crate::{error::IngesterError, transaction_info::TransactionInfo};

use self::bundle::EventBundle;

pub mod bundle;
use solana_program::pubkey;

const TOKEN_PROGRAM_ID: Pubkey = pubkey!("9sixVEthz2kMSKfeApZXHwuboT6DZuT6crAYJTciUCqE");
const ACCOUNT_COMPRESSION_PROGRAM_ID: Pubkey =
    pubkey!("DmtCHY9V1vqkYfQ5xYESzvGoMGhePHLja9GQ994GKTTc");
const NOOP_PROGRAM_ID: Pubkey = pubkey!("noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV");

pub fn parse_transaction(tx: TransactionInfo) -> Result<Vec<EventBundle>, IngesterError> {
    info!("Parsing transaction: {}", tx);
    let mut event_bundles = Vec::new();
    for instruction_group in tx.instruction_groups {
        let mut ordered_intructions = Vec::new();
        ordered_intructions.push(instruction_group.outer_instruction);
        ordered_intructions.extend(instruction_group.inner_instructions);

        for (index, instruction) in ordered_intructions.iter().enumerate() {
            if index < ordered_intructions.len() - 1 {
                let next_instruction = &ordered_intructions[index + 1];
                // We need to check if the instruction contains a noop account. We use to determine
                // if the instruction emits a noop. If it doesn't then we want avoid indexing
                // the following noop instruction because it'll contain either irrelevant or malicious data.
                if instruction.accounts.contains(&NOOP_PROGRAM_ID)
                    && next_instruction.program_id == NOOP_PROGRAM_ID
                {
                    let instruction_data = &mut next_instruction.data.as_slice();
                    let event_bundle = match instruction.program_id {
                        TOKEN_PROGRAM_ID => {
                            let event_bundle = PublicTransactionEvent::deserialize(
                                instruction_data,
                            )
                            .map_err(|e| {
                                IngesterError::ParserError(format!(
                                    "Failed to deserialize PublicTransactionEvent: {}",
                                    e
                                ))
                            })?;
                            Some(EventBundle::LegacyPublicStateTransaction(event_bundle))
                        }
                        ACCOUNT_COMPRESSION_PROGRAM_ID => {
                            let event_bundle =
                                Changelogs::deserialize(&mut next_instruction.data.as_slice())
                                    .map_err(|e| {
                                        IngesterError::ParserError(format!(
                                            "Failed to deserialize Changelogs: {}",
                                            e
                                        ))
                                    })?;
                            Some(EventBundle::LegacyChangeLogEvent(event_bundle))
                        }
                        _ => None,
                    };

                    if let Some(event_bundle) = event_bundle {
                        event_bundles.push(event_bundle);
                    }
                }
            }
        }
    }
    Ok(event_bundles)
}
