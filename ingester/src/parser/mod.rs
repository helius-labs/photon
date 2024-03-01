use borsh::BorshDeserialize;
use light_verifier_sdk::public_transaction::PublicTransactionEvent;
use solana_sdk::pubkey::Pubkey;

use crate::{error::IngesterError, transaction_info::TransactionInfo};

use self::bundle::EventBundle;

pub mod bundle;
use solana_program::pubkey;

const NOOP_PROGRAM_ID: Pubkey = pubkey!("noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV");
const MERKLE_TREE_PROGRAM_ID: Pubkey = pubkey!("JA5cjkRJ1euVi9xLWsCJVzsRzEkT8vcC4rqw9sVAo5d6");

pub fn parse_transaction(tx: TransactionInfo) -> Result<Vec<EventBundle>, IngesterError> {
    let mut event_bundles = Vec::new();
    for instruction_group in tx.instruction_groups {
        // To index compressed transactions, we need to log NOOP output after operations like compressed
        // token transfers. Since Solana RPC doesn't provide a hierarchical representation, we rely on
        // instruction ordering. We group instructions by their outer instruction and use the instruction
        // ordering to determine which ones emitted the noop data.
        let mut ordered_intructions = Vec::new();
        ordered_intructions.push(instruction_group.outer_instruction);
        ordered_intructions.extend(instruction_group.inner_instructions);

        for (index, instruction) in ordered_intructions.iter().enumerate() {
            if index < ordered_intructions.len() - 1 {
                let next_instruction = &ordered_intructions[index + 1];
                if instruction.program_id == MERKLE_TREE_PROGRAM_ID
                    && next_instruction.program_id == NOOP_PROGRAM_ID
                {
                    let event_bundle = EventBundle::LegacyPublicStateTransaction(
                        PublicTransactionEvent::deserialize(&mut next_instruction.data.as_slice())
                            .map_err(|e| {
                                IngesterError::ParserError(format!(
                                    "Failed to deserialize PublicTransactionEvent: {}",
                                    e
                                ))
                            })?,
                    );
                    event_bundles.push(event_bundle);
                }
            }
        }
    }
    Ok(event_bundles)
}
