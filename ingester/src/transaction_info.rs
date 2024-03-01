use solana_sdk::{
    clock::{Slot, UnixTimestamp},
    pubkey::Pubkey,
    transaction::VersionedTransaction,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
    EncodedTransactionWithStatusMeta, UiInstruction,
};
use std::fmt;

use std::convert::TryFrom;

use crate::error::IngesterError;

pub struct Instruction {
    pub program_id: Pubkey,
    pub data: Vec<u8>,
    pub accounts: Vec<Pubkey>,
}

pub struct InstructionGroup {
    pub outer_instruction: Instruction,
    pub inner_instructions: Vec<Instruction>,
}

pub struct TransactionInfo {
    pub slot: Slot,
    pub block_time: Option<UnixTimestamp>,
    pub instruction_groups: Vec<InstructionGroup>,
}

impl fmt::Display for Instruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Instruction {{ program_id: {}}}", self.program_id,)
    }
}

impl fmt::Display for InstructionGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "InstructionGroup {{ outer_instruction: {}, inner_instructions: [{}] }}",
            self.outer_instruction,
            self.inner_instructions
                .iter()
                .map(Instruction::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

impl fmt::Display for TransactionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TransactionInfo {{ slot: {}, block_time: {:?}, instruction_groups: [{}] }}",
            self.slot,
            self.block_time,
            self.instruction_groups
                .iter()
                .map(InstructionGroup::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl TryFrom<EncodedConfirmedTransactionWithStatusMeta> for TransactionInfo {
    type Error = IngesterError;

    fn try_from(tx: EncodedConfirmedTransactionWithStatusMeta) -> Result<Self, Self::Error> {
        let EncodedConfirmedTransactionWithStatusMeta {
            slot,
            block_time,
            transaction,
        } = tx;

        let EncodedTransactionWithStatusMeta {
            transaction, meta, ..
        } = transaction;

        let versioned_transaction: VersionedTransaction = transaction.decode().ok_or(
            IngesterError::ParserError("Transaction cannot be decoded".to_string()),
        )?;
        let accounts = versioned_transaction.message.static_account_keys();
        let mut instruction_groups: Vec<InstructionGroup> = versioned_transaction
            .message
            .instructions()
            .iter()
            .map(|ix| {
                let program_id = accounts[ix.program_id_index as usize];
                let data = ix.data.clone();
                InstructionGroup {
                    outer_instruction: Instruction {
                        program_id,
                        data,
                        accounts: accounts.into(),
                    },
                    inner_instructions: Vec::new(),
                }
            })
            .collect();

        let meta = meta.ok_or(IngesterError::ParserError("Missing metadata".to_string()))?;
        if let OptionSerializer::Some(inner_instructions_vec) = meta.inner_instructions.as_ref() {
            for inner_instructions in inner_instructions_vec.iter() {
                let index = inner_instructions.index;
                for ui_instruction in inner_instructions.instructions.iter() {
                    match ui_instruction {
                        UiInstruction::Compiled(ui_compiled_instruction) => {
                            let program_id =
                                accounts[ui_compiled_instruction.program_id_index as usize];
                            let data = bs58::decode(&ui_compiled_instruction.data)
                                .into_vec()
                                .map_err(|e| IngesterError::ParserError(e.to_string()))?;
                            let accounts = ui_compiled_instruction
                                .accounts
                                .iter()
                                .map(|account_index| accounts[*account_index as usize])
                                .collect();
                            instruction_groups[index as usize].inner_instructions.push(
                                Instruction {
                                    program_id,
                                    data: data.into(),
                                    accounts,
                                },
                            );
                        }
                        // Plerkle serialization worked without parsed instructions, so no implementation needed currently.
                        UiInstruction::Parsed(_) => {
                            return Err(IngesterError::ParserError(
                                "Parsed instructions are not implemented".to_string(),
                            ));
                        }
                    }
                }
            }
        };
        Ok(TransactionInfo {
            slot,
            block_time,
            instruction_groups,
        })
    }
}
