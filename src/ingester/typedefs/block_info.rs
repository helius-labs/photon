use solana_sdk::{
    clock::{Slot, UnixTimestamp},
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
    EncodedTransactionWithStatusMeta, UiConfirmedBlock, UiInstruction, UiTransactionStatusMeta,
};
use std::{fmt, str::FromStr};

use std::convert::TryFrom;

use crate::dao::typedefs::hash::Hash;

use super::super::error::IngesterError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instruction {
    pub program_id: Pubkey,
    pub data: Vec<u8>,
    pub accounts: Vec<Pubkey>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstructionGroup {
    pub outer_instruction: Instruction,
    pub inner_instructions: Vec<Instruction>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionInfo {
    pub instruction_groups: Vec<InstructionGroup>,
    pub signature: Signature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockInfo {
    pub metadata: BlockMetadata,
    pub transactions: Vec<TransactionInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMetadata {
    pub slot: Slot,
    // In Solana, slots can be skipped. So there are not necessarily sequential.
    pub parent_slot: Slot,
    pub block_time: UnixTimestamp,
    pub blockhash: Hash,
    pub parent_blockhash: Hash,
    pub block_height: u64,
}

pub fn parse_ui_confirmed_blocked(
    block: UiConfirmedBlock,
    slot: Slot,
) -> Result<BlockInfo, IngesterError> {
    let UiConfirmedBlock {
        parent_slot,
        block_time,
        transactions,
        blockhash,
        previous_blockhash,
        block_height,
        ..
    } = block;

    let transactions: Result<Vec<_>, _> = transactions
        .unwrap_or(Vec::new())
        .into_iter()
        .map(_parse_transaction)
        .collect();

    Ok(BlockInfo {
        transactions: transactions?,
        metadata: BlockMetadata {
            parent_slot,
            block_time: block_time
                .ok_or(IngesterError::ParserError("Missing block_time".to_string()))?,
            slot,
            blockhash: Hash::try_from(blockhash.as_str()).map_err(|e| {
                IngesterError::ParserError(format!("Failed to parse blockhash: {}", e))
            })?,
            parent_blockhash: Hash::try_from(previous_blockhash.as_str()).map_err(|e| {
                IngesterError::ParserError(format!("Failed to parse previous_blockhash: {}", e))
            })?,
            block_height: block_height.ok_or(IngesterError::ParserError(
                "Missing block_height".to_string(),
            ))?,
        },
    })
}

fn _parse_transaction(
    transaction: EncodedTransactionWithStatusMeta,
) -> Result<TransactionInfo, IngesterError> {
    let EncodedTransactionWithStatusMeta {
        transaction, meta, ..
    } = transaction;

    let versioned_transaction: VersionedTransaction = transaction.decode().ok_or(
        IngesterError::ParserError("Transaction cannot be decoded".to_string()),
    )?;

    let signature = versioned_transaction.signatures[0];
    let instruction_groups = parse_instruction_groups(versioned_transaction, meta)?;
    Ok(TransactionInfo {
        instruction_groups,
        signature,
    })
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
            "TransactionInfo {{ instruction_groups: [{}] }}",
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
        let EncodedConfirmedTransactionWithStatusMeta { transaction, .. } = tx;

        let EncodedTransactionWithStatusMeta {
            transaction, meta, ..
        } = transaction;

        let versioned_transaction: VersionedTransaction = transaction.decode().ok_or(
            IngesterError::ParserError("Transaction cannot be decoded".to_string()),
        )?;
        let signature = versioned_transaction.signatures[0];

        Ok(TransactionInfo {
            instruction_groups: parse_instruction_groups(versioned_transaction, meta)?,
            signature: signature,
        })
    }
}

pub fn parse_instruction_groups(
    versioned_transaction: VersionedTransaction,
    meta: Option<UiTransactionStatusMeta>,
) -> Result<Vec<InstructionGroup>, IngesterError> {
    let mut accounts = Vec::from(versioned_transaction.message.static_account_keys());
    if versioned_transaction
        .message
        .address_table_lookups()
        .is_some()
    {
        if let Some(meta) = &meta {
            if let OptionSerializer::Some(loaded_addresses) = meta.loaded_addresses.clone() {
                for address in loaded_addresses
                    .writable
                    .iter()
                    .chain(loaded_addresses.readonly.iter())
                {
                    let pubkey = Pubkey::from_str(&address)
                        .map_err(|e| IngesterError::ParserError(e.to_string()))?;
                    accounts.push(pubkey);
                }
            }
        }
    }

    // Parse outer instructions and bucket them into groups
    let mut instruction_groups: Vec<InstructionGroup> = versioned_transaction
        .message
        .instructions()
        .iter()
        .map(|ix| {
            let program_id = accounts[ix.program_id_index as usize];
            let data = ix.data.clone();
            let accounts: Vec<Pubkey> = ix
                .accounts
                .iter()
                .map(|account_index| accounts[*account_index as usize])
                .collect();

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

    // Parse inner instructions and place them into the correct instruction group
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
                        instruction_groups[index as usize]
                            .inner_instructions
                            .push(Instruction {
                                program_id,
                                data: data.into(),
                                accounts,
                            });
                    }
                    UiInstruction::Parsed(_) => {
                        return Err(IngesterError::ParserError(
                            "Parsed instructions are not implemented yet".to_string(),
                        ));
                    }
                }
            }
        }
    };

    Ok(instruction_groups)
}
