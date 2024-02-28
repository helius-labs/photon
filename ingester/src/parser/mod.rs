use std::collections::HashSet;

use blockbuster::instruction::{order_instructions, InstructionBundle};
use log::{debug, info};
use plerkle_serialization::TransactionInfo;
use plerkle_serialization::{AccountInfo, Pubkey as FBPubkey};
use sea_orm::Transaction;
use solana_program::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::program;
use solana_sdk::{signature::Signature, transaction::VersionedTransaction};

use crate::error::IngesterError;

use self::bundle::{EventBundle, PublicStateTransitionBundle};

pub mod bundle;

const COMPRESSED_TOKEN_PROGRAM_ID: Pubkey = pubkey!("9sixVEthz2kMSKfeApZXHwuboT6DZuT6crAYJTciUCqE");
const NOOP_PROGRAM_ID: Pubkey = pubkey!("noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV");

pub fn parse_transaction(tx: TransactionInfo) -> Result<EventBundle, IngesterError> {
    let sig: Option<&str> = tx.signature();
    info!("Handling Transaction: {:?}", sig);

    let mut programs = HashSet::new();
    programs.insert(COMPRESSED_TOKEN_PROGRAM_ID);
    let ref_set: HashSet<&[u8]> = programs.iter().map(|k| k.as_ref()).collect();

    let instructions = order_instructions(ref_set, &tx);

    let accounts = tx.account_keys().unwrap_or_default();
    let slot = tx.slot();
    let txn_id = tx.signature().unwrap_or("");
    let mut keys: Vec<FBPubkey> = Vec::with_capacity(accounts.len());
    for k in accounts.into_iter() {
        keys.push(*k);
    }

    let ixlen = instructions.len();
    debug!("Instructions: {}", ixlen);
    let contains = instructions
        .iter()
        .filter(|(ib, _inner)| ib.0 .0.as_ref() == COMPRESSED_TOKEN_PROGRAM_ID.as_ref());

    debug!("Instructions account compression: {}", contains.count());
    for (outer_ix, inner_ix) in instructions {
        let (program, instruction) = outer_ix;

        
        let ix_accounts = instruction.accounts().unwrap().iter().collect::<Vec<_>>();
        let ix_account_len = ix_accounts.len();
        let max = ix_accounts.iter().max().copied().unwrap_or(0) as usize;
        if keys.len() < max {
            return Err(IngesterError::ParsingError(
                "Missing Accounts in serialized Ixn/Txn".to_string(),
            ));
        }
        let ix_accounts =
            ix_accounts
                .iter()
                .fold(Vec::with_capacity(ix_account_len), |mut acc, a| {
                    if let Some(key) = keys.get(*a as usize) {
                        acc.push(*key);
                    }
                    acc
                });
        let ix = InstructionBundle {
            txn_id,
            program,
            instruction: Some(instruction),
            inner_ix,
            keys: ix_accounts.as_slice(),
            slot,
        };
        _parse_instruction_bundle(&ix);
    }

    panic!("Not implemented error");
}

fn _parse_instruction_bundle(bundle: &InstructionBundle) -> Result<(), IngesterError> {
    let InstructionBundle {
        txn_id,
        instruction,
        inner_ix,
        keys,
        ..
    } = bundle;
    let outer_ix_data = match instruction {
        Some(compiled_ix) if compiled_ix.data().is_some() => {
            let data = compiled_ix.data().unwrap();
            data.iter().collect::<Vec<_>>()
        }
        _ => {
            return Err(IngesterError::ParsingError(
                "No outer instruction data".to_string(),
            ));
        }
    };
    if let Some(ixs) = inner_ix {
        for ix in ixs {
            if true {
                // ix.0.0 == NOOP_ID.bytes().to_owned() {
                let cix = ix.1;
                if let Some(inner_ix_data) = cix.data() {
                    let inner_ix_data = inner_ix_data.iter().collect::<Vec<_>>();
                    if !inner_ix_data.is_empty() {}
                } else {
                    return Err(IngesterError::ParsingError(
                        "No inner instruction data".to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}
