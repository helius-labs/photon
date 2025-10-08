use crate::common::typedefs::account::AccountWithContext;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::PublicTransactionEvent;
use crate::ingester::parser::state_update::{AccountTransaction, StateUpdate};
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::parser::{get_compression_program_id, NOOP_PROGRAM_ID};
use crate::ingester::typedefs::block_info::{Instruction, TransactionInfo};
use anchor_lang::AnchorDeserialize;
use light_compressed_account::TreeType;
use log::info;
use solana_sdk::signature::Signature;
use std::collections::HashMap;

pub async fn parse_public_transaction_event_v1<T>(
    conn: &T,
    tx: &TransactionInfo,
    slot: u64,
    compression_instruction: &Instruction,
    noop_instruction: &Instruction,
) -> Result<Option<StateUpdate>, IngesterError>
where
    T: sea_orm::ConnectionTrait + sea_orm::TransactionTrait,
{
    if get_compression_program_id() == compression_instruction.program_id
        && noop_instruction.program_id == NOOP_PROGRAM_ID
        && tx.error.is_none()
    {
        info!(
            "Indexing transaction with slot {} and id {}",
            slot, tx.signature
        );

        let public_transaction_event = PublicTransactionEvent::deserialize(
            &mut noop_instruction.data.as_slice(),
        )
        .map_err(|e| {
            IngesterError::ParserError(format!(
                "Failed to deserialize PublicTransactionEvent: {}",
                e
            ))
        })?;
        create_state_update_v1(conn, tx.signature, slot, public_transaction_event.into())
            .await
            .map(Some)
    } else {
        Ok(None)
    }
}

pub async fn create_state_update_v1<T>(
    conn: &T,
    tx: Signature,
    slot: u64,
    transaction_event: PublicTransactionEvent,
) -> Result<StateUpdate, IngesterError>
where
    T: sea_orm::ConnectionTrait + sea_orm::TransactionTrait,
{
    let mut state_update = StateUpdate::new();
    let mut tree_to_seq_number = transaction_event
        .sequence_numbers
        .iter()
        .map(|seq| (seq.pubkey, seq.seq))
        .collect::<HashMap<_, _>>();

    for hash in transaction_event.input_compressed_account_hashes {
        state_update.in_accounts.insert(hash.into());
    }

    for ((out_account, hash), leaf_index) in transaction_event
        .output_compressed_accounts
        .into_iter()
        .zip(transaction_event.output_compressed_account_hashes)
        .zip(transaction_event.output_leaf_indices.iter())
    {
        let tree = transaction_event.pubkey_array[out_account.merkle_tree_index as usize];
        let tree_and_queue = match TreeInfo::get_by_pubkey(conn, &tree)
            .await
            .map_err(|e| IngesterError::ParserError(format!("Failed to get tree info: {}", e)))?
        {
            Some(info) => info,
            None => {
                if super::SKIP_UNKNOWN_TREES {
                    log::warn!("Skipping unknown tree: {}", tree.to_string());
                    continue;
                } else {
                    return Err(IngesterError::ParserError(format!(
                        "Missing queue for tree: {}",
                        tree.to_string()
                    )));
                }
            }
        };

        let mut seq = None;
        if tree_and_queue.tree_type == TreeType::StateV1 {
            seq = Some(*tree_to_seq_number.get(&tree).ok_or_else(|| {
                IngesterError::ParserError("Missing sequence number".to_string())
            })?);

            let seq = tree_to_seq_number
                .get_mut(&tree)
                .ok_or_else(|| IngesterError::ParserError("Missing sequence number".to_string()))?;
            *seq += 1;
        }

        let in_output_queue = tree_and_queue.tree_type == TreeType::StateV2;
        let tree_pubkey = solana_pubkey::Pubkey::new_from_array(tree_and_queue.tree.to_bytes());
        let queue_pubkey = solana_pubkey::Pubkey::new_from_array(tree_and_queue.queue.to_bytes());

        let enriched_account = AccountWithContext::new(
            out_account.compressed_account.clone(),
            &hash,
            tree_pubkey,
            queue_pubkey,
            *leaf_index,
            slot,
            seq,
            in_output_queue,
            false,
            None,
            None,
            tree_and_queue.tree_type,
        );

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
