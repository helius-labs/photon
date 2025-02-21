use crate::common::typedefs::account::AccountWithContext;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::PublicTransactionEvent;
use crate::ingester::parser::state_update::{AccountTransaction, StateUpdate};
use lazy_static::lazy_static;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::collections::HashMap;
use std::str::FromStr;

// TODO: add a table which stores tree metadata: tree_pubkey | queue_pubkey | type | ...
lazy_static! {
    pub static ref QUEUE_TREE_MAPPING: HashMap<String, String> = {
        let mut m = HashMap::new();
        m.insert(
            "6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU".to_string(), // queue
            "HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu".to_string(), // tree
        );
        m
    };
}

fn queue_to_tree(queue: &str) -> Option<Pubkey> {
    QUEUE_TREE_MAPPING
        .get(queue)
        .map(|x| Pubkey::from_str(x.as_str()).unwrap())
}

pub fn parse_public_transaction_event(
    tx: Signature,
    slot: u64,
    transaction_event: PublicTransactionEvent,
) -> Result<StateUpdate, IngesterError> {
    let PublicTransactionEvent {
        input_compressed_account_hashes,
        output_compressed_account_hashes,
        output_compressed_accounts,
        pubkey_array,
        sequence_numbers,
        ..
    } = transaction_event;

    let mut state_update = StateUpdate::new();

    let mut has_batched_instructions = false;
    for seq in sequence_numbers.iter() {
        if queue_to_tree(&seq.pubkey.to_string()).is_some() {
            has_batched_instructions = true;
            break;
        }
    }

    let mut tree_to_seq_number = HashMap::new();
    if has_batched_instructions {
        for seq in sequence_numbers.iter() {
            if let Some(tree) = queue_to_tree(&seq.pubkey.to_string()) {
                tree_to_seq_number.insert(tree, seq.seq);
            }
        }
    } else {
        tree_to_seq_number = sequence_numbers
            .iter()
            .map(|seq| (seq.pubkey, seq.seq))
            .collect::<HashMap<_, _>>();
    }

    for hash in input_compressed_account_hashes {
        state_update.in_accounts.insert(hash.into());
    }

    for ((out_account, hash), leaf_index) in output_compressed_accounts
        .into_iter()
        .zip(output_compressed_account_hashes)
        .zip(transaction_event.output_leaf_indices.iter())
    {
        let mut tree = pubkey_array[out_account.merkle_tree_index as usize];
        let mut queue = queue_to_tree(&tree.to_string());
        if let Some(q) = queue {
            // swap tree and q
            let temp = tree;
            tree = q;
            queue = Some(temp);
        };

        let mut seq = None;
        if queue.is_none() {
            seq = Some(*tree_to_seq_number.get(&tree).ok_or_else(|| {
                IngesterError::ParserError("Missing sequence number".to_string())
            })?);
        }
        let enriched_account = AccountWithContext::new(
            out_account.compressed_account,
            hash,
            tree,
            queue,
            *leaf_index,
            slot,
            seq,
            queue.is_some(),
            false,
            None,
            None,
        );

        if queue.is_none() {
            let seq = tree_to_seq_number
                .get_mut(&tree)
                .ok_or_else(|| IngesterError::ParserError("Missing sequence number".to_string()))?;
            *seq += 1;
        }

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
