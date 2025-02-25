use crate::common::typedefs::account::AccountWithContext;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::PublicTransactionEvent;
use crate::ingester::parser::state_update::{AccountTransaction, StateUpdate};
use lazy_static::lazy_static;
use light_merkle_tree_metadata::merkle_tree::TreeType;
use solana_program::pubkey::Pubkey;
use solana_sdk::pubkey;
use solana_sdk::signature::Signature;
use std::collections::HashMap;

pub struct TreeAndQueue {
    tree: Pubkey,
    queue: Pubkey,
    _height: u16,
    pub(crate) tree_type: TreeType,
}

// TODO: add a table which stores tree metadata: tree_pubkey | queue_pubkey | type | ...
lazy_static! {
    pub static ref QUEUE_TREE_MAPPING: HashMap<String, TreeAndQueue> = {
        let mut m = HashMap::new();

        m.insert(
            "6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU".to_string(),
            TreeAndQueue {
                tree: pubkey!("HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu"),
                queue: pubkey!("6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU"),
                _height: 32,
                tree_type: TreeType::BatchedState,
            },
        );

        m.insert(
            "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT".to_string(),
            TreeAndQueue {
                tree: pubkey!("smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT"),
                queue: pubkey!("nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148"),
                _height: 26,
                tree_type: TreeType::State,
            },
        );

        m.insert(
            "smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho".to_string(),
            TreeAndQueue {
                tree: pubkey!("smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho"),
                queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
                _height: 26,
                tree_type: TreeType::State,
            },
        );

        m.insert(
            "HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu".to_string(),
            TreeAndQueue {
                tree: pubkey!("HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu"),
                queue: pubkey!("6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU"),
                _height: 32,
                tree_type: TreeType::BatchedState,
            },
        );

        m.insert(
            "nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148".to_string(),
            TreeAndQueue {
                tree: pubkey!("smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT"),
                queue: pubkey!("nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148"),
                _height: 26,
                tree_type: TreeType::State,
            },
        );

        m.insert(
            "nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X".to_string(),
            TreeAndQueue {
                tree: pubkey!("smt2rJAFdyJJupwMKAqTNAJwvjhmiZ4JYGZmbVRw1Ho"),
                queue: pubkey!("nfq2hgS7NYemXsFaFUCe3EMXSDSfnZnAe27jC6aPP1X"),
                _height: 26,
                tree_type: TreeType::State,
            },
        );

        m
    };
}

pub fn map_tree_and_queue_accounts<'a>(pubkey: String) -> Option<&'a TreeAndQueue> {
    QUEUE_TREE_MAPPING.get(pubkey.as_str())
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
    let mut tree_to_seq_number = HashMap::new();

    for seq in sequence_numbers.iter() {
        if let Some(queue_to_tree) = map_tree_and_queue_accounts(seq.pubkey.to_string()) {
            if queue_to_tree.tree_type == TreeType::BatchedState
                || queue_to_tree.tree_type == TreeType::BatchedAddress
            {
                tree_to_seq_number.insert(queue_to_tree.tree, seq.seq);
                has_batched_instructions = true;
            }
        }
    }

    if !has_batched_instructions {
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
        let tree = pubkey_array[out_account.merkle_tree_index as usize];
        let tree_and_queue = map_tree_and_queue_accounts(tree.to_string())
            .ok_or(IngesterError::ParserError("Missing queue".to_string()))?;

        let mut seq = None;
        if tree_and_queue.tree_type == TreeType::State {
            seq = Some(*tree_to_seq_number.get(&tree).ok_or_else(|| {
                IngesterError::ParserError("Missing sequence number".to_string())
            })?);

            let seq = tree_to_seq_number
                .get_mut(&tree)
                .ok_or_else(|| IngesterError::ParserError("Missing sequence number".to_string()))?;
            *seq += 1;
        }

        let in_output_queue = tree_and_queue.tree_type == TreeType::BatchedState;
        let enriched_account = AccountWithContext::new(
            out_account.compressed_account,
            hash,
            tree_and_queue.tree,
            tree_and_queue.queue,
            *leaf_index,
            slot,
            seq,
            in_output_queue,
            false,
            None,
            None,
            tree_and_queue.tree_type as u16,
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
