use borsh::BorshDeserialize;
use byteorder::{ByteOrder, LittleEndian};
use light_utils::instruction::event::event_from_light_transaction;
use indexer_events::{IndexedMerkleTreeEvent, MerkleTreeEvent, NullifierEvent};
use log::{debug, info};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use state_update::{IndexedTreeLeafUpdate, LeafNullification};

use crate::common::typedefs::{
    account::{Account, AccountData},
    bs64_string::Base64String,
    hash::Hash,
    serializable_pubkey::SerializablePubkey,
    unsigned_integer::UnsignedInteger,
};

use super::{error::IngesterError, typedefs::block_info::TransactionInfo};

use self::{
    indexer_events::{CompressedAccount, PublicTransactionEvent},
    state_update::{AccountTransaction, StateUpdate, Transaction},
};

pub mod indexer_events;
pub mod state_update;

use solana_program::pubkey;
use crate::ingester::parser::indexer_events::{CompressedAccountData, MerkleTreeSequenceNumber, OutputCompressedAccountWithPackedContext};

pub const ACCOUNT_COMPRESSION_PROGRAM_ID: Pubkey =
    pubkey!("compr6CUsB5m2jS4Y3831ztGSTnDpnKJTKS95d64XVq");
const SYSTEM_PROGRAM: Pubkey = pubkey!("11111111111111111111111111111111");
const NOOP_PROGRAM_ID: Pubkey = pubkey!("noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV");
const VOTE_PROGRAM_ID: Pubkey = pubkey!("Vote111111111111111111111111111111111111111");

pub fn parse_transaction(tx: &TransactionInfo, slot: u64) -> Result<StateUpdate, IngesterError> {
    let mut state_updates = Vec::new();
    let mut is_compression_transaction = false;

    let mut logged_transaction = false;

    let enable_new_parse_logic = true;

    for instruction_group in tx.clone().instruction_groups {
        let mut ordered_instructions = Vec::new();
        ordered_instructions.push(instruction_group.outer_instruction.clone());
        ordered_instructions.extend(instruction_group.inner_instructions.clone());

        let mut vec_accounts = Vec::<Vec<Pubkey>>::new();
        let mut vec_instructions_data = Vec::new();
        vec_instructions_data.push(instruction_group.outer_instruction.data);

        instruction_group.inner_instructions.iter().find_map(|inner_instruction| {
            vec_instructions_data.push(inner_instruction.data.clone());
            vec_accounts.push(
                inner_instruction
                    .accounts.clone()

            );
            None::<PublicTransactionEvent>
        });

        let e = if enable_new_parse_logic {
            event_from_light_transaction(&*vec_instructions_data, vec_accounts)
                .map_err(|e| IngesterError::ParserError(e.to_string()))?
        } else {
            None
        };

        if let Some(public_transaction_event) = e {
            let event = PublicTransactionEvent {
                input_compressed_account_hashes: public_transaction_event.input_compressed_account_hashes,
                output_compressed_account_hashes: public_transaction_event.output_compressed_account_hashes,
                output_compressed_accounts: public_transaction_event.output_compressed_accounts.iter().map(|x| OutputCompressedAccountWithPackedContext {
                    compressed_account: CompressedAccount {
                        owner: x.compressed_account.owner,
                        lamports: x.compressed_account.lamports,
                        address: x.compressed_account.address,
                        data: x.compressed_account.data.as_ref().map(|d| CompressedAccountData {
                            discriminator: d.discriminator,
                            data: d.data.clone(),
                            data_hash: d.data_hash,
                        }),
                    },
                    merkle_tree_index: x.merkle_tree_index,
                }).collect(),
                output_leaf_indices: public_transaction_event.output_leaf_indices,
                sequence_numbers: public_transaction_event.sequence_numbers.iter().map(|x| MerkleTreeSequenceNumber {
                    pubkey: x.pubkey,
                    seq: x.seq,
                }).collect(),
                relay_fee: public_transaction_event.relay_fee,
                is_compress: public_transaction_event.is_compress,
                compression_lamports: public_transaction_event.compress_or_decompress_lamports,
                pubkey_array: public_transaction_event.pubkey_array,
                message: public_transaction_event.message,
            };
            let state_update = parse_public_transaction_event(tx.signature, slot, event)?;
            is_compression_transaction = true;
            state_updates.push(state_update);

        } else {
            for (index, instruction) in ordered_instructions.iter().enumerate() {
                if ordered_instructions.len() - index > 3 {
                    let next_instruction = &ordered_instructions[index + 1];
                    let next_next_instruction = &ordered_instructions[index + 2];
                    let next_next_next_instruction = &ordered_instructions[index + 3];
                // We need to check if the account compression instruction contains a noop account to determine
                // if the instruction emits a noop event. If it doesn't then we want avoid indexing
                // the following noop instruction because it'll contain either irrelevant or malicious data.

                if ACCOUNT_COMPRESSION_PROGRAM_ID == instruction.program_id
                    && next_instruction.program_id == SYSTEM_PROGRAM
                    && next_next_instruction.program_id == SYSTEM_PROGRAM
                    && next_next_next_instruction.program_id == NOOP_PROGRAM_ID {
                    if !logged_transaction {
                        info!(
                            "Indexing transaction with slot {} and id {}",
                            slot, tx.signature
                        );
                        logged_transaction = true;
                    }
                    is_compression_transaction = true;

                    if tx.error.is_none() {
                        let public_transaction_event = PublicTransactionEvent::deserialize(
                            &mut next_next_next_instruction.data.as_slice(),
                        )
                        .map_err(|e| {
                            IngesterError::ParserError(format!(
                                "Failed to deserialize PublicTransactionEvent: {}",
                                e
                            ))
                        })?;
                        let state_update = parse_public_transaction_event(
                            tx.signature,
                            slot,
                            public_transaction_event,
                        )?;
                        state_updates.push(state_update);
                    }
                }
            }
            if ordered_intructions.len() - index > 2 {
                debug!("ordered_instructions: {:?}", ordered_intructions);

                    let next_instruction = &ordered_instructions[index + 1];
                    let next_next_instruction = &ordered_instructions[index + 2];
                // We need to check if the account compression instruction contains a noop account to determine
                // if the instruction emits a noop event. If it doesn't then we want avoid indexing
                // the following noop instruction because it'll contain either irrelevant or malicious data.

                if ACCOUNT_COMPRESSION_PROGRAM_ID == instruction.program_id
                    && next_instruction.program_id == SYSTEM_PROGRAM
                    && next_next_instruction.program_id == NOOP_PROGRAM_ID {
                    if !logged_transaction {
                        debug!(
                            "Indexing transaction with slot {} and id {}",
                            slot, tx.signature
                        );
                        logged_transaction = true;
                    }
                    is_compression_transaction = true;

                    if tx.error.is_none() {
                        let public_transaction_event = PublicTransactionEvent::deserialize(
                            &mut next_next_instruction.data.as_slice(),
                        )
                        .map_err(|e| {
                            IngesterError::ParserError(format!(
                                "Failed to deserialize PublicTransactionEvent: {}",
                                e
                            ))
                        })?;
                        let state_update = parse_public_transaction_event(
                            tx.signature,
                            slot,
                            public_transaction_event,
                        )?;
                        state_updates.push(state_update);
                    }
                }
            }
            if ordered_intructions.len() - index > 1 {
                debug!("Parsing tx with signature: {}", tx.signature);
                let next_instruction = &ordered_intructions[index + 1];
                if ACCOUNT_COMPRESSION_PROGRAM_ID == instruction.program_id
                    && next_instruction.program_id == NOOP_PROGRAM_ID
                {
                    is_compression_transaction = true;
                    if tx.error.is_none() {
                        info!("Indexing tx with signature: {}", tx.signature);
                        let merkle_tree_event =
                            MerkleTreeEvent::deserialize(&mut next_instruction.data.as_slice())
                                .map_err(|e| {
                                    IngesterError::ParserError(format!(
                                        "Failed to deserialize NullifierEvent: {}",
                                        e
                                    ))
                                })?;
                        info!("Merkle tree event: {:?}", merkle_tree_event);
                        let state_update = match merkle_tree_event {
                            MerkleTreeEvent::V2(nullifier_event) => {
                                parse_nullifier_event(tx.signature, nullifier_event)?
                            }
                            MerkleTreeEvent::V3(indexed_merkle_tree_event) => {
                                parse_indexed_merkle_tree_update(indexed_merkle_tree_event)?
                            }
                            _ => {
                                return Err(IngesterError::ParserError(
                                    "Expected nullifier event or merkle tree update".to_string(),
                                ))
                            }
                        };
                        state_updates.push(state_update);
                    }
                }
            }
        }
    }
    }
    let mut state_update = StateUpdate::merge_updates(state_updates);

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

fn parse_account_data(
    compressed_account: CompressedAccount,
    hash: [u8; 32],
    tree: Pubkey,
    leaf_index: u32,
    slot: u64,
    seq: u64,
) -> Account {
    info!("Parsing account data: {:?}, hash: {:?}, tree: {:?}, leaf_index: {:?}, slot: {:?}, seq: {:?}", compressed_account, hash, tree, leaf_index, slot, seq);
    let CompressedAccount {
        owner,
        lamports,
        address,
        data,
    } = compressed_account;

    let data = data.map(|d| AccountData {
        discriminator: UnsignedInteger(LittleEndian::read_u64(&d.discriminator)),
        data: Base64String(d.data),
        data_hash: Hash::from(d.data_hash),
    });

    Account {
        owner: owner.into(),
        lamports: UnsignedInteger(lamports),
        address: address.map(SerializablePubkey::from),
        data,
        hash: hash.into(),
        slot_created: UnsignedInteger(slot),
        leaf_index: UnsignedInteger(leaf_index as u64),
        tree: SerializablePubkey::from(tree),
        seq: UnsignedInteger(seq),
    }
}

fn parse_indexed_merkle_tree_update(
    indexed_merkle_tree_event: IndexedMerkleTreeEvent,
) -> Result<StateUpdate, IngesterError> {
    info!("Parsing indexed merkle tree update: {:?}", indexed_merkle_tree_event);
    let IndexedMerkleTreeEvent {
        id,
        updates,
        mut seq,
    } = indexed_merkle_tree_event;
    let mut state_update = StateUpdate::new();

    for update in updates {
        for (leaf, hash) in [
            (update.new_low_element, update.new_low_element_hash),
            (update.new_high_element, update.new_high_element_hash),
        ]
        .iter()
        {
            let indexed_tree_leaf_update = IndexedTreeLeafUpdate {
                tree: Pubkey::try_from(id).map_err(|_e| {
                    IngesterError::ParserError("Unable to parse tree pubkey".to_string())
                })?,
                hash: *hash,
                leaf: *leaf,
                seq,
            };
            seq += 1;
            state_update.indexed_merkle_tree_updates.insert(
                (indexed_tree_leaf_update.tree, leaf.index as u64),
                indexed_tree_leaf_update,
            );
        }
    }

    Ok(state_update)
}

fn parse_nullifier_event(
    tx: Signature,
    nullifier_event: NullifierEvent,
) -> Result<StateUpdate, IngesterError> {
    info!("Parsing nullifier event: {:?}, tx: {:?}", nullifier_event, tx);
    let NullifierEvent {
        id,
        nullified_leaves_indices,
        seq,
    } = nullifier_event;

    let mut state_update = StateUpdate::new();

    for (i, leaf_index) in nullified_leaves_indices.iter().enumerate() {
        let leaf_nullification: LeafNullification = {
            LeafNullification {
                tree: Pubkey::try_from(id).map_err(|_e| {
                    IngesterError::ParserError("Unable to parse tree pubkey".to_string())
                })?,
                leaf_index: *leaf_index,
                seq: seq + i as u64,
                signature: tx,
            }
        };
        state_update.leaf_nullifications.insert(leaf_nullification);
    }

    Ok(state_update)
}

fn parse_public_transaction_event(
    tx: Signature,
    slot: u64,
    transaction_event: PublicTransactionEvent,
) -> Result<StateUpdate, IngesterError> {
    info!("Parsing public transaction event: {:?}, tx: {:?}, slot: {:?}", transaction_event, tx, slot);
    let PublicTransactionEvent {
        input_compressed_account_hashes,
        output_compressed_account_hashes,
        output_compressed_accounts,
        pubkey_array,
        sequence_numbers,
        ..
    } = transaction_event;

    let mut state_update = StateUpdate::new();

    let mut tree_to_seq_number = sequence_numbers
        .iter()
        .map(|seq| (seq.pubkey, seq.seq))
        .collect::<std::collections::HashMap<_, _>>();

    for hash in input_compressed_account_hashes {
        state_update.in_accounts.insert(hash.into());
    }

    for ((out_account, hash), leaf_index) in output_compressed_accounts
        .into_iter()
        .zip(output_compressed_account_hashes)
        .zip(transaction_event.output_leaf_indices.iter())
    {
        info!("Parsing output compressed account: {:?}, hash: {:?}, leaf_index: {:?}", out_account, hash, leaf_index);
        let tree = pubkey_array[out_account.merkle_tree_index as usize];
        let seq = tree_to_seq_number
            .get_mut(&tree)
            .ok_or_else(|| IngesterError::ParserError("Missing sequence number".to_string()))?;
        info!("Tree: {:?}, seq: {:?}", tree, seq);
        let enriched_account = parse_account_data(
            out_account.compressed_account,
            hash,
            tree,
            *leaf_index,
            slot,
            *seq,
        );
        info!("Enriched account: {:?}", enriched_account);
        *seq += 1;
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
                    hash: a.hash.clone(),
                    signature: tx,
                }),
        );

    Ok(state_update)
}
