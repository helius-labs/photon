use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::{
    BatchPublicTransactionEvent, CompressedAccount, CompressedAccountData,
    MerkleTreeSequenceNumberV1, MerkleTreeSequenceNumberV2,
    OutputCompressedAccountWithPackedContext, PublicTransactionEvent,
};
use crate::ingester::parser::state_update::{AccountTransaction, StateUpdate};
use crate::ingester::parser::tx_event_parser::create_state_update_v1;

use super::state_update::AddressQueueUpdate;
use crate::common::typedefs::hash::Hash;
use light_compressed_account::Pubkey as LightPubkey;
use light_event::parse::event_from_light_transaction;
use solana_pubkey::Pubkey;
use solana_signature::Signature;

// Helper function for pubkey conversion
fn to_light_pubkey(pubkey: &Pubkey) -> LightPubkey {
    LightPubkey::from(pubkey.to_bytes())
}

pub fn parse_public_transaction_event_v2(
    program_ids: &[Pubkey],
    instructions: &[Vec<u8>],
    accounts: Vec<Vec<Pubkey>>,
) -> Option<Vec<BatchPublicTransactionEvent>> {
    let light_program_ids: Vec<LightPubkey> =
        program_ids.iter().map(|p| to_light_pubkey(p)).collect();
    let light_accounts: Vec<Vec<LightPubkey>> = accounts
        .into_iter()
        .map(|acc_vec| {
            acc_vec
                .into_iter()
                .map(|acc| to_light_pubkey(&acc))
                .collect()
        })
        .collect();
    let events =
        event_from_light_transaction(&light_program_ids, instructions, light_accounts).ok()?;
    events.map(|events| {
        events
            .into_iter()
            .map(|public_transaction_event| {
                let event = PublicTransactionEvent {
                    input_compressed_account_hashes: public_transaction_event
                        .event
                        .input_compressed_account_hashes,
                    output_compressed_account_hashes: public_transaction_event
                        .event
                        .output_compressed_account_hashes,
                    output_compressed_accounts: public_transaction_event
                        .event
                        .output_compressed_accounts
                        .iter()
                        .map(|x| OutputCompressedAccountWithPackedContext {
                            compressed_account: CompressedAccount {
                                owner: x.compressed_account.owner,
                                lamports: x.compressed_account.lamports,
                                address: x.compressed_account.address,
                                data: x.compressed_account.data.as_ref().map(|d| {
                                    CompressedAccountData {
                                        discriminator: d.discriminator,
                                        data: d.data.clone(),
                                        data_hash: d.data_hash,
                                    }
                                }),
                            },
                            merkle_tree_index: x.merkle_tree_index,
                        })
                        .collect(),
                    output_leaf_indices: public_transaction_event.event.output_leaf_indices,
                    sequence_numbers: public_transaction_event
                        .event
                        .sequence_numbers
                        .iter()
                        .map(|x| MerkleTreeSequenceNumberV1 {
                            pubkey: x.tree_pubkey,
                            seq: x.seq,
                        })
                        .collect(),
                    relay_fee: public_transaction_event.event.relay_fee,
                    is_compress: public_transaction_event.event.is_compress,
                    compression_lamports: public_transaction_event
                        .event
                        .compress_or_decompress_lamports,
                    pubkey_array: public_transaction_event.event.pubkey_array,
                    message: public_transaction_event.event.message,
                };

                let batch_public_transaction_event = BatchPublicTransactionEvent {
                    event,
                    new_addresses: public_transaction_event.new_addresses,
                    input_sequence_numbers: public_transaction_event
                        .input_sequence_numbers
                        .iter()
                        .map(|x| MerkleTreeSequenceNumberV2 {
                            tree_pubkey: x.tree_pubkey,
                            queue_pubkey: x.queue_pubkey,
                            tree_type: x.tree_type,
                            seq: x.seq,
                        })
                        .collect(),
                    address_sequence_numbers: public_transaction_event
                        .address_sequence_numbers
                        .iter()
                        .map(|x| MerkleTreeSequenceNumberV2 {
                            tree_pubkey: x.tree_pubkey,
                            queue_pubkey: x.queue_pubkey,
                            tree_type: x.tree_type,
                            seq: x.seq,
                        })
                        .collect(),
                    batch_input_accounts: public_transaction_event.batch_input_accounts,
                    tx_hash: public_transaction_event.tx_hash,
                };

                batch_public_transaction_event
            })
            .collect::<Vec<_>>()
    })
}

pub async fn create_state_update_v2<T>(
    conn: &T,
    tx: Signature,
    slot: u64,
    transaction_event: Vec<BatchPublicTransactionEvent>,
) -> Result<StateUpdate, IngesterError>
where
    T: sea_orm::ConnectionTrait + sea_orm::TransactionTrait,
{
    if transaction_event.is_empty() {
        return Ok(StateUpdate::new());
    }
    let mut state_updates = Vec::new();
    for event in transaction_event.iter() {
        let mut state_update_event =
            create_state_update_v1(conn, tx, slot, event.clone().event).await?;

        state_update_event
            .batch_nullify_context
            .extend(event.batch_input_accounts.clone());

        // Create account_transactions for v2 batch input accounts
        // but only for accounts that are not being created in this same transaction
        let output_account_hashes: std::collections::HashSet<_> = state_update_event
            .out_accounts
            .iter()
            .map(|acc| acc.account.hash.clone())
            .collect();

        state_update_event.account_transactions.extend(
            event
                .batch_input_accounts
                .iter()
                .filter(|batch_account| {
                    !output_account_hashes.contains(&Hash::from(batch_account.account_hash))
                })
                .map(|batch_account| AccountTransaction {
                    hash: batch_account.account_hash.into(),
                    signature: tx,
                }),
        );

        state_update_event.batch_new_addresses.extend(
            event
                .new_addresses
                .clone()
                .iter()
                .filter(|x| x.queue_index != u64::MAX) // Exclude AddressV1 trees
                .map(|x| AddressQueueUpdate {
                    tree: SerializablePubkey::from(x.mt_pubkey),
                    address: x.address,
                    queue_index: x.queue_index,
                }),
        );

        state_updates.push(state_update_event);
    }

    let merged = StateUpdate::merge_updates(state_updates);
    Ok(merged)
}
