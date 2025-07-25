use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::error::IngesterError;
use crate::ingester::parser::indexer_events::{
    BatchPublicTransactionEvent, CompressedAccount, CompressedAccountData,
    MerkleTreeSequenceNumberV1, MerkleTreeSequenceNumberV2,
    OutputCompressedAccountWithPackedContext, PublicTransactionEvent,
};
use crate::ingester::parser::state_update::StateUpdate;
use crate::ingester::parser::tx_event_parser::create_state_update_v1;

use light_compressed_account::indexer_event::parse::event_from_light_transaction;
use light_compressed_account::Pubkey as LightPubkey;
use solana_pubkey::Pubkey;
use solana_sdk::signature::Signature;

use super::state_update::AddressQueueUpdate;

pub fn parse_public_transaction_event_v2(
    program_ids: &[Pubkey],
    instructions: &[Vec<u8>],
    accounts: Vec<Vec<Pubkey>>,
) -> Option<Vec<BatchPublicTransactionEvent>> {
    let light_program_ids: Vec<LightPubkey> = program_ids.iter().map(|p| (*p).into()).collect();
    let light_accounts: Vec<Vec<LightPubkey>> = accounts
        .into_iter()
        .map(|acc_vec| acc_vec.into_iter().map(|acc| acc.into()).collect())
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
                                owner: x.compressed_account.owner.into(),
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
                            pubkey: x.tree_pubkey.into(),
                            seq: x.seq,
                        })
                        .collect(),
                    relay_fee: public_transaction_event.event.relay_fee,
                    is_compress: public_transaction_event.event.is_compress,
                    compression_lamports: public_transaction_event
                        .event
                        .compress_or_decompress_lamports,
                    pubkey_array: public_transaction_event
                        .event
                        .pubkey_array
                        .into_iter()
                        .map(|p| p.into())
                        .collect(),
                    message: public_transaction_event.event.message,
                };

                let batch_public_transaction_event = BatchPublicTransactionEvent {
                    event,
                    new_addresses: public_transaction_event.new_addresses,
                    input_sequence_numbers: public_transaction_event
                        .input_sequence_numbers
                        .iter()
                        .map(|x| MerkleTreeSequenceNumberV2 {
                            tree_pubkey: x.tree_pubkey.into(),
                            queue_pubkey: x.queue_pubkey.into(),
                            tree_type: x.tree_type,
                            seq: x.seq,
                        })
                        .collect(),
                    address_sequence_numbers: public_transaction_event
                        .address_sequence_numbers
                        .iter()
                        .map(|x| MerkleTreeSequenceNumberV2 {
                            tree_pubkey: x.tree_pubkey.into(),
                            queue_pubkey: x.queue_pubkey.into(),
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

pub fn create_state_update_v2(
    tx: Signature,
    slot: u64,
    transaction_event: Vec<BatchPublicTransactionEvent>,
) -> Result<StateUpdate, IngesterError> {
    if transaction_event.is_empty() {
        return Ok(StateUpdate::new());
    }
    let mut state_updates = Vec::new();
    for event in transaction_event.iter() {
        let mut state_update_event = create_state_update_v1(tx, slot, event.clone().event.into())?;

        state_update_event
            .batch_nullify_context
            .extend(event.batch_input_accounts.clone());

        state_update_event.batch_new_addresses.extend(
            event
                .new_addresses
                .clone()
                .iter()
                .filter(|x| x.queue_index != u64::MAX) // Exclude AddressV1 trees
                .map(|x| AddressQueueUpdate {
                    tree: SerializablePubkey::from(x.tree_pubkey),
                    address: x.address,
                    queue_index: x.queue_index,
                }),
        );

        state_updates.push(state_update_event);
    }

    let merged = StateUpdate::merge_updates(state_updates);
    Ok(merged)
}
