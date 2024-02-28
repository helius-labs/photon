use log::{debug, info};
use solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::VersionedTransaction};

use crate::{
    error::IngesterError, VersionedConfirmedTransactionWithUiStatusMeta,
    VersionedTransactionWithUiStatusMeta,
};

use self::bundle::{EventBundle, PublicStateTransitionBundle};

pub mod bundle;

const MERKLE_TREE_PROGRAM_ID: Pubkey = pubkey!("MERKLE_TREE_PROGRAM_ID");

pub fn parse_transaction(
    tx: VersionedConfirmedTransactionWithUiStatusMeta,
) -> Result<EventBundle, IngesterError> {
    let VersionedConfirmedTransactionWithUiStatusMeta {
        slot,
        block_time,
        tx_with_meta,
    } = tx;

    let VersionedTransactionWithUiStatusMeta { transaction, meta } = tx_with_meta;

    // In Solana, the first Signature is the transaction id
    let tx_id: Signature = transaction.signatures[0];

    let msg = transaction.message;

    let outer_instructions = msg.instructions();
    let inner_instructions = meta.inner_instructions;

    let atl_keys = msg.address_table_lookups();
    let account_keys = msg.static_account_keys();
    panic!("Not implemented");
}
