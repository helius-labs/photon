use log::{debug, info};
use solana_sdk::{signature::Signature, transaction::VersionedTransaction};

use crate::{
    error::IngesterError, VersionedConfirmedTransactionWithUiStatusMeta,
    VersionedTransactionWithUiStatusMeta,
};

use self::{
    bundle::{EventBundle, PublicStateTransitionBundle},
    error::ParserError,
};

pub mod bundle;

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

    let mut not_impl = 0;
    let ixlen = meta.inner_instructions.len();
    debug!("Instructions: {}", ixlen);
    let contains = meta
        .inner_instructions
        .iter()
        .filter(|(ib, _inner)| ib.0 .0.as_ref() == mpl_bubblegum::ID.as_ref());
    debug!("Instructions bgum: {}", contains.count());
    for (outer_ix, inner_ix) in instructions {
        let (program, instruction) = outer_ix;
        let ix_accounts = instruction.accounts().unwrap().iter().collect::<Vec<_>>();
        let ix_account_len = ix_accounts.len();
        let max = ix_accounts.iter().max().copied().unwrap_or(0) as usize;
        if keys.len() < max {
            return Err(IngesterError::DeserializationError(
                "Missing Accounts in Serialized Ixn/Txn".to_string(),
            ));
        }
    }
}
