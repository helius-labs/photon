use super::{
    bs64_string::Base64String, hash::Hash, serializable_pubkey::SerializablePubkey,
    unsigned_integer::UnsignedInteger,
};
use crate::ingester::parser::indexer_events::CompressedAccount;
use byteorder::{ByteOrder, LittleEndian};
use serde::Serialize;
use solana_program::pubkey::Pubkey;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Account {
    pub hash: Hash,
    pub address: Option<SerializablePubkey>,
    pub data: Option<AccountData>,
    pub owner: SerializablePubkey,
    pub lamports: UnsignedInteger,
    pub tree: SerializablePubkey,
    pub leaf_index: UnsignedInteger,
    // For legacy trees is always Some() since the user tx appends directly to the Merkle tree
    // for batched tress:
    // 2.1. None when is in output queue
    // 2.2. Some once it was inserted into the Merkle tree from the output queue
    pub seq: Option<UnsignedInteger>,
    pub slot_created: UnsignedInteger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountContext {
    pub queue: Option<SerializablePubkey>,
    pub in_output_queue: bool,
    pub spent: bool,
    pub nullified_in_tree: bool,
    // if nullifier_queue_index is not None, then this account is in input queue
    // an account can be in the input and output queue at the same time.
    // an account that is in the input queue must have been in the output queue before or currently is in the output queue
    pub nullifier_queue_index: Option<UnsignedInteger>,
    // Legacy trees: None
    // Batched trees:
    // None if not inserted into input queue or inserted into merkle tree from input queue
    // Some(H(account_hash, leaf_index, tx_hash))
    pub nullifier: Option<Hash>,
    // tx_hash is:
    // Legacy: None
    // Batched: None if not inserted into input queue or inserted in tree from input queue, else Some(nullifier)
    pub tx_hash: Option<Hash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountWithContext {
    pub account: Account,
    pub context: AccountContext,
}

impl AccountWithContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        compressed_account: CompressedAccount,
        hash: [u8; 32],
        tree: Pubkey,
        queue: Option<Pubkey>,
        leaf_index: u32,
        slot: u64,
        seq: Option<u64>,
        in_output_queue: bool,
        spent: bool,
        nullifier: Option<Hash>,
        nullifier_queue_index: Option<u64>,
    ) -> Self {
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

        Self {
            account: Account {
                owner: owner.into(),
                lamports: UnsignedInteger(lamports),
                address: address.map(SerializablePubkey::from),
                data,
                hash: hash.into(),
                slot_created: UnsignedInteger(slot),
                leaf_index: UnsignedInteger(leaf_index as u64),
                tree: SerializablePubkey::from(tree),
                seq: seq.map(UnsignedInteger),
            },
            context: AccountContext {
                queue: queue.map(SerializablePubkey::from),
                in_output_queue,
                spent,
                nullified_in_tree: false,
                nullifier_queue_index: nullifier_queue_index.map(UnsignedInteger),
                nullifier,
                tx_hash: None,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountData {
    pub discriminator: UnsignedInteger,
    pub data: Base64String,
    pub data_hash: Hash,
}
