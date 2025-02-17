use serde::Serialize;

use utoipa::ToSchema;

use super::{
    bs64_string::Base64String, hash::Hash, serializable_pubkey::SerializablePubkey,
    unsigned_integer::UnsignedInteger,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountV1 {
    pub hash: Hash,
    pub address: Option<SerializablePubkey>,
    pub data: Option<AccountData>,
    pub owner: SerializablePubkey,
    pub lamports: UnsignedInteger,
    pub tree: SerializablePubkey,
    pub leaf_index: UnsignedInteger,
    pub seq: Option<UnsignedInteger>,
    pub slot_created: UnsignedInteger,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountV2 {
    pub hash: Hash,
    pub address: Option<SerializablePubkey>,
    pub data: Option<AccountData>,
    pub owner: SerializablePubkey,
    pub lamports: UnsignedInteger,
    pub tree: SerializablePubkey,
    pub queue: Option<SerializablePubkey>,
    pub in_queue: bool,
    pub spent: bool,
    pub spent_in_queue: bool,
    pub nullifier_queue_index: Option<UnsignedInteger>,
    pub nullifier: Option<Hash>,
    pub tx_hash: Option<Hash>,
    pub leaf_index: UnsignedInteger,
    pub seq: Option<UnsignedInteger>,
    pub slot_created: UnsignedInteger,
}

// output account:
// 1. in_queue: true, spent: false
// 2. in_queue: false, spent: false
// 3. in_queue: true, spent: true
// 4. in_queue: false, spent: true

// 1 => 3 => 4
// 1 => 2 => 3 => 4

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountData {
    pub discriminator: UnsignedInteger,
    pub data: Base64String,
    pub data_hash: Hash,
}
