use sea_orm::Set;
use serde::{Deserialize, Serialize};
use sqlx::types::Decimal;
use utoipa::ToSchema;

use crate::dao::generated::accounts;

use super::{bs64_string::Base64String, hash::Hash, serializable_pubkey::SerializablePubkey};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Account {
    pub hash: Hash,
    pub address: Option<SerializablePubkey>,
    pub data: Option<AccountData>,
    pub owner: SerializablePubkey,
    pub lamports: u64,
    pub tree: SerializablePubkey,
    pub leaf_index: u32,
    pub seq: Option<u64>,
    pub slot_updated: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountData {
    pub discriminator: u64,
    pub data: Base64String,
    pub data_hash: Hash,
}

