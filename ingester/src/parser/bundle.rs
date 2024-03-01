use std::fmt::{Display, Formatter};

use account_compression::Changelogs;
use light_verifier_sdk::public_transaction::PublicTransactionEvent;
use solana_sdk::{pubkey::Pubkey, signature::Signature};

pub enum EventBundle {
    PublicStateTransition(PublicStateTransitionBundle),
    PublicNullifier(PublicNullifierBundle),
    LegacyPublicStateTransaction(PublicTransactionEvent),
    LegacyChangeLogEvent(Changelogs),
}

impl Display for EventBundle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EventBundle::PublicStateTransition(_) => {
                write!(f, "PublicStateTransition")
            }
            EventBundle::PublicNullifier(_) => {
                write!(f, "PublicNullifier")
            }
            EventBundle::LegacyPublicStateTransaction(_) => {
                write!(f, "LegacyPublicStateTransaction")
            }
            EventBundle::LegacyChangeLogEvent(_) => {
                write!(f, "LegacyChangeLogEvent")
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PublicStateTransitionBundle {
    pub in_utxos: Vec<UTXOEvent>,
    pub out_utxos: Vec<UTXOEvent>,
    pub changelogs: Vec<ChangelogEvent>,
    pub transaction: Signature,
    pub slot_updated: i64,
}

impl Into<EventBundle> for PublicStateTransitionBundle {
    fn into(self) -> EventBundle {
        EventBundle::PublicStateTransition(self)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct PublicNullifierBundle {
    pub nullifier_hashes: Vec<Hash>,
    pub changelogs: Vec<ChangelogEvent>,
    pub transaction: Signature,
    pub slot_updated: i64,
}

impl Into<EventBundle> for PublicNullifierBundle {
    fn into(self) -> EventBundle {
        EventBundle::PublicNullifier(self)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ChangelogEvent {
    pub tree: Pubkey,
    pub seq: i32,
    pub path: Vec<PathNode>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PathNode {
    pub index: i64,
    pub hash: Hash,
}

#[derive(Debug, Eq, PartialEq)]
pub struct UTXOEvent {
    pub hash: Hash,
    pub data: Vec<u8>,
    pub owner: Pubkey,
    pub blinding: Hash,
    pub account: Option<Pubkey>,
    pub lamports: Option<i64>,
    pub tree: Pubkey,
    pub seq: i64,
}

#[derive(Eq, PartialEq, Clone, Copy)]
pub struct Hash([u8; 32]);

impl Hash {
    pub fn new(hash: [u8; 32]) -> Self {
        Hash(hash)
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash({})", bs58::encode(self.0).into_string())
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", bs58::encode(self.0).into_string())
    }
}

impl Default for Hash {
    fn default() -> Self {
        Hash([0; 32])
    }
}
