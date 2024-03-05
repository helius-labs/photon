use std::fmt::{Display, Formatter};

use dao::typedefs::hash::Hash;
use light_merkle_tree_event::Changelogs;
use psp_compressed_pda::event::PublicTransactionEvent;
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountState {
    Uninitialized,
    Initialized,
    Frozen,
}

// Copied from the light code. Can't import it right now because rely on two branches of the Light code.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TokenTlvData {
    /// The mint associated with this account
    pub mint: Pubkey,
    /// The owner of this account.
    pub owner: Pubkey,
    /// The amount of tokens this account holds.
    pub amount: u64,
    /// If `delegate` is `Some` then `delegated_amount` represents
    /// the amount authorized by the delegate
    pub delegate: Option<Pubkey>,
    /// The account's state
    pub state: AccountState,
    /// If is_some, this is a native token, and the value logs the rent-exempt
    /// reserve. An Account is required to be rent-exempt, so the value is
    /// used by the Processor to ensure that wrapped SOL accounts do not
    /// drop below this threshold.
    pub is_native: Option<u64>,
    /// The amount delegated
    pub delegated_amount: u64,
    /// Optional authority to close the account.
    pub close_authority: Option<Pubkey>,
}
