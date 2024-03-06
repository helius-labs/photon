use std::fmt::{Display, Formatter};

use dao::typedefs::hash::Hash;
use light_merkle_tree_event::Changelogs;
use psp_compressed_pda::{event::PublicTransactionEvent, utxo::Utxo};
use solana_sdk::{pubkey::Pubkey, signature::Signature};

pub enum EventBundle {
    PublicTransactionEvent(PublicTransactionEventBundle),
    Changelogs(Changelogs),
}

impl Display for EventBundle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EventBundle::PublicTransactionEvent(_) => {
                write!(f, "PublicTransactionEvent")
            }
            EventBundle::Changelogs(_) => {
                write!(f, "Changelogs")
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Location {
    pub index: u32,
    pub tree: Pubkey,
}

#[derive(Debug, PartialEq)]
pub struct PublicTransactionEventBundle {
    pub in_utxos: Vec<Utxo>,
    pub out_utxos: Vec<Utxo>,
    pub out_uxtos_locations: Vec<Location>,
    pub transaction: Signature,
    pub slot: u64,
}

impl From<PublicTransactionEventBundle> for EventBundle {
    fn from(bundle: PublicTransactionEventBundle) -> EventBundle {
        EventBundle::PublicTransactionEvent(bundle)
    }
}

impl From<Changelogs> for EventBundle {
    fn from(changelogs: Changelogs) -> EventBundle {
        EventBundle::Changelogs(changelogs)
    }
}
