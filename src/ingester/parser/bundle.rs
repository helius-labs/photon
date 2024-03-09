use std::fmt::{Display, Formatter};

use light_merkle_tree_event::Changelogs;
use psp_compressed_pda::utxo::Utxo;
use solana_sdk::signature::Signature;

#[derive(Debug)]
pub enum EventBundle {
    PublicTransactionEvent(PublicTransactionEventBundle),
}

impl Display for EventBundle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EventBundle::PublicTransactionEvent(_) => {
                write!(f, "PublicTransactionEvent")
            }
        }
    }
}

#[derive(Debug)]
pub struct PublicTransactionEventBundle {
    pub in_utxos: Vec<Utxo>,
    pub out_utxos: Vec<Utxo>,
    pub changelogs: Changelogs,
    pub transaction: Signature,
    pub slot: u64,
}

impl From<PublicTransactionEventBundle> for EventBundle {
    fn from(bundle: PublicTransactionEventBundle) -> EventBundle {
        EventBundle::PublicTransactionEvent(bundle)
    }
}
