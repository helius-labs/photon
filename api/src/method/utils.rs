use dao::generated::utxos;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use dao::typedefs::hash::Hash;
use dao::typedefs::serializable_pubkey::SerializablePubkey;

use crate::error::PhotonApiError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Utxo {
    pub hash: Hash,
    pub account: Option<SerializablePubkey>,
    pub owner: SerializablePubkey,
    pub data: String,
    pub tree: Option<SerializablePubkey>,
    pub lamports: u64,
    pub slot_created: u64,
}

pub fn parse_utxo_model(utxo: utxos::Model) -> Result<Utxo, PhotonApiError> {
    Ok(Utxo {
        hash: utxo.hash.try_into()?,
        account: utxo.account.map(SerializablePubkey::try_from).transpose()?,
        #[allow(deprecated)]
        data: base64::encode(utxo.data),
        owner: utxo.owner.try_into()?,
        tree: utxo.tree.map(|tree| tree.try_into()).transpose()?,
        lamports: utxo.lamports as u64,
        slot_created: utxo.slot_updated as u64,
    })
}
