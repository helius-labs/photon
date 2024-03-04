use dao::generated::utxos;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::error::PhotonApiError;
use dao::typedefs::hash::Hash;
use dao::typedefs::serializable_pubkey::SerializablePubkey;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountRequest {
    pub hash: Option<Hash>,
    pub account_id: Option<SerializablePubkey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountResponse {
    pub hash: Hash,
    pub account: Option<SerializablePubkey>,
    pub data: String,
    pub owner: SerializablePubkey,
    pub lamports: Option<i64>,
    pub tree: SerializablePubkey,
    pub seq: i64,
    pub slot_updated: i64,
}

pub async fn get_compressed_account(
    conn: &DatabaseConnection,
    request: GetCompressedAccountRequest,
) -> Result<Option<GetCompressedAccountResponse>, PhotonApiError> {
    let GetCompressedAccountRequest { hash, account_id } = request;

    let filter = if let Some(h) = hash {
        Ok(utxos::Column::Hash.eq::<Vec<u8>>(h.into()))
    } else if let Some(a) = account_id {
        Ok(utxos::Column::Account.eq::<Vec<u8>>(a.into()))
    } else {
        Err(PhotonApiError::ValidationError(
            "Must provide either `hash` or `account`".to_string(),
        ))
    }?;

    let result = utxos::Entity::find().filter(filter).one(conn).await?;

    if let Some(utxo) = result {
        let res = GetCompressedAccountResponse {
            hash: utxo.hash.into(),
            account: utxo.account.map(SerializablePubkey::from),
            #[allow(deprecated)]
            data: base64::encode(utxo.data),
            owner: utxo.owner.into(),
            lamports: utxo.lamports,
            tree: utxo.tree.into(),
            seq: utxo.seq,
            slot_updated: utxo.slot_updated,
        };

        Ok(Some(res))
    } else {
        Ok(None)
    }
}
