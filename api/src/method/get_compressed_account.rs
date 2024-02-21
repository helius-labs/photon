use dao::generated::utxos;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use crate::error::PhotonApiError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountRequest {
    pub hash: Option<String>,
    pub account_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetCompressedAccountResponse {
    pub hash: String,
    pub account: Option<String>,
    pub data: String,
    pub owner: String,
    pub lamports: Option<i64>,
    pub tree: String,
    pub seq: i64,
    pub slot_updated: i64,
}

pub async fn get_compressed_account(
    conn: &DatabaseConnection,
    request: GetCompressedAccountRequest,
) -> Result<Option<GetCompressedAccountResponse>, PhotonApiError> {
    let GetCompressedAccountRequest { hash, account_id } = request;

    let filter = if let Some(h) = hash {
        let hash_vec = bs58::decode(h)
            .into_vec()
            .map_err(|_| PhotonApiError::InvalidPubkey {
                field: "hash".to_string(),
            })?;
        Ok(utxos::Column::Hash.eq(hash_vec))
    } else if let Some(a) = account_id {
        let acc_vec = bs58::decode(a)
            .into_vec()
            .map_err(|_| PhotonApiError::InvalidPubkey {
                field: "account_id".to_string(),
            })?;
        Ok(utxos::Column::Account.eq(acc_vec))
    } else {
        Err(PhotonApiError::ValidationError(
            "Must provide either `hash` or `account`".to_string(),
        ))
    }?;

    let result = utxos::Entity::find().filter(filter).one(conn).await?;
    if let Some(utxo) = result {
        let res = GetCompressedAccountResponse {
            hash: bs58::encode(utxo.hash).into_string(),
            account: utxo.account.map(|a| bs58::encode(a).into_string()),
            data: base64::encode(utxo.data),
            owner: bs58::encode(utxo.owner).into_string(),
            lamports: utxo.lamports,
            tree: bs58::encode(utxo.tree).into_string(),
            seq: utxo.seq,
            slot_updated: utxo.slot_updated,
        };
        Ok(Some(res))
    } else {
        Ok(None)
    }
}
