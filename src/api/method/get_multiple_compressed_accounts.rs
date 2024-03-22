use crate::dao::generated::utxos;
use schemars::JsonSchema;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use serde::{Deserialize, Serialize};

use super::{
    super::error::PhotonApiError,
    utils::{Context, ResponseWithContext},
};
use crate::dao::typedefs::hash::Hash;
use crate::dao::typedefs::serializable_pubkey::SerializablePubkey;

use super::utils::{parse_utxo_model, Utxo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetMultipleCompressedAccountsRequest {
    pub hashes: Option<Vec<Hash>>,
    pub addresses: Option<Vec<SerializablePubkey>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UtxoList {
    pub items: Vec<Utxo>,
}

pub type GetMultipleCompressedAccountsResponse = ResponseWithContext<UtxoList>;

async fn fetch_accounts_from_hashes(
    conn: &DatabaseConnection,
    hashes: Vec<Hash>,
) -> Result<Vec<utxos::Model>, PhotonApiError> {
    let raw_hashes: Vec<Vec<u8>> = hashes.into_iter().map(|hash| hash.to_vec()).collect();

    let accounts = utxos::Entity::find()
        .filter(utxos::Column::Hash.is_in(raw_hashes.clone()))
        .all(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

    let found_hashes: Vec<Vec<u8>> = accounts
        .iter()
        .map(|account| account.hash.clone())
        .collect();

    let not_found_hashes: Vec<Vec<u8>> = raw_hashes
        .into_iter()
        .filter(|hash| !found_hashes.contains(hash))
        .collect();

    let mut formatted_not_found_hashes: Vec<Hash> = Vec::new();
    for hash in not_found_hashes {
        formatted_not_found_hashes.push(
            Hash::try_from(hash)
                .map_err(|e| PhotonApiError::UnexpectedError(format!("Invalid hash: {}", e)))?,
        );
    }

    if !formatted_not_found_hashes.is_empty() {
        return Err(PhotonApiError::RecordNotFound(format!(
            "Some accounts were not found: {:?}",
            formatted_not_found_hashes
        )));
    }

    Ok(accounts)
}

async fn fetch_account_from_addresses(
    conn: &DatabaseConnection,
    addresses: Vec<SerializablePubkey>,
) -> Result<Vec<utxos::Model>, PhotonApiError> {
    let raw_addresses: Vec<Vec<u8>> = addresses.into_iter().map(|addr| addr.into()).collect();

    let accounts = utxos::Entity::find()
        .filter(utxos::Column::Account.is_in(raw_addresses.clone()))
        .all(conn)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("DB error: {}", e)))?;

    let found_addresses: Vec<Vec<u8>> = accounts
        .iter()
        .filter_map(|account| account.account.clone())
        .collect();

    let not_found_addresses: Vec<Vec<u8>> = raw_addresses
        .into_iter()
        .filter(|addr| !found_addresses.contains(addr))
        .collect();

    let mut formatted_not_found_addresses: Vec<SerializablePubkey> = Vec::new();
    for addr in not_found_addresses {
        formatted_not_found_addresses.push(
            SerializablePubkey::try_from(addr)
                .map_err(|e| PhotonApiError::UnexpectedError(format!("Invalid address: {}", e)))?,
        );
    }

    if !formatted_not_found_addresses.is_empty() {
        return Err(PhotonApiError::RecordNotFound(format!(
            "Some accounts were not found: {:?}",
            formatted_not_found_addresses
        )));
    }

    Ok(accounts)
}

pub async fn get_multiple_compressed_accounts(
    conn: &DatabaseConnection,
    request: GetMultipleCompressedAccountsRequest,
) -> Result<GetMultipleCompressedAccountsResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;

    let accounts = match (request.hashes, request.addresses) {
        (Some(hashes), None) => fetch_accounts_from_hashes(conn, hashes).await?,
        (None, Some(addresses)) => fetch_account_from_addresses(conn, addresses).await?,
        _ => panic!("Either hashes or addresses must be provided"),
    };

    Ok(GetMultipleCompressedAccountsResponse {
        context,
        value: UtxoList {
            items: accounts
                .into_iter()
                .map(parse_utxo_model)
                .collect::<Result<Vec<Utxo>, PhotonApiError>>()?,
        },
    })
}
