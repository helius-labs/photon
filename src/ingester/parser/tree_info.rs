use crate::api::error::PhotonApiError;
use crate::dao::generated::{prelude::*, tree_metadata};
use light_compressed_account::TreeType;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionTrait};
use solana_pubkey::Pubkey;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct TreeInfo {
    pub tree: Pubkey,
    pub queue: Pubkey,
    pub height: u32,
    pub tree_type: TreeType,
    pub root_history_capacity: u64,
}

impl TreeInfo {
    pub async fn get<T>(conn: &T, pubkey: &str) -> Result<Option<TreeInfo>, PhotonApiError>
    where
        T: ConnectionTrait + TransactionTrait,
    {
        let pubkey_parsed = Pubkey::from_str(pubkey)
            .map_err(|e| PhotonApiError::UnexpectedError(format!("Invalid pubkey: {}", e)))?;

        Self::get_by_pubkey(conn, &pubkey_parsed).await
    }

    pub async fn get_by_pubkey<T>(
        conn: &T,
        pubkey: &Pubkey,
    ) -> Result<Option<TreeInfo>, PhotonApiError>
    where
        T: ConnectionTrait + TransactionTrait,
    {
        let tree_bytes = pubkey.to_bytes().to_vec();

        let metadata = TreeMetadata::find()
            .filter(tree_metadata::Column::TreePubkey.eq(tree_bytes.clone()))
            .one(conn)
            .await
            .map_err(|e| PhotonApiError::UnexpectedError(format!("Database error: {}", e)))?;

        if let Some(metadata) = metadata {
            return Ok(Some(TreeInfo::from_metadata(metadata, *pubkey)?));
        }

        let metadata = TreeMetadata::find()
            .filter(tree_metadata::Column::QueuePubkey.eq(tree_bytes.clone()))
            .one(conn)
            .await
            .map_err(|e| PhotonApiError::UnexpectedError(format!("Database error: {}", e)))?;

        if let Some(metadata) = metadata {
            let tree_bytes: [u8; 32] =
                metadata.tree_pubkey.as_slice().try_into().map_err(|_| {
                    PhotonApiError::UnexpectedError("Invalid tree pubkey length in DB".to_string())
                })?;
            let tree_pubkey = Pubkey::from(tree_bytes);
            return Ok(Some(TreeInfo::from_metadata(metadata, tree_pubkey)?));
        }

        Ok(None)
    }

    pub async fn height<T>(conn: &T, pubkey: &str) -> Result<Option<u32>, PhotonApiError>
    where
        T: ConnectionTrait + TransactionTrait,
    {
        let info = Self::get(conn, pubkey).await?;
        Ok(info.map(|x| x.height))
    }

    pub async fn get_tree_type<T>(conn: &T, pubkey: &Pubkey) -> Result<TreeType, PhotonApiError>
    where
        T: ConnectionTrait + TransactionTrait,
    {
        let tree_pubkey_str = pubkey.to_string();
        let info = Self::get(conn, &tree_pubkey_str).await?;
        Ok(info.map(|i| i.tree_type).unwrap_or(TreeType::AddressV2))
    }

    pub async fn get_tree_type_from_pubkey<T>(
        conn: &T,
        pubkey: &[u8; 32],
    ) -> Result<TreeType, PhotonApiError>
    where
        T: ConnectionTrait + TransactionTrait,
    {
        let pubkey = Pubkey::from(*pubkey);
        Self::get_tree_type(conn, &pubkey).await
    }

    pub async fn get_tree_types_batch<T>(
        conn: &T,
        pubkeys: &[Pubkey],
    ) -> Result<std::collections::HashMap<Pubkey, TreeType>, PhotonApiError>
    where
        T: ConnectionTrait + TransactionTrait,
    {
        let tree_bytes_vec: Vec<Vec<u8>> = pubkeys.iter().map(|p| p.to_bytes().to_vec()).collect();

        let metadata_list = TreeMetadata::find()
            .filter(tree_metadata::Column::TreePubkey.is_in(tree_bytes_vec))
            .all(conn)
            .await
            .map_err(|e| PhotonApiError::UnexpectedError(format!("Database error: {}", e)))?;

        let mut result = std::collections::HashMap::new();

        for metadata in metadata_list {
            let tree_bytes: [u8; 32] =
                metadata.tree_pubkey.as_slice().try_into().map_err(|_| {
                    PhotonApiError::UnexpectedError("Invalid tree pubkey length in DB".to_string())
                })?;
            let tree_pubkey = Pubkey::from(tree_bytes);

            let tree_type = match metadata.tree_type {
                1 => TreeType::StateV1,
                2 => TreeType::AddressV1,
                3 => TreeType::StateV2,
                4 => TreeType::AddressV2,
                _ => TreeType::AddressV2,
            };

            result.insert(tree_pubkey, tree_type);
        }

        for pubkey in pubkeys {
            result.entry(*pubkey).or_insert(TreeType::AddressV2);
        }

        Ok(result)
    }

    pub async fn get_by_sdk_pubkey<T>(
        conn: &T,
        pubkey: &solana_sdk::pubkey::Pubkey,
    ) -> Result<Option<TreeInfo>, crate::ingester::error::IngesterError>
    where
        T: ConnectionTrait + TransactionTrait,
    {
        let pubkey_bytes = pubkey.to_bytes();
        let pubkey_converted = Pubkey::from(pubkey_bytes);

        Self::get_by_pubkey(conn, &pubkey_converted)
            .await
            .map_err(|e| {
                crate::ingester::error::IngesterError::ParserError(format!(
                    "Failed to get tree info: {}",
                    e
                ))
            })
    }

    fn from_metadata(
        metadata: tree_metadata::Model,
        tree_pubkey: Pubkey,
    ) -> Result<TreeInfo, PhotonApiError> {
        let queue_bytes: [u8; 32] = metadata.queue_pubkey.as_slice().try_into().map_err(|_| {
            PhotonApiError::UnexpectedError("Invalid queue pubkey length in DB".to_string())
        })?;
        let queue_pubkey = Pubkey::from(queue_bytes);

        let tree_type = match metadata.tree_type {
            1 => TreeType::StateV1,
            2 => TreeType::AddressV1,
            3 => TreeType::StateV2,
            4 => TreeType::AddressV2,
            _ => {
                return Err(PhotonApiError::UnexpectedError(format!(
                    "Unknown tree type: {}",
                    metadata.tree_type
                )))
            }
        };

        Ok(TreeInfo {
            tree: tree_pubkey,
            queue: queue_pubkey,
            height: metadata.height as u32,
            tree_type,
            root_history_capacity: metadata.root_history_capacity as u64,
        })
    }
}
