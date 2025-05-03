use light_compressed_account::TreeType;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction, Statement,
    TransactionTrait,
};
use serde::{Deserialize, Serialize};
use solana_pubkey::{pubkey, Pubkey};
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::persisted_indexed_merkle_tree::{
    format_bytes, get_exclusion_range_with_proof_v1, get_exclusion_range_with_proof_v2,
};

pub const MAX_ADDRESSES: usize = 50;
pub const ADDRESS_TREE_V1: Pubkey = pubkey!("amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2");

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct AddressWithTree {
    pub address: SerializablePubkey,
    pub tree: SerializablePubkey,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct MerkleContextWithNewAddressProof {
    pub root: Hash,
    pub address: SerializablePubkey,
    pub lowerRangeAddress: SerializablePubkey,
    pub higherRangeAddress: SerializablePubkey,
    pub nextIndex: u32,
    pub proof: Vec<Hash>,
    pub merkleTree: SerializablePubkey,
    pub rootSeq: u64,
    pub lowElementLeafIndex: u32,
}

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetMultipleNewAddressProofsResponse {
    pub context: Context,
    pub value: Vec<MerkleContextWithNewAddressProof>,
}

pub async fn get_multiple_new_address_proofs_helper(
    txn: &DatabaseTransaction,
    addresses: Vec<AddressWithTree>,
    check_queue: bool,
) -> Result<Vec<MerkleContextWithNewAddressProof>, PhotonApiError> {
    if addresses.is_empty() {
        return Err(PhotonApiError::ValidationError(
            "No addresses provided".to_string(),
        ));
    }

    if addresses.len() > MAX_ADDRESSES {
        return Err(PhotonApiError::ValidationError(
            format!(
                "Too many addresses requested {}. Maximum allowed: {}",
                addresses.len(),
                MAX_ADDRESSES
            )
            .to_string(),
        ));
    }

    let mut new_address_proofs: Vec<MerkleContextWithNewAddressProof> = Vec::new();

    for AddressWithTree { address, tree } in addresses {
        let tree_and_queue = TreeInfo::get(&tree.to_string())
            .ok_or(PhotonApiError::InvalidPubkey {
                field: tree.to_string(),
            })?
            .clone();

        // For V2 trees, check if the address is in the queue but not yet in the tree
        if check_queue && tree_and_queue.tree_type == TreeType::AddressV2 {
            // Check if the address is in the queue
            let address_queue_stmt = Statement::from_string(
                txn.get_database_backend(),
                format!(
                    "SELECT COUNT(*) as count FROM address_queues
                     WHERE tree = {} AND address = {}",
                    format_bytes(tree.to_bytes_vec(), txn.get_database_backend()),
                    format_bytes(address.to_bytes_vec(), txn.get_database_backend())
                ),
            );

            let queue_result = txn.query_one(address_queue_stmt).await.map_err(|e| {
                PhotonApiError::UnexpectedError(format!("Failed to query address queue: {}", e))
            })?;

            let in_queue = match queue_result {
                Some(row) => {
                    let count: i64 = row.try_get("", "count").map_err(|e| {
                        PhotonApiError::UnexpectedError(format!("Failed to get count: {}", e))
                    })?;
                    count > 0
                }
                None => false,
            };

            if in_queue {
                return Err(PhotonApiError::ValidationError(format!(
                    "Address {} is in the queue for tree {} but not yet in the tree",
                    address.to_string(),
                    tree.to_string()
                )));
            }
        }

        let (model, proof) = match tree_and_queue.tree_type {
            TreeType::AddressV1 => {
                let address = address.to_bytes_vec();
                let tree = tree.to_bytes_vec();
                get_exclusion_range_with_proof_v1(txn, tree, tree_and_queue.height + 1, address)
                    .await?
            }
            TreeType::AddressV2 => {
                get_exclusion_range_with_proof_v2(
                    txn,
                    tree.to_bytes_vec(),
                    tree_and_queue.height + 1,
                    address.to_bytes_vec(),
                )
                .await?
            }
            _ => {
                return Err(PhotonApiError::UnexpectedError(
                    "Invalid tree type".to_string(),
                ));
            }
        };

        let new_address_proof = MerkleContextWithNewAddressProof {
            root: proof.root,
            address,
            lowerRangeAddress: SerializablePubkey::try_from(model.value)?,
            higherRangeAddress: SerializablePubkey::try_from(model.next_value)?,
            nextIndex: model.next_index as u32,
            proof: proof.proof,
            lowElementLeafIndex: model.leaf_index as u32,
            merkleTree: tree,
            rootSeq: proof.root_seq,
        };
        new_address_proofs.push(new_address_proof);
    }
    Ok(new_address_proofs)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AddressList(pub Vec<SerializablePubkey>);

pub async fn get_multiple_new_address_proofs(
    conn: &DatabaseConnection,
    addresses: AddressList,
) -> Result<GetMultipleNewAddressProofsResponse, PhotonApiError> {
    let addresses_with_trees = AddressListWithTrees(
        addresses
            .0
            .into_iter()
            .map(|address| AddressWithTree {
                address,
                tree: SerializablePubkey::from(ADDRESS_TREE_V1),
            })
            .collect(),
    );

    get_multiple_new_address_proofs_v2(conn, addresses_with_trees).await
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AddressListWithTrees(pub Vec<AddressWithTree>);

// V2 is the same as V1, but it takes a list of AddressWithTree instead of AddressList.
pub async fn get_multiple_new_address_proofs_v2(
    conn: &DatabaseConnection,
    addresses_with_trees: AddressListWithTrees,
) -> Result<GetMultipleNewAddressProofsResponse, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let tx = conn.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }

    let new_address_proofs =
        get_multiple_new_address_proofs_helper(&tx, addresses_with_trees.0, true).await?;
    tx.commit().await?;

    Ok(GetMultipleNewAddressProofsResponse {
        value: new_address_proofs,
        context,
    })
}
