use light_compressed_account::TreeType;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, Statement, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use solana_pubkey::{pubkey, Pubkey};
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::common::format_bytes;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::indexed_merkle_tree::get_multiple_exclusion_ranges_with_proofs_v2;
use std::collections::HashMap;

pub const MAX_ADDRESSES: usize = 30_000;

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
    max_addresses: usize,
    check_queue: bool,
) -> Result<Vec<MerkleContextWithNewAddressProof>, PhotonApiError> {
    if addresses.is_empty() {
        return Err(PhotonApiError::ValidationError(
            "No addresses provided".to_string(),
        ));
    }

    if addresses.len() > max_addresses {
        return Err(PhotonApiError::ValidationError(
            format!(
                "Too many addresses requested {}. Maximum allowed: {}",
                addresses.len(),
                max_addresses
            )
            .to_string(),
        ));
    }

    // Group addresses by tree for batching
    let mut addresses_by_tree: HashMap<SerializablePubkey, Vec<(usize, SerializablePubkey)>> =
        HashMap::new();
    for (idx, AddressWithTree { address, tree }) in addresses.iter().enumerate() {
        addresses_by_tree
            .entry(*tree)
            .or_insert_with(Vec::new)
            .push((idx, *address));
    }

    let mut indexed_proofs: Vec<(usize, MerkleContextWithNewAddressProof)> = Vec::new();

    for (tree, tree_addresses) in addresses_by_tree {
        let tree_and_queue =
            TreeInfo::get(txn, &tree.to_string())
                .await?
                .ok_or(PhotonApiError::InvalidPubkey {
                    field: tree.to_string(),
                })?;

        // For V2 trees, check if addresses are in the queue but not yet in the tree
        if check_queue && tree_and_queue.tree_type == TreeType::AddressV2 {
            let address_list = tree_addresses
                .iter()
                .map(|(_, addr)| format_bytes(addr.to_bytes_vec(), txn.get_database_backend()))
                .collect::<Vec<_>>()
                .join(", ");

            let address_queue_stmt = Statement::from_string(
                txn.get_database_backend(),
                format!(
                    "SELECT address FROM address_queues
                     WHERE tree = {} AND address IN ({})",
                    format_bytes(tree.to_bytes_vec(), txn.get_database_backend()),
                    address_list
                ),
            );

            let queue_results = txn.query_all(address_queue_stmt).await.map_err(|e| {
                PhotonApiError::UnexpectedError(format!("Failed to query address queue: {}", e))
            })?;

            if !queue_results.is_empty() {
                let queued_address: Vec<u8> =
                    queue_results[0].try_get("", "address").map_err(|e| {
                        PhotonApiError::UnexpectedError(format!("Failed to get address: {}", e))
                    })?;
                let queued_address = SerializablePubkey::try_from(queued_address)?;
                return Err(PhotonApiError::ValidationError(format!(
                    "Address {} already exists",
                    queued_address
                )));
            }
        }

        let address_values: Vec<Vec<u8>> = tree_addresses
            .iter()
            .map(|(_, addr)| addr.to_bytes_vec())
            .collect();

        let tree_bytes = tree.to_bytes_vec();
        let tree_type = tree_and_queue.tree_type;

        let results = get_multiple_exclusion_ranges_with_proofs_v2(
            txn,
            tree_bytes.clone(),
            tree_and_queue.height + 1,
            address_values.clone(),
            tree_type,
        )
        .await?;

        for (original_idx, address) in tree_addresses {
            let address_bytes = address.to_bytes_vec();

            let (model, proof) = results.get(&address_bytes).ok_or_else(|| {
                PhotonApiError::RecordNotFound(format!("No proof found for address {}", address))
            })?;

            let new_address_proof = MerkleContextWithNewAddressProof {
                root: proof.root.clone(),
                address,
                lowerRangeAddress: SerializablePubkey::try_from(model.value.clone())?,
                higherRangeAddress: SerializablePubkey::try_from(model.next_value.clone())?,
                nextIndex: model.next_index as u32,
                proof: proof.proof.clone(),
                lowElementLeafIndex: model.leaf_index as u32,
                merkleTree: tree,
                rootSeq: proof.root_seq,
            };

            indexed_proofs.push((original_idx, new_address_proof));
        }
    }

    indexed_proofs.sort_by_key(|(idx, _)| *idx);
    let new_address_proofs = indexed_proofs.into_iter().map(|(_, proof)| proof).collect();

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
    crate::api::set_transaction_isolation_if_needed(&tx).await?;

    let new_address_proofs =
        get_multiple_new_address_proofs_helper(&tx, addresses_with_trees.0, MAX_ADDRESSES, true)
            .await?;
    tx.commit().await?;

    Ok(GetMultipleNewAddressProofsResponse {
        value: new_address_proofs,
        context,
    })
}
