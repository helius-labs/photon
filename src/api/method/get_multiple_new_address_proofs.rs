use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction, Statement,
    TransactionTrait,
};
use serde::{Deserialize, Serialize};
use solana_program::pubkey;
use solana_sdk::pubkey::Pubkey;
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::persist::persisted_indexed_merkle_tree::get_exclusion_range_with_proof;

pub const ADDRESS_TREE_HEIGHT: u32 = 27;
pub const ADDRESS_TREE_ADDRESS: Pubkey = pubkey!("amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2");
pub const MAX_ADDRESSES: usize = 50;

use super::utils::Context;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
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
    addresses: Vec<SerializablePubkey>,
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

    for address in addresses {
        let (model, proof) = get_exclusion_range_with_proof(
            txn,
            ADDRESS_TREE_ADDRESS.to_bytes().to_vec(),
            ADDRESS_TREE_HEIGHT,
            address.to_bytes_vec(),
        )
        .await?;
        let new_address_proof = MerkleContextWithNewAddressProof {
            root: proof.root,
            address,
            lowerRangeAddress: SerializablePubkey::try_from(model.value)?,
            higherRangeAddress: SerializablePubkey::try_from(model.next_value)?,
            nextIndex: model.next_index as u32,
            proof: proof.proof,
            lowElementLeafIndex: model.leaf_index as u32,
            merkleTree: SerializablePubkey::from(ADDRESS_TREE_ADDRESS),
            rootSeq: proof.rootSeq as u64,
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
    let context = Context::extract(conn).await?;
    let tx = conn.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }
    let new_address_proofs = get_multiple_new_address_proofs_helper(&tx, addresses.0).await?;
    tx.commit().await?;

    Ok(GetMultipleNewAddressProofsResponse {
        value: new_address_proofs,
        context,
    })
}
