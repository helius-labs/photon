use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use solana_program::pubkey;
use solana_sdk::pubkey::Pubkey;
use utoipa::ToSchema;

use crate::api::error::PhotonApiError;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::persist::persisted_indexed_merkle_tree::get_exclusion_range_with_proof;

pub const ADDRESS_TREE_HEIGHT: u32 = 27;
const ADDRESS_TREE_ADDRESS: Pubkey = pubkey!("C83cpRN6oaafjNgMQJvaYgAz592EP5wunKvbokeTKPLn");

use super::utils::Context;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MerkleContextWithNewAddressProof {
    pub root: Hash,
    pub address: SerializablePubkey,
    pub lower_range_address: SerializablePubkey,
    pub higher_range_address: SerializablePubkey,
    pub leaf_index: u32,
    pub proof: Vec<Hash>,
    pub merkle_tree: SerializablePubkey,
    pub root_seq: u64,
    pub low_element_leaf_index: u32,
}

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetMultipleNewAddressProofsResponse {
    pub context: Context,
    pub value: Vec<MerkleContextWithNewAddressProof>,
}

pub async fn get_multiple_new_address_proofs_helper(
    conn: &DatabaseConnection,
    addresses: Vec<SerializablePubkey>,
) -> Result<Vec<MerkleContextWithNewAddressProof>, PhotonApiError> {
    if addresses.is_empty() {
        return Err(PhotonApiError::ValidationError(
            "No addresses provided".to_string(),
        ));
    }
    let mut new_address_proofs: Vec<MerkleContextWithNewAddressProof> = Vec::new();

    for address in addresses {
        let (model, proof) = get_exclusion_range_with_proof(
            conn,
            ADDRESS_TREE_ADDRESS.to_bytes().to_vec(),
            ADDRESS_TREE_HEIGHT,
            address.to_bytes_vec(),
        )
        .await?;
        let new_address_proof = MerkleContextWithNewAddressProof {
            root: proof.root,
            address,
            lower_range_address: SerializablePubkey::try_from(model.value)?,
            higher_range_address: SerializablePubkey::try_from(model.next_value)?,
            leaf_index: model.next_index as u32,
            proof: proof.proof,
            low_element_leaf_index: model.leaf_index as u32,
            merkle_tree: SerializablePubkey::from(ADDRESS_TREE_ADDRESS),
            root_seq: proof.root_seq as u64,
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
    let new_address_proofs = get_multiple_new_address_proofs_helper(conn, addresses.0).await?;

    Ok(GetMultipleNewAddressProofsResponse {
        value: new_address_proofs,
        context,
    })
}
