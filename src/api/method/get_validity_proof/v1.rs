use crate::api::method::get_multiple_new_address_proofs::{
    get_multiple_new_address_proofs_helper, AddressWithTree, MAX_ADDRESSES,
};
use crate::api::method::get_validity_proof::prover::prove::generate_proof;
use crate::api::method::get_validity_proof::CompressedProof;
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::ingester::persist::get_multiple_compressed_leaf_proofs;
use crate::{
    api::error::PhotonApiError, common::typedefs::serializable_pubkey::SerializablePubkey,
};
use jsonrpsee_core::Serialize;
use light_sdk_types::constants::ADDRESS_TREE_V1;
use sea_orm::{DatabaseConnection, TransactionTrait};
use serde::Deserialize;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Default, ToSchema, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CompressedProofWithContext {
    pub compressed_proof: CompressedProof,
    pub roots: Vec<String>,
    pub root_indices: Vec<u64>,
    pub leaf_indices: Vec<u32>,
    pub leaves: Vec<String>,
    pub merkle_trees: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetValidityProofRequest {
    #[serde(default)]
    pub hashes: Vec<Hash>,
    #[serde(default)]
    #[schema(deprecated = true)]
    pub new_addresses: Vec<SerializablePubkey>,
    #[serde(default)]
    pub new_addresses_with_trees: Vec<AddressWithTree>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetValidityProofRequestDocumentation {
    #[serde(default)]
    pub hashes: Vec<Hash>,
    #[serde(default)]
    pub new_addresses_with_trees: Vec<AddressWithTree>,
}

#[derive(Serialize, Deserialize, Default, ToSchema, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetValidityProofResponse {
    pub value: CompressedProofWithContext,
    pub context: Context,
}

pub async fn get_validity_proof(
    conn: &DatabaseConnection,
    prover_url: &str,
    prover_api_key: Option<&str>,
    mut request: GetValidityProofRequest,
) -> Result<GetValidityProofResponse, PhotonApiError> {
    if request.hashes.is_empty()
        && request.new_addresses.is_empty()
        && request.new_addresses_with_trees.is_empty()
    {
        return Err(PhotonApiError::ValidationError(
            "No hashes or new addresses provided for proof generation".to_string(),
        ));
    }
    if !request.new_addresses_with_trees.is_empty() && !request.new_addresses.is_empty() {
        return Err(PhotonApiError::ValidationError(
            "Cannot provide both newAddresses and newAddressesWithTree".to_string(),
        ));
    }
    if !request.new_addresses.is_empty() {
        request.new_addresses_with_trees = request
            .new_addresses
            .iter()
            .map(|new_address| AddressWithTree {
                address: *new_address,
                tree: SerializablePubkey::from(ADDRESS_TREE_V1),
            })
            .collect();
        request.new_addresses.clear();
    }

    let context = Context::extract(conn).await?;

    let tx = conn.begin().await?;
    crate::api::set_transaction_isolation_if_needed(&tx).await?;

    let db_account_proofs = if !request.hashes.is_empty() {
        get_multiple_compressed_leaf_proofs(&tx, request.hashes.clone()).await?
    } else {
        Vec::new()
    };

    let db_new_address_proofs = if !request.new_addresses_with_trees.is_empty() {
        get_multiple_new_address_proofs_helper(
            &tx,
            request.new_addresses_with_trees.clone(),
            MAX_ADDRESSES,
            true,
        )
        .await?
    } else {
        Vec::new()
    };

    // Fetch root_history_capacity per tree so each proof uses its own tree's modulus.
    let trees: Vec<Vec<u8>> = db_account_proofs
        .iter()
        .map(|p| p.merkle_tree.to_bytes_vec())
        .chain(db_new_address_proofs.iter().map(|p| p.merkleTree.to_bytes_vec()))
        .collect();
    let root_history_capacities = super::fetch_root_history_capacities(&tx, trees).await?;

    tx.commit().await?;

    if db_account_proofs.is_empty() && db_new_address_proofs.is_empty() {
        return Err(PhotonApiError::ValidationError(
            "No valid proofs found for the provided hashes or new addresses after DB check."
                .to_string(),
        ));
    }

    let proof_result = generate_proof(
        conn,
        db_account_proofs,
        db_new_address_proofs,
        &root_history_capacities,
        prover_url,
        prover_api_key,
    )
    .await?;

    let v1_value = CompressedProofWithContext {
        compressed_proof: proof_result.compressed_proof,
        roots: proof_result
            .account_proof_details
            .iter()
            .map(|d| d.root.clone())
            .chain(
                proof_result
                    .address_proof_details
                    .iter()
                    .map(|d| d.root.clone()),
            )
            .collect(),
        root_indices: proof_result
            .account_proof_details
            .iter()
            .map(|d| d.root_index_mod_queue)
            .chain(
                proof_result
                    .address_proof_details
                    .iter()
                    .map(|d| d.root_index_mod_queue),
            )
            .collect(),
        leaf_indices: proof_result
            .account_proof_details
            .iter()
            .map(|d| d.leaf_index)
            .chain(
                proof_result
                    .address_proof_details
                    .iter()
                    .map(|d| d.path_index),
            )
            .collect(),
        leaves: proof_result
            .account_proof_details
            .iter()
            .map(|d| d.hash.clone())
            .chain(
                proof_result
                    .address_proof_details
                    .iter()
                    .map(|d| d.address.clone()),
            )
            .collect(),
        merkle_trees: proof_result
            .account_proof_details
            .iter()
            .map(|d| d.merkle_tree_id.clone())
            .chain(
                proof_result
                    .address_proof_details
                    .iter()
                    .map(|d| d.merkle_tree_id.clone()),
            )
            .collect(),
    };

    Ok(GetValidityProofResponse {
        value: v1_value,
        context,
    })
}
