mod prover;
mod v1;
mod v2;

use crate::api::error::PhotonApiError;
use crate::dao::generated::{prelude::*, tree_metadata};
use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
use std::collections::HashMap;

/// Fetches `root_history_capacity` for each unique tree referenced by the given
/// proofs. Each tree (V1/V2, state/address) has its own queue size, so proofs
/// must use their own tree's modulus when computing the root index.
pub(crate) async fn fetch_root_history_capacities(
    tx: &DatabaseTransaction,
    trees: Vec<Vec<u8>>,
) -> Result<HashMap<Vec<u8>, u64>, PhotonApiError> {
    let mut capacities: HashMap<Vec<u8>, u64> = HashMap::new();
    for tree in trees {
        if capacities.contains_key(&tree) {
            continue;
        }
        let meta = TreeMetadata::find()
            .filter(tree_metadata::Column::TreePubkey.eq(tree.clone()))
            .one(tx)
            .await?
            .ok_or_else(|| {
                PhotonApiError::ValidationError(format!(
                    "Tree metadata not found for {}. Please ensure tree metadata sync has been run.",
                    bs58::encode(&tree).into_string()
                ))
            })?;
        capacities.insert(tree, meta.root_history_capacity as u64);
    }
    Ok(capacities)
}

pub use prover::CompressedProof;
pub use v1::{
    get_validity_proof, CompressedProofWithContext, GetValidityProofRequest,
    GetValidityProofRequestDocumentation, GetValidityProofResponse,
};
pub use v2::{
    get_validity_proof_v2, AccountProofInputs, AddressProofInputs, CompressedProofWithContextV2,
    GetValidityProofRequestV2, GetValidityProofResponseV2, MerkleContextV2, RootIndex,
    TreeContextInfo,
};
