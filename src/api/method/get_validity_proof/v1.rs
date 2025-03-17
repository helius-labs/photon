use super::common::{get_public_input_hash, hash_to_hex};
use crate::{
    api::error::PhotonApiError, common::typedefs::serializable_pubkey::SerializablePubkey,
};
use light_batched_merkle_tree::constants::{
    DEFAULT_BATCH_ADDRESS_TREE_HEIGHT, DEFAULT_BATCH_STATE_TREE_HEIGHT,
};
use light_batched_merkle_tree::merkle_tree_metadata::BatchedMerkleTreeMetadata;
use light_prover_client::prove_utils::CircuitType;
use light_sdk::STATE_MERKLE_TREE_HEIGHT;
use reqwest::Client;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};

use crate::api::method::get_multiple_new_address_proofs::{
    get_multiple_new_address_proofs_helper, AddressWithTree, LEGACY_ADDRESS_TREE,
};
use crate::api::method::get_validity_proof::common::{
    convert_inclusion_proofs_to_hex, convert_non_inclusion_merkle_proof_to_hex,
    negate_and_compress_proof, proof_from_json_struct, CompressedProofWithContext,
    GetValidityProofRequest, GetValidityProofResponse, GnarkProofJson, HexBatchInputsForProver,
    STATE_TREE_QUEUE_SIZE,
};
use crate::common::typedefs::context::Context;
use crate::ingester::persist::get_multiple_compressed_leaf_proofs;

pub async fn get_validity_proof(
    conn: &DatabaseConnection,
    prover_url: &str,
    mut request: GetValidityProofRequest,
) -> Result<GetValidityProofResponse, PhotonApiError> {
    if request.hashes.is_empty()
        && request.newAddresses.is_empty()
        && request.newAddressesWithTrees.is_empty()
    {
        return Err(PhotonApiError::ValidationError(
            "No hashes or new addresses provided for proof generation".to_string(),
        ));
    }
    if !request.newAddressesWithTrees.is_empty() && !request.newAddresses.is_empty() {
        return Err(PhotonApiError::ValidationError(
            "Cannot provide both newAddresses and newAddressesWithTree".to_string(),
        ));
    }
    if !request.newAddresses.is_empty() {
        request.newAddressesWithTrees = request
            .newAddresses
            .iter()
            .map(|new_address| AddressWithTree {
                address: *new_address,
                tree: SerializablePubkey::from(LEGACY_ADDRESS_TREE),
            })
            .collect();
    }
    let context = Context::extract(conn).await?;
    let client = Client::new();
    let tx = conn.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }

    let account_proofs = match !request.hashes.is_empty() {
        true => get_multiple_compressed_leaf_proofs(&tx, request.hashes).await?,
        false => {
            vec![]
        }
    };

    let new_address_proofs = match !request.newAddressesWithTrees.is_empty() {
        true => get_multiple_new_address_proofs_helper(&tx, request.newAddressesWithTrees).await?,
        false => {
            vec![]
        }
    };
    tx.commit().await?;

    let state_tree_height = if account_proofs.is_empty() {
        0
    } else {
        account_proofs[0].proof.len()
    };
    let all_state_trees_height_is_equal = account_proofs
        .iter()
        .all(|x| x.proof.len() == state_tree_height);
    if !all_state_trees_height_is_equal {
        return Err(PhotonApiError::ValidationError(
            "All state trees must have the same height".to_string(),
        ));
    }

    let address_tree_height = if new_address_proofs.is_empty() {
        0
    } else {
        new_address_proofs[0].proof.len()
    };

    let all_address_trees_height_is_equal = new_address_proofs
        .iter()
        .all(|x| x.proof.len() == address_tree_height);
    if !all_address_trees_height_is_equal {
        return Err(PhotonApiError::ValidationError(
            "All address trees must have the same height".to_string(),
        ));
    }

    if state_tree_height != address_tree_height
        && address_tree_height != 0
        && state_tree_height != 0
    {
        // TODO: change error msg and if condition once batched address Merkle trees are supported
        return Err(PhotonApiError::ValidationError(
            "State tree height must be equal to address tree height (height 26).
               Address creation with batched Merkle trees is not supported at this time."
                .to_string(),
        ));
    }
    let circuit_type = match (account_proofs.is_empty(), new_address_proofs.is_empty()) {
        (false, true) => CircuitType::Inclusion,
        (true, false) => CircuitType::NonInclusion,
        (false, false) => CircuitType::Combined,
        _ => {
            return Err(PhotonApiError::ValidationError(
                "No proofs found for the provided hashes or new addresses".to_string(),
            ))
        }
    };

    let is_v2 = (state_tree_height == DEFAULT_BATCH_STATE_TREE_HEIGHT as usize)
        || (address_tree_height == DEFAULT_BATCH_ADDRESS_TREE_HEIGHT as usize);
    let public_input_hash = if is_v2 {
        hash_to_hex(&crate::common::typedefs::hash::Hash(get_public_input_hash(
            &account_proofs,
            &new_address_proofs,
        )))
    } else {
        String::new()
    };

    let queue_size = if state_tree_height == STATE_MERKLE_TREE_HEIGHT {
        STATE_TREE_QUEUE_SIZE
    } else {
        BatchedMerkleTreeMetadata::default().root_history_capacity as u64
    };

    let batch_inputs = HexBatchInputsForProver {
        circuit_type: circuit_type.to_string(),
        state_tree_height: state_tree_height as u32,
        address_tree_height: address_tree_height as u32,
        public_input_hash,
        input_compressed_accounts: convert_inclusion_proofs_to_hex(account_proofs.clone()),
        new_addresses: convert_non_inclusion_merkle_proof_to_hex(new_address_proofs.clone()),
    };

    let inclusion_proof_url = format!("{}/prove", prover_url);
    let json_body = serde_json::to_string(&batch_inputs).map_err(|e| {
        PhotonApiError::UnexpectedError(format!("Got an error while serializing the request {}", e))
    })?;

    let res = client
        .post(&inclusion_proof_url)
        .body(json_body.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Error fetching proof {}", e)))?;

    if !res.status().is_success() {
        return Err(PhotonApiError::UnexpectedError(format!(
            "Error fetching proof {:?}",
            res.text().await,
        )));
    }

    let text = res
        .text()
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Error fetching proof {}", e)))?;

    let proof: GnarkProofJson = serde_json::from_str(&text).map_err(|e| {
        PhotonApiError::UnexpectedError(format!(
            "Got an error while deserializing the response {}",
            e
        ))
    })?;

    let proof = proof_from_json_struct(proof);
    // Allow non-snake case
    #[allow(non_snake_case)]
    let compressedProof = negate_and_compress_proof(proof);

    let compressed_proof_with_context = CompressedProofWithContext {
        compressedProof,
        roots: account_proofs
            .iter()
            .map(|x| x.root.clone().to_string())
            .chain(
                new_address_proofs
                    .iter()
                    .map(|x| x.root.clone().to_string()),
            )
            .collect(),
        rootIndices: account_proofs
            .iter()
            .map(|x| x.root_seq)
            .chain(new_address_proofs.iter().map(|x| x.rootSeq))
            .map(|x| x % queue_size)
            .collect(),
        leafIndices: account_proofs
            .iter()
            .map(|x| x.leaf_index)
            .chain(new_address_proofs.iter().map(|x| x.lowElementLeafIndex))
            .collect(),
        leaves: account_proofs
            .iter()
            .map(|x| x.hash.clone().to_string())
            .chain(
                new_address_proofs
                    .iter()
                    .map(|x| x.address.clone().to_string()),
            )
            .collect(),
        merkleTrees: account_proofs
            .iter()
            .map(|x| x.merkle_tree.clone().to_string())
            .chain(
                new_address_proofs
                    .iter()
                    .map(|x| x.merkleTree.clone().to_string()),
            )
            .collect(),
    };
    Ok(GetValidityProofResponse {
        value: compressed_proof_with_context,
        context,
    })
}
