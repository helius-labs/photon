use crate::api::error::PhotonApiError;
use crate::api::method::get_multiple_new_address_proofs::MerkleContextWithNewAddressProof;
use crate::api::method::get_validity_proof::prover::gnark::compress_proof;
use crate::api::method::get_validity_proof::prover::helpers::{
    convert_inclusion_proofs_to_hex, convert_non_inclusion_merkle_proof_to_hex,
    get_public_input_hash, hash_to_hex, proof_from_json_struct,
};
use crate::api::method::get_validity_proof::prover::structs::{
    AccountProofDetail, AddressProofDetail, CircuitType, GnarkProofJson, HexBatchInputsForProver,
    ProverResult,
};
use crate::common::typedefs::hash::Hash;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::MerkleProofWithContext;
use light_batched_merkle_tree::constants::{
    DEFAULT_BATCH_ADDRESS_TREE_HEIGHT, DEFAULT_BATCH_STATE_TREE_HEIGHT,
};
use reqwest::Client;
use sea_orm::DatabaseConnection;

pub(crate) async fn generate_proof(
    conn: &DatabaseConnection,
    db_account_proofs: Vec<MerkleProofWithContext>,
    db_new_address_proofs: Vec<MerkleContextWithNewAddressProof>,
    root_history_capacity: u64,
    prover_url: &str,
) -> Result<ProverResult, PhotonApiError> {
    let state_tree_height = if db_account_proofs.is_empty() {
        0
    } else {
        db_account_proofs[0].proof.len()
    };
    if !db_account_proofs
        .iter()
        .all(|x| x.proof.len() == state_tree_height)
    {
        return Err(PhotonApiError::ValidationError(
            "All state trees for account proofs must have the same height".to_string(),
        ));
    }

    let address_tree_height = if db_new_address_proofs.is_empty() {
        0
    } else {
        db_new_address_proofs[0].proof.len()
    };

    if !db_new_address_proofs
        .iter()
        .all(|x| x.proof.len() == address_tree_height)
    {
        return Err(PhotonApiError::ValidationError(
            "All address trees for new address proofs must have the same height".to_string(),
        ));
    }

    let circuit_type = match (
        db_account_proofs.is_empty(),
        db_new_address_proofs.is_empty(),
    ) {
        (false, true) => CircuitType::Inclusion,
        (true, false) => CircuitType::NonInclusion,
        (false, false) => CircuitType::Combined,
        (true, true) => {
            return Err(PhotonApiError::ValidationError(
                "No proofs provided to generate_proof_and_details_internal.".to_string(),
            ));
        }
    };

    let is_v2_tree_height = (state_tree_height == DEFAULT_BATCH_STATE_TREE_HEIGHT as usize)
        || (address_tree_height == DEFAULT_BATCH_ADDRESS_TREE_HEIGHT as usize);

    // Compute and send public_input_hash for V2 circuits
    let public_input_hash_opt = if is_v2_tree_height {
        let public_input_hash_bytes =
            get_public_input_hash(&db_account_proofs, &db_new_address_proofs)?;
        Some(hash_to_hex(&Hash(public_input_hash_bytes)))
    } else {
        None
    };

    // Always use the actual root_history_capacity from the database
    // Each tree (V1 or V2) has its own queue size stored as root_history_capacity
    let queue_size = root_history_capacity;

    log::debug!(
        "Queue size: state_tree_height={}, address_tree_height={}, queue_size={}",
        state_tree_height,
        address_tree_height,
        queue_size
    );

    let batch_inputs = HexBatchInputsForProver {
        circuit_type: circuit_type.to_string(),
        state_tree_height: state_tree_height as u32,
        address_tree_height: address_tree_height as u32,
        public_input_hash: public_input_hash_opt.clone(),
        input_compressed_accounts: convert_inclusion_proofs_to_hex(db_account_proofs.clone()),
        new_addresses: convert_non_inclusion_merkle_proof_to_hex(db_new_address_proofs.clone()),
    };

    let client = Client::new();
    let prover_request_url = format!("{}/prove", prover_url);
    let json_body = serde_json::to_string(&batch_inputs).map_err(|e| {
        PhotonApiError::UnexpectedError(format!("Error serializing prover request: {}", e))
    })?;

    let res = client
        .post(&prover_request_url)
        .body(json_body)
        .header("Content-Type", "application/json")
        .send()
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Error sending request to prover: {}", e))
        })?;

    if !res.status().is_success() {
        return Err(PhotonApiError::UnexpectedError(format!(
            "Error fetching proof {:?}",
            res.text().await,
        )));
    }

    let response_text = res.text().await.map_err(|e| {
        PhotonApiError::UnexpectedError(format!("Error reading prover response: {}", e))
    })?;

    let proof_json: GnarkProofJson = serde_json::from_str(&response_text).map_err(|e| {
        PhotonApiError::UnexpectedError(format!(
            "Error deserializing prover response: {}. Response text: '{}'",
            e, response_text
        ))
    })?;

    let proof = proof_from_json_struct(proof_json)?;
    let compressed_proof = compress_proof(&proof)?;
    let mut account_details = Vec::with_capacity(db_account_proofs.len());
    for acc_proof in db_account_proofs.iter() {
        log::debug!("Proof generation: tree {} leaf_index {} root_seq {} queue_size {} root_index_mod_queue {}",
            acc_proof.merkle_tree, acc_proof.leaf_index, acc_proof.root_seq, queue_size, acc_proof.root_seq % queue_size);

        let tree_info = TreeInfo::get(conn, &acc_proof.merkle_tree.to_string())
            .await?
            .ok_or(PhotonApiError::UnexpectedError(format!(
                "Failed to get TreeInfo for account tree '{}'",
                acc_proof.merkle_tree
            )))?;
        account_details.push(AccountProofDetail {
            hash: acc_proof.hash.to_string(),
            root: acc_proof.root.to_string(),
            root_index_mod_queue: acc_proof.root_seq % queue_size,
            leaf_index: acc_proof.leaf_index,
            merkle_tree_id: acc_proof.merkle_tree.to_string(),
            tree_info,
        });
    }

    let mut address_details = Vec::with_capacity(db_new_address_proofs.len());
    for addr_proof in db_new_address_proofs.iter() {
        let tree_info = TreeInfo::get(conn, &addr_proof.merkleTree.to_string())
            .await?
            .ok_or(PhotonApiError::UnexpectedError(format!(
                "Failed to get TreeInfo for address tree '{}'",
                addr_proof.merkleTree
            )))?;
        address_details.push(AddressProofDetail {
            address: addr_proof.address.to_string(),
            root: addr_proof.root.to_string(),
            root_index_mod_queue: addr_proof.rootSeq % queue_size,
            path_index: addr_proof.lowElementLeafIndex,
            merkle_tree_id: addr_proof.merkleTree.to_string(),
            tree_info,
        });
    }

    Ok(ProverResult {
        compressed_proof,
        account_proof_details: account_details,
        address_proof_details: address_details,
    })
}
