use crate::api::error::PhotonApiError;
use crate::api::method::get_multiple_new_address_proofs::MerkleContextWithNewAddressProof;
use crate::api::method::get_validity_proof::prover::gnark::negate_g1;
use crate::api::method::get_validity_proof::prover::structs::{
    GnarkProofJson, InclusionHexInputsForProver, NonInclusionHexInputsForProver, ProofABC,
};
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::persist::MerkleProofWithContext;
use borsh::BorshSerialize;
use light_compressed_account::hash_chain::create_two_inputs_hash_chain;

pub fn convert_non_inclusion_merkle_proof_to_hex(
    non_inclusion_merkle_proof_inputs: Vec<MerkleContextWithNewAddressProof>,
) -> Vec<NonInclusionHexInputsForProver> {
    let mut inputs: Vec<NonInclusionHexInputsForProver> = Vec::new();
    for i in 0..non_inclusion_merkle_proof_inputs.len() {
        let input = NonInclusionHexInputsForProver {
            root: hash_to_hex(&non_inclusion_merkle_proof_inputs[i].root),
            value: pubkey_to_hex(&non_inclusion_merkle_proof_inputs[i].address),
            path_index: non_inclusion_merkle_proof_inputs[i].lowElementLeafIndex,
            path_elements: non_inclusion_merkle_proof_inputs[i]
                .proof
                .iter()
                .map(hash_to_hex)
                .collect(),
            next_index: non_inclusion_merkle_proof_inputs[i].nextIndex,
            leaf_lower_range_value: pubkey_to_hex(
                &non_inclusion_merkle_proof_inputs[i].lowerRangeAddress,
            ),
            leaf_higher_range_value: pubkey_to_hex(
                &non_inclusion_merkle_proof_inputs[i].higherRangeAddress,
            ),
        };
        inputs.push(input);
    }
    inputs
}

pub fn convert_inclusion_proofs_to_hex(
    inclusion_proof_inputs: Vec<MerkleProofWithContext>,
) -> Vec<InclusionHexInputsForProver> {
    let mut inputs: Vec<InclusionHexInputsForProver> = Vec::new();
    for i in 0..inclusion_proof_inputs.len() {
        let input = InclusionHexInputsForProver {
            root: hash_to_hex(&inclusion_proof_inputs[i].root),
            path_index: inclusion_proof_inputs[i].leaf_index,
            path_elements: inclusion_proof_inputs[i]
                .proof
                .iter()
                .map(hash_to_hex)
                .collect(),
            leaf: hash_to_hex(&inclusion_proof_inputs[i].hash),
        };
        inputs.push(input);
    }
    inputs
}

pub fn hash_to_hex(hash: &Hash) -> String {
    let bytes = hash.to_vec();
    let hex = hex::encode(bytes);
    format!("0x{}", hex)
}

fn pubkey_to_hex(pubkey: &SerializablePubkey) -> String {
    let bytes = pubkey.to_bytes_vec();
    let hex = hex::encode(bytes);

    format!("0x{}", hex)
}

pub fn deserialize_hex_string_to_bytes(hex_str: &str) -> Result<Vec<u8>, PhotonApiError> {
    let hex_str = if hex_str.starts_with("0x") {
        &hex_str[2..]
    } else {
        hex_str
    };
    let hex_str = format!("{:0>64}", hex_str);

    hex::decode(hex_str)
        .map_err(|_| PhotonApiError::UnexpectedError("Failed to decode hex string".to_string()))
}

pub fn proof_from_json_struct(json: GnarkProofJson) -> Result<ProofABC, PhotonApiError> {
    let proof_a_x = deserialize_hex_string_to_bytes(&json.ar[0])?;
    let proof_a_y = deserialize_hex_string_to_bytes(&json.ar[1])?;
    let proof_a: [u8; 64] = [proof_a_x, proof_a_y].concat().try_into().map_err(|_| {
        PhotonApiError::UnexpectedError("Failed to convert proof_a to [u8; 64]".to_string())
    })?;
    let proof_a = negate_g1(&proof_a)?;

    let proof_b_x_0 = deserialize_hex_string_to_bytes(&json.bs[0][0])?;
    let proof_b_x_1 = deserialize_hex_string_to_bytes(&json.bs[0][1])?;
    let proof_b_y_0 = deserialize_hex_string_to_bytes(&json.bs[1][0])?;
    let proof_b_y_1 = deserialize_hex_string_to_bytes(&json.bs[1][1])?;
    let proof_b: [u8; 128] = [proof_b_x_0, proof_b_x_1, proof_b_y_0, proof_b_y_1]
        .concat()
        .try_into()
        .map_err(|_| {
            PhotonApiError::UnexpectedError("Failed to convert proof_b to [u8; 128]".to_string())
        })?;

    let proof_c_x = deserialize_hex_string_to_bytes(&json.krs[0])?;
    let proof_c_y = deserialize_hex_string_to_bytes(&json.krs[1])?;
    let proof_c: [u8; 64] = [proof_c_x, proof_c_y].concat().try_into().map_err(|_| {
        PhotonApiError::UnexpectedError("Failed to convert proof_c to [u8; 64]".to_string())
    })?;

    Ok(ProofABC {
        a: proof_a,
        b: proof_b,
        c: proof_c,
    })
}

pub fn get_public_input_hash(
    account_proofs: &[MerkleProofWithContext],
    new_address_proofs: &[MerkleContextWithNewAddressProof],
) -> Result<[u8; 32], PhotonApiError> {
    let account_hashes: Result<Vec<[u8; 32]>, PhotonApiError> = account_proofs
        .iter()
        .map(|x| {
            x.hash.to_vec().try_into().map_err(|_| {
                PhotonApiError::UnexpectedError("Failed to convert hash to [u8; 32]".to_string())
            })
        })
        .collect();
    let account_hashes = account_hashes?;

    let account_roots: Result<Vec<[u8; 32]>, PhotonApiError> = account_proofs
        .iter()
        .map(|x| {
            x.root.to_vec().try_into().map_err(|_| {
                PhotonApiError::UnexpectedError("Failed to convert root to [u8; 32]".to_string())
            })
        })
        .collect();
    let account_roots = account_roots?;

    let inclusion_hash_chain = create_two_inputs_hash_chain(&account_roots, &account_hashes)
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to create hash chain: {}", e))
        })?;

    let new_address_hashes: Result<Vec<[u8; 32]>, PhotonApiError> = new_address_proofs
        .iter()
        .map(|x| {
            x.address
                .try_to_vec()
                .map_err(|e| {
                    PhotonApiError::UnexpectedError(format!("Failed to serialize address: {}", e))
                })?
                .try_into()
                .map_err(|_| {
                    PhotonApiError::UnexpectedError(
                        "Failed to convert address bytes to [u8; 32]".to_string(),
                    )
                })
        })
        .collect();
    let new_address_hashes = new_address_hashes?;

    let new_address_roots: Result<Vec<[u8; 32]>, PhotonApiError> = new_address_proofs
        .iter()
        .map(|x| {
            x.root.to_vec().try_into().map_err(|_| {
                PhotonApiError::UnexpectedError(
                    "Failed to convert new address root to [u8; 32]".to_string(),
                )
            })
        })
        .collect();
    let new_address_roots = new_address_roots?;

    let non_inclusion_hash_chain =
        create_two_inputs_hash_chain(&new_address_roots, &new_address_hashes).map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to create hash chain: {}", e))
        })?;

    if non_inclusion_hash_chain != [0u8; 32] {
        Ok(non_inclusion_hash_chain)
    } else if inclusion_hash_chain != [0u8; 32] {
        Ok(inclusion_hash_chain)
    } else {
        create_two_inputs_hash_chain(&[inclusion_hash_chain], &[non_inclusion_hash_chain]).map_err(
            |e| PhotonApiError::UnexpectedError(format!("Failed to create hash chain: {}", e)),
        )
    }
}
