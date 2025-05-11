use crate::api::method::get_multiple_new_address_proofs::MerkleContextWithNewAddressProof;
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

fn deserialize_hex_string_to_bytes(hex_str: &str) -> Vec<u8> {
    let hex_str = if hex_str.starts_with("0x") {
        &hex_str[2..]
    } else {
        hex_str
    };

    // Left pad with 0s if the length is not 64
    let hex_str = format!("{:0>64}", hex_str);

    hex::decode(&hex_str).expect("Failed to decode hex string")
}

pub fn proof_from_json_struct(json: GnarkProofJson) -> ProofABC {
    let proof_ax = deserialize_hex_string_to_bytes(&json.ar[0]);
    let proof_ay = deserialize_hex_string_to_bytes(&json.ar[1]);
    let proof_a = [proof_ax, proof_ay].concat();

    let proof_bx0 = deserialize_hex_string_to_bytes(&json.bs[0][0]);
    let proof_bx1 = deserialize_hex_string_to_bytes(&json.bs[0][1]);
    let proof_by0 = deserialize_hex_string_to_bytes(&json.bs[1][0]);
    let proof_by1 = deserialize_hex_string_to_bytes(&json.bs[1][1]);
    let proof_b = [proof_bx0, proof_bx1, proof_by0, proof_by1].concat();

    let proof_cx = deserialize_hex_string_to_bytes(&json.krs[0]);
    let proof_cy = deserialize_hex_string_to_bytes(&json.krs[1]);
    let proof_c = [proof_cx, proof_cy].concat();

    ProofABC {
        a: proof_a,
        b: proof_b,
        c: proof_c,
    }
}

pub fn get_public_input_hash(
    account_proofs: &[MerkleProofWithContext],
    new_address_proofs: &[MerkleContextWithNewAddressProof],
) -> [u8; 32] {
    let account_hashes: Vec<[u8; 32]> = account_proofs
        .iter()
        .map(|x| x.hash.to_vec().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let account_roots: Vec<[u8; 32]> = account_proofs
        .iter()
        .map(|x| x.root.to_vec().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let inclusion_hash_chain: [u8; 32] =
        create_two_inputs_hash_chain(&account_roots, &account_hashes).unwrap();
    let new_address_hashes: Vec<[u8; 32]> = new_address_proofs
        .iter()
        .map(|x| x.address.try_to_vec().unwrap().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let new_address_roots: Vec<[u8; 32]> = new_address_proofs
        .iter()
        .map(|x| x.root.to_vec().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let non_inclusion_hash_chain =
        create_two_inputs_hash_chain(&new_address_roots, &new_address_hashes).unwrap();
    if non_inclusion_hash_chain != [0u8; 32] {
        non_inclusion_hash_chain
    } else if inclusion_hash_chain != [0u8; 32] {
        inclusion_hash_chain
    } else {
        create_two_inputs_hash_chain(&[inclusion_hash_chain], &[non_inclusion_hash_chain]).unwrap()
    }
}
