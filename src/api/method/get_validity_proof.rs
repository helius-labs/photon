use crate::{
    api::error::PhotonApiError,
    common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey},
    ingester::persist::persisted_state_tree::{
        get_multiple_compressed_leaf_proofs, MerkleProofWithContext,
    },
};
use lazy_static::lazy_static;
use num_bigint::BigUint;
use reqwest::Client;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use utoipa::ToSchema;

use super::get_multiple_new_address_proofs::{
    get_multiple_new_address_proofs_helper, MerkleContextWithNewAddressProof,
};

lazy_static! {
    pub static ref FIELD_SIZE: BigUint = BigUint::from_str(
        "21888242871839275222246405745257275088548364400416034343698204186575808495616"
    )
    .unwrap();
}

pub const STATE_TREE_QUEUE_SIZE: u64 = 2400;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InclusionHexInputsForProver {
    root: String,
    path_index: u32,
    path_elements: Vec<String>,
    leaf: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NonInclusionHexInputsForProver {
    root: String,
    value: String,
    path_index: u32,
    path_elements: Vec<String>,
    leaf_lower_range_value: String,
    leaf_higher_range_value: String,
    next_index: u32,
}

fn convert_non_inclusion_merkle_proof_to_hex(
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
                .map(|x| hash_to_hex(x))
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

fn convert_inclusion_proofs_to_hex(
    inclusion_proof_inputs: Vec<MerkleProofWithContext>,
) -> Vec<InclusionHexInputsForProver> {
    let mut inputs: Vec<InclusionHexInputsForProver> = Vec::new();
    for i in 0..inclusion_proof_inputs.len() {
        let input = InclusionHexInputsForProver {
            root: hash_to_hex(&inclusion_proof_inputs[i].root),
            path_index: inclusion_proof_inputs[i].leafIndex,
            path_elements: inclusion_proof_inputs[i]
                .proof
                .iter()
                .map(|x| hash_to_hex(x))
                .collect(),
            leaf: hash_to_hex(&inclusion_proof_inputs[i].hash),
        };
        inputs.push(input);
    }
    inputs
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HexBatchInputsForProver {
    #[serde(
        rename = "input-compressed-accounts",
        skip_serializing_if = "Vec::is_empty"
    )]
    input_compressed_accounts: Vec<InclusionHexInputsForProver>,
    #[serde(rename = "new-addresses", skip_serializing_if = "Vec::is_empty")]
    new_addresses: Vec<NonInclusionHexInputsForProver>,
}

#[derive(Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct CompressedProofWithContext {
    pub compressedProof: CompressedProof,
    roots: Vec<String>,
    rootIndices: Vec<u64>,
    leafIndices: Vec<u32>,
    leaves: Vec<String>,
    merkleTrees: Vec<String>,
}

fn hash_to_hex(hash: &Hash) -> String {
    let bytes = hash.to_vec();
    let hex = hex::encode(bytes);
    format!("0x{}", hex)
}

fn pubkey_to_hex(pubkey: &SerializablePubkey) -> String {
    let bytes = pubkey.to_bytes_vec();
    let hex = hex::encode(bytes);
    format!("0x{}", hex)
}

#[derive(Serialize, Deserialize, Debug)]
struct GnarkProofJson {
    ar: [String; 2],
    bs: [[String; 2]; 2],
    krs: [String; 2],
}

#[derive(Debug)]
struct ProofABC {
    a: Vec<u8>,
    b: Vec<u8>,
    c: Vec<u8>,
}

#[derive(Serialize, Deserialize, ToSchema, Default)]
pub struct CompressedProof {
    a: Vec<u8>,
    b: Vec<u8>,
    c: Vec<u8>,
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

fn proof_from_json_struct(json: GnarkProofJson) -> ProofABC {
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

fn y_element_is_positive_g1(y_element: &BigUint) -> bool {
    y_element <= &(FIELD_SIZE.clone() - y_element)
}

fn y_element_is_positive_g2(y_element1: &BigUint, y_element2: &BigUint) -> bool {
    let field_midpoint = FIELD_SIZE.clone() / 2u32;

    if y_element1 < &field_midpoint {
        true
    } else if y_element1 > &field_midpoint {
        false
    } else {
        y_element2 < &field_midpoint
    }
}

fn add_bitmask_to_byte(mut byte: u8, y_is_positive: bool) -> u8 {
    if !y_is_positive {
        byte |= 1 << 7;
    }
    byte
}

fn negate_and_compress_proof(proof: ProofABC) -> CompressedProof {
    let proof_a = &proof.a;
    let proof_b = &proof.b;
    let proof_c = &proof.c;

    let a_x_element = &mut proof_a[0..32].to_vec();
    let a_y_element = BigUint::from_bytes_be(&proof_a[32..64]);

    let proof_a_is_positive = !y_element_is_positive_g1(&a_y_element);
    a_x_element[0] = add_bitmask_to_byte(a_x_element[0], proof_a_is_positive);

    let b_x_element = &mut proof_b[0..64].to_vec();
    let b_y_element = &proof_b[64..128];
    let b_y1_element = BigUint::from_bytes_be(&b_y_element[0..32]);
    let b_y2_element = BigUint::from_bytes_be(&b_y_element[32..64]);

    let proof_b_is_positive = y_element_is_positive_g2(&b_y1_element, &b_y2_element);
    b_x_element[0] = add_bitmask_to_byte(b_x_element[0], proof_b_is_positive);

    let c_x_element = &mut proof_c[0..32].to_vec();
    let c_y_element = BigUint::from_bytes_be(&proof_c[32..64]);

    let proof_c_is_positive = y_element_is_positive_g1(&c_y_element);
    c_x_element[0] = add_bitmask_to_byte(c_x_element[0], proof_c_is_positive);

    CompressedProof {
        a: a_x_element.clone(),
        b: b_x_element.clone(),
        c: c_x_element.clone(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct GetValidityProofRequest {
    pub hashes: Vec<Hash>,
    pub newAddresses: Vec<SerializablePubkey>,
}

pub async fn get_validity_proof(
    conn: &DatabaseConnection,
    request: GetValidityProofRequest,
) -> Result<CompressedProofWithContext, PhotonApiError> {
    if request.hashes.is_empty() && request.newAddresses.is_empty() {
        return Err(PhotonApiError::UnexpectedError(
            "No hashes or new addresses provided for proof generation".to_string(),
        ));
    }
    let client = Client::new();
    let prover_endpoint = "http://localhost:3001";

    let account_proofs = match !request.hashes.is_empty() {
        true => get_multiple_compressed_leaf_proofs(conn, request.hashes).await?,
        false => {
            vec![]
        }
    };
    let new_address_proofs = match !request.newAddresses.is_empty() {
        true => get_multiple_new_address_proofs_helper(conn, request.newAddresses).await?,
        false => {
            vec![]
        }
    };

    let batch_inputs = HexBatchInputsForProver {
        input_compressed_accounts: convert_inclusion_proofs_to_hex(account_proofs.clone()),
        new_addresses: convert_non_inclusion_merkle_proof_to_hex(new_address_proofs.clone()),
    };

    let inclusion_proof_url = format!("{}/prove", prover_endpoint);
    let json_body = serde_json::to_string(&batch_inputs).map_err(|e| {
        PhotonApiError::UnexpectedError(format!(
            "Got an error while serializing the request {}",
            e.to_string()
        ))
    })?;
    let res = client
        .post(&inclusion_proof_url)
        .body(json_body.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Error fetching proof {}", e.to_string()))
        })?;

    if !res.status().is_success() {
        return Err(PhotonApiError::UnexpectedError(format!(
            "Error fetching proof {:?}",
            res.text().await,
        )));
    }

    let text = res.text().await.map_err(|e| {
        PhotonApiError::UnexpectedError(format!("Error fetching proof {}", e.to_string()))
    })?;

    let proof: GnarkProofJson = serde_json::from_str(&text).map_err(|e| {
        PhotonApiError::UnexpectedError(format!(
            "Got an error while deserializing the response {}",
            e.to_string()
        ))
    })?;

    let proof = proof_from_json_struct(proof);
    // Allow non-snake case
    #[allow(non_snake_case)]
    let compressedProof = negate_and_compress_proof(proof);

    Ok(CompressedProofWithContext {
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
            .map(|x| x.rootSeq)
            .chain(new_address_proofs.iter().map(|x| x.rootSeq))
            .map(|x| x % STATE_TREE_QUEUE_SIZE)
            .collect(),
        leafIndices: account_proofs
            .iter()
            .map(|x| x.leafIndex)
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
            .map(|x| x.merkleTree.clone().to_string())
            .chain(
                new_address_proofs
                    .iter()
                    .map(|x| x.merkleTree.clone().to_string()),
            )
            .collect(),
    })
}
