use crate::{
    api::{api::PhotonApi, error::PhotonApiError},
    common::typedefs::hash::Hash,
};
use lazy_static::lazy_static;
use num_bigint::BigUint;
use reqwest::Client;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::error::Error;
use std::{convert::TryInto, str::FromStr};

use super::get_multiple_compressed_account_proofs::{
    get_multiple_compressed_account_proofs_helper, MerkleProofWithContext,
};

lazy_static! {
    static ref FIELD_SIZE: BigUint = BigUint::from_str(
        "21888242871839275222246405745257275088548364400416034343698204186575808495616"
    )
    .unwrap();
}
#[derive(Serialize, Deserialize)]
struct CompressedProofWithContext {
    compressed_proof: CompressedProof,
    roots: Vec<String>,
    root_indices: Vec<u64>,
    leaf_indices: Vec<u32>,
    leaves: Vec<Hash>,
    merkle_trees: Vec<String>,
    nullifier_queues: Vec<String>,
}

fn hash_to_hex(hash: &Hash) -> String {
    let bytes = hash.to_vec();
    let hex = hex::encode(bytes);
    hex
}

#[derive(Default, Serialize)]
struct GrankProverInput {
    roots: Vec<String>,
    in_path_indices: Vec<u32>,
    in_path_elements: Vec<Vec<String>>,
    leaves: Vec<String>,
}

impl GrankProverInput {
    fn append_proof(&mut self, proof: &MerkleProofWithContext) {
        self.roots.push(hash_to_hex(&proof.root));
        self.in_path_indices.push(proof.leaf_index);
        self.in_path_elements
            .push(proof.proof.iter().map(hash_to_hex).collect());
        self.leaves.push(hash_to_hex(&proof.hash));
    }
}

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
struct CompressedProof {
    a: Vec<u8>,
    b: Vec<u8>,
    c: Vec<u8>,
}

fn deserialize_hex_string_to_bytes(hex_str: &str) -> Vec<u8> {
    hex::decode(hex_str).expect("Failed to decode hex string")
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

fn add_bitmask_to_byte(byte: u8, is_positive: bool) -> u8 {
    if is_positive {
        byte | 0x80
    } else {
        byte & 0x7F
    }
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

async fn get_validity_proof(
    conn: &DatabaseConnection,
    hashes: Vec<Hash>,
) -> Result<CompressedProofWithContext, Box<dyn Error>> {
    let client = Client::new();
    let prover_endpoint = "http://localhost:3001"; // Change this as necessary

    // Get merkle proofs
    let merkle_proofs_with_context =
        get_multiple_compressed_account_proofs_helper(conn, hashes).await?;

    let mut grank_proof_input = GrankProverInput::default();
    for proof in merkle_proofs_with_context.iter() {
        grank_proof_input.append_proof(proof);
    }

    let inclusion_proof_url = format!("{}/inclusion", prover_endpoint);
    let json_body = serde_json::to_string(&grank_proof_input).map_err(|e| {
        PhotonApiError::UnexpectedError(format!(
            "Got an error while serializing the request {}",
            e.to_string()
        ))
    })?;
    let res = client
        .post(&inclusion_proof_url)
        .body(json_body)
        .header("Content-Type", "application/json")
        .send()
        .await?;

    if !res.status().is_success() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Error fetching proof: {}", res.status()),
        )));
    }

    let text = res
        .text()
        .await
        .map_err(|e| PhotonApiError::UnexpectedError("Error fetching proof".to_string()))?;

    let proof: GnarkProofJson = serde_json::from_str(&text).map_err(|e| {
        PhotonApiError::UnexpectedError(format!(
            "Got an error while deserializing the response {}",
            e.to_string()
        ))
    })?;

    let proof = proof_from_json_struct(proof);
    let compressed_proof = negate_and_compress_proof(proof);

    Ok(CompressedProofWithContext {
        compressed_proof,
        roots: grank_proof_input
            .roots
            .iter()
            .map(|x| x.parse().unwrap())
            .collect(),
        root_indices: todo!(),
        leaf_indices: todo!(),
        leaves: todo!(),
        merkle_trees: todo!(),
        nullifier_queues: todo!(),
    })
}
