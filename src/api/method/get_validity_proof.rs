use crate::common::typedefs::hash::Hash;
use reqwest::Client;
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::error::Error;

use super::get_multiple_compressed_account_proofs::{get_multiple_compressed_account_proofs_helper, MerkleProofWithContext};

#[derive(Serialize, Deserialize)]
struct CompressedProofWithContext {
    compressed_proof: String,
    roots: Vec<u64>,
    root_indices: Vec<usize>,
    leaf_indices: Vec<usize>,
    leaves: Vec<u64>,
    merkle_trees: Vec<String>,
    nullifier_queues: Vec<String>,
}

fn to_hex(num: u64) -> String {
    format!("{:x}", num)
}

struct GrankProverInput {
    roots: Vec<Hash>,
    inPathIndices: Vec<u32>,
    inPathElements: Vec<Vec<String>>,
    leaves: Vec<String>,
}

impl GrankProverInput {
    fn append_proof(&self, proof: MerkleProofWithContext) {
        roots.
    }
}

async fn get_validity_proof(
    conn: &DatabaseConnection,
    hashes: Vec<Hash>,
) -> Result<CompressedProofWithContext, Box<dyn Error>> {
    let client = Client::new();
    let prover_endpoint = "http://localhost:8000"; // Change this as necessary

    // Get merkle proofs
    let merkle_proofs_with_context =
        get_multiple_compressed_account_proofs_helper(conn, hashes).await?;

    // Convert to hex
    // let inputs = json!({
    //     "roots": merkle_proofs_with_context.iter().map(|ctx| to_hex(ctx.root)).collect::<Vec<_>>(),
    //     "inPathIndices": merkle_proofs_with_context.iter().map(|proof| proof.leaf_index).collect::<Vec<_>>(),
    //     "inPathElements": merkle_proofs_with_context.iter().map(|proof| proof.merkle_proof.iter().map(to_hex).collect::<Vec<_>>()).collect::<Vec<_>>(),
    //     "leaves": merkle_proofs_with_context.iter().map(|proof| to_hex(bn(proof.hash))).collect::<Vec<_>>(),
    // });

    let inputs = 

    let inclusion_proof_url = format!("{}/inclusion", prover_endpoint);
    let res = client
        .post(&inclusion_proof_url)
        .json(&inputs)
        .send()
        .await?;

    if !res.status().is_success() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Error fetching proof: {}", res.status()),
        )));
    }

    let data = res.json::<serde_json::Value>().await?;
    let parsed = proof_from_json_struct(&data); // Assumes a function to parse JSON into your Rust types
    let compressed_proof = negate_and_compress_proof(parsed); // Assumes implementation available

    let value = CompressedProofWithContext {
        compressed_proof,
        roots: merkle_proofs_with_context
            .iter()
            .map(|proof| proof.root)
            .collect(),
        root_indices: merkle_proofs_with_context
            .iter()
            .map(|proof| proof.root_index)
            .collect(),
        leaf_indices: merkle_proofs_with_context
            .iter()
            .map(|proof| proof.leaf_index)
            .collect(),
        leaves: merkle_proofs_with_context
            .iter()
            .map(|proof| bn(proof.hash))
            .collect(),
        merkle_trees: merkle_proofs_with_context
            .iter()
            .map(|proof| proof.merkle_tree.clone())
            .collect(),
        nullifier_queues: merkle_proofs_with_context
            .iter()
            .map(|proof| proof.nullifier_queue.clone())
            .collect(),
    };
    Ok(value)
}
