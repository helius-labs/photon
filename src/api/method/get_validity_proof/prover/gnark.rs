use crate::api::error::PhotonApiError;
use crate::api::method::get_validity_proof::prover::structs::{CompressedProof, ProofABC};
use ark_serialize::{CanonicalDeserialize, CanonicalSerialize, Compress, Validate};
use solana_program::alt_bn128::compression::prelude::{
    alt_bn128_g1_compress, alt_bn128_g2_compress, convert_endianness,
};
use std::ops::Neg;

type G1 = ark_bn254::g1::G1Affine;

pub fn negate_g1(g1_be: &[u8; 64]) -> Result<[u8; 64], PhotonApiError> {
    let g1_le = convert_endianness::<32, 64>(g1_be);
    let g1: G1 = G1::deserialize_with_mode(g1_le.as_slice(), Compress::No, Validate::No).unwrap();

    let g1_neg = g1.neg();
    let mut g1_neg_be = [0u8; 64];
    g1_neg
        .x
        .serialize_with_mode(&mut g1_neg_be[..32], Compress::No)
        .map_err(|_| {
            PhotonApiError::UnexpectedError("Failed to serialize G1 x coordinate".to_string())
        })?;
    g1_neg
        .y
        .serialize_with_mode(&mut g1_neg_be[32..], Compress::No)
        .map_err(|_| {
            PhotonApiError::UnexpectedError("Failed to serialize G1 y coordinate".to_string())
        })?;
    let g1_neg_be: [u8; 64] = convert_endianness::<32, 64>(&g1_neg_be);
    Ok(g1_neg_be)
}

pub fn compress_proof(proof: &ProofABC) -> Result<CompressedProof, PhotonApiError> {
    let proof_a = alt_bn128_g1_compress(&proof.a)
        .map_err(|_| PhotonApiError::UnexpectedError("Failed to compress G1 proof".to_string()))?;
    let proof_b = alt_bn128_g2_compress(&proof.b)
        .map_err(|_| PhotonApiError::UnexpectedError("Failed to compress G2 proof".to_string()))?;
    let proof_c = alt_bn128_g1_compress(&proof.c)
        .map_err(|_| PhotonApiError::UnexpectedError("Failed to compress G1 proof".to_string()))?;

    Ok(CompressedProof {
        a: Vec::from(proof_a),
        b: Vec::from(proof_b),
        c: Vec::from(proof_c),
    })
}
