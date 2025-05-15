use crate::api::method::get_validity_proof::prover::structs::{CompressedProof, ProofABC};
use ark_serialize::{CanonicalDeserialize, CanonicalSerialize, Compress, Validate};
use std::ops::Neg;
use crate::api::error::PhotonApiError;

type G1 = ark_bn254::g1::G1Affine;

/// Changes the endianness of the given slice of bytes by reversing the order of every 32-byte chunk.
///
/// # Arguments
///
/// * `bytes` - A reference to a slice of bytes whose endianness will be changed.
///
/// # Returns
///
/// A `Vec<u8>` containing the bytes with their order reversed in chunks of 32 bytes. If the number
/// of bytes in the slice is not a multiple of 32, the remaining bytes at the end will also be reversed.
///
/// # Examples
///
/// ```
/// let input = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
///                  0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
///                  0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
///                  0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20];
/// let output = change_endianness(&input);
/// assert_eq!(output, vec![0x20, 0x1F, 0x1E, 0x1D, 0x1C, 0x1B, 0x1A, 0x19,
///                         0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11,
///                         0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0x09,
///                         0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
///
/// let input = vec![0x01, 0x02, 0x03];
/// let output = change_endianness(&input);
/// assert_eq!(output, vec![0x03, 0x02, 0x01]);
/// ```
fn change_endianness(bytes: &[u8]) -> Vec<u8> {
    let mut vec = Vec::new();
    for b in bytes.chunks(32) {
        for byte in b.iter().rev() {
            vec.push(*byte);
        }
    }
    vec
}

/// Negates the `a` component of the given proof and compresses the proof into a `CompressedProof`.
///
/// # Arguments
///
/// * `proof` - A `ProofABC` structure containing three components: `a`, `b`, and `c`.
///
///     - `a` is negated and serialized in big-endian format.
///     - `b` and `c` are trimmed and included as-is in the compressed form.
///
/// # Returns
///
/// A `CompressedProof` containing:
///
/// * The negated and serialized `a` component as a vector of bytes.
/// * The first 64 bytes of the `b` component.
/// * The first 32 bytes of the `c` component.
///
/// # Panics
///
/// This function will panic if:
///
/// * The deserialization or serialization of the `G1` point fails.
/// * The `proof.a` slice length is insufficient to produce a valid G1 when adding padding for deserialization.
///
/// # Note
///
/// The function assumes that the `ProofABC` structure contains its `a`, `b`, and `c` components in valid formats
/// necessary for transformation and compression.
pub fn negate_proof(proof: ProofABC) -> Result<CompressedProof, PhotonApiError> {
    let mut proof_a_neg = [0u8; 65];

    let proof_a: G1 = G1::deserialize_with_mode(
        &*[&change_endianness(&proof.a), &[0u8][..]].concat(),
        Compress::No,
        Validate::No,
    ).map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to deserialize G1 point: {}", e)))?;

    proof_a
        .neg()
        .x
        .serialize_with_mode(&mut proof_a_neg[..32], Compress::No)
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to serialize x coordinate: {}", e)))?;

    proof_a
        .neg()
        .y
        .serialize_with_mode(&mut proof_a_neg[32..], Compress::No)
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to serialize y coordinate: {}", e)))?;

    let compressed_proof = CompressedProof {
        a: proof_a_neg[0..32].to_vec(),
        b: proof.b[0..64].to_vec(),
        c: proof.c[0..32].to_vec(),
    };

    Ok(compressed_proof)
}

