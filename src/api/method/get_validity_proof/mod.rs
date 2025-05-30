mod prover;
mod v1;
mod v2;

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
