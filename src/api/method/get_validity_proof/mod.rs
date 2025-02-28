mod common;
mod v1;
mod v2;

pub use common::{
    CompressedProof, CompressedProofWithContext, CompressedProofWithContextV2, ContextInfo,
    GetValidityProofRequest, GetValidityProofRequestDocumentation, GetValidityProofRequestV2,
    GetValidityProofResponse, GetValidityProofResponseV2, MerkleContextV2, RootIndex,
};
pub use v1::get_validity_proof;
pub use v2::get_validity_proof_v2;
