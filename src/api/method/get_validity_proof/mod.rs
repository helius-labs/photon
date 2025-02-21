mod common;
mod v1;
mod v2;

pub use common::{
    CompressedProof, CompressedProofWithContext, GetValidityProofRequest,
    GetValidityProofRequestDocumentation, GetValidityProofResponse, GetValidityProofResponseV2,
};
pub use v1::get_validity_proof;
pub use v2::get_validity_proof_v2;
