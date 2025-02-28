mod v1;
mod v2;

pub use v1::{
    get_compressed_account_proof, GetCompressedAccountProofResponse,
    GetCompressedAccountProofResponseValueV1,
};
pub use v2::{
    get_compressed_account_proof_v2, GetCompressedAccountProofResponseV2,
    GetCompressedAccountProofResponseValueV2,
};
