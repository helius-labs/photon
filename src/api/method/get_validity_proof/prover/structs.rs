use crate::ingester::parser::tree_info::TreeInfo;
use jsonrpsee_core::Serialize;
use num_traits::identities::Zero;
use serde::Deserialize;
use utoipa::ToSchema;

#[derive(Debug, Clone)]
pub(crate) struct AccountProofDetail {
    pub hash: String,
    pub root: String,
    pub root_index_mod_queue: u64,
    pub leaf_index: u32,
    pub merkle_tree_id: String,
    pub tree_info: TreeInfo,
}

#[derive(Debug, Clone)]
pub(crate) struct AddressProofDetail {
    pub address: String,
    pub root: String,
    pub root_index_mod_queue: u64,
    pub path_index: u32,
    pub merkle_tree_id: String,
    pub tree_info: TreeInfo,
}

#[derive(Debug, Clone)]
pub(crate) struct ProverResult {
    pub compressed_proof: CompressedProof,
    pub account_proof_details: Vec<AccountProofDetail>,
    pub address_proof_details: Vec<AddressProofDetail>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InclusionHexInputsForProver {
    pub root: String,
    pub path_index: u32,
    pub path_elements: Vec<String>,
    pub leaf: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct NonInclusionHexInputsForProver {
    pub root: String,
    pub value: String,
    pub path_index: u32,
    pub path_elements: Vec<String>,
    pub leaf_lower_range_value: String,
    pub leaf_higher_range_value: String,
    pub next_index: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct GnarkProofJson {
    pub ar: [String; 2],
    pub bs: [[String; 2]; 2],
    pub krs: [String; 2],
}

#[derive(Debug)]
pub(crate) struct ProofABC {
    pub a: [u8; 64],
    pub b: [u8; 128],
    pub c: [u8; 64],
}

#[derive(Serialize, Deserialize, Default, ToSchema, Debug, Clone)]
pub struct CompressedProof {
    pub a: Vec<u8>,
    pub b: Vec<u8>,
    pub c: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HexBatchInputsForProver {
    #[serde(rename = "circuitType")]
    pub circuit_type: String,
    #[serde(rename = "stateTreeHeight", skip_serializing_if = "u32::is_zero")]
    pub state_tree_height: u32,
    #[serde(rename = "addressTreeHeight", skip_serializing_if = "u32::is_zero")]
    pub address_tree_height: u32,
    #[serde(rename = "publicInputHash", skip_serializing_if = "String::is_empty")]
    pub public_input_hash: String,
    #[serde(
        rename = "inputCompressedAccounts",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub input_compressed_accounts: Vec<InclusionHexInputsForProver>,
    #[serde(rename = "newAddresses", skip_serializing_if = "Vec::is_empty")]
    pub new_addresses: Vec<NonInclusionHexInputsForProver>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CircuitType {
    Combined,
    Inclusion,
    NonInclusion,
}

impl CircuitType {
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        match self {
            Self::Combined => "combined".to_string(),
            Self::Inclusion => "inclusion".to_string(),
            Self::NonInclusion => "non-inclusion".to_string(),
        }
    }
}
