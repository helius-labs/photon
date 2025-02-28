use crate::api::method::get_multiple_new_address_proofs::{
    AddressWithTree, MerkleContextWithNewAddressProof,
};
use crate::common::typedefs::context::Context;
use crate::common::typedefs::hash::Hash;
use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::MerkleProofWithContext;
use borsh::BorshSerialize;
use jsonrpsee_core::Serialize;
use lazy_static::lazy_static;
use light_compressed_account::hash_chain::create_two_inputs_hash_chain;
use num_bigint::BigUint;
use num_traits::identities::Zero;
use serde::Deserialize;
use std::str::FromStr;
use utoipa::ToSchema;

lazy_static! {
    pub static ref FIELD_SIZE: BigUint = BigUint::from_str(
        "21888242871839275222246405745257275088548364400416034343698204186575808495616"
    )
    .unwrap();
}

pub const STATE_TREE_QUEUE_SIZE: u64 = 2400;

#[derive(Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct GetValidityProofRequest {
    #[serde(default)]
    pub hashes: Vec<Hash>,
    #[serde(default)]
    #[schema(deprecated = true)]
    pub newAddresses: Vec<SerializablePubkey>,
    #[serde(default)]
    pub newAddressesWithTrees: Vec<AddressWithTree>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct GetValidityProofRequestV2 {
    #[serde(default)]
    pub hashes: Vec<Hash>,
    #[serde(default)]
    pub newAddressesWithTrees: Vec<AddressWithTree>,
}

impl From<GetValidityProofRequestV2> for GetValidityProofRequest {
    fn from(value: GetValidityProofRequestV2) -> Self {
        GetValidityProofRequest {
            hashes: value.hashes,
            newAddresses: vec![],
            newAddressesWithTrees: value.newAddressesWithTrees,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[allow(non_snake_case)]
// Create to hide the deprecated newAddresses field from the documentation
pub struct GetValidityProofRequestDocumentation {
    #[serde(default)]
    pub hashes: Vec<Hash>,
    #[serde(default)]
    pub newAddressesWithTrees: Vec<AddressWithTree>,
}

#[derive(Serialize, Deserialize, Default, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetValidityProofResponse {
    pub value: CompressedProofWithContext,
    pub context: Context,
}

#[derive(Serialize, Deserialize, Default, ToSchema, Debug)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetValidityProofResponseV2 {
    pub value: CompressedProofWithContextV2,
    pub context: Context,
}

impl From<GetValidityProofResponse> for GetValidityProofResponseV2 {
    fn from(response: GetValidityProofResponse) -> Self {
        GetValidityProofResponseV2 {
            value: CompressedProofWithContextV2 {
                compressedProof: Some(response.value.compressedProof),
                roots: response.value.roots,
                rootIndices: response
                    .value
                    .rootIndices
                    .into_iter()
                    .map(|x| RootIndex {
                        root_index: x,
                        prove_by_index: false,
                    })
                    .collect(),
                leafIndices: response.value.leafIndices,
                leaves: response.value.leaves,
                merkle_context: response
                    .value
                    .merkleTrees
                    .iter()
                    .map(|tree| {
                        let tree_info = TreeInfo::get(tree.as_str()).unwrap(); // TODO: remove unwrap
                        println!("tree_info: {:?}", tree_info);
                        MerkleContextV2 {
                            tree_type: tree_info.tree_type as u16,
                            tree: SerializablePubkey::from(tree_info.tree),
                            queue: SerializablePubkey::from(tree_info.queue),
                            cpi_context: None,
                            next_context: None,
                        }
                    })
                    .collect(),
            },
            context: response.context,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InclusionHexInputsForProver {
    root: String,
    path_index: u32,
    path_elements: Vec<String>,
    leaf: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NonInclusionHexInputsForProver {
    root: String,
    value: String,
    path_index: u32,
    path_elements: Vec<String>,
    leaf_lower_range_value: String,
    leaf_higher_range_value: String,
    next_index: u32,
}

pub fn convert_non_inclusion_merkle_proof_to_hex(
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
                .map(hash_to_hex)
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

pub fn convert_inclusion_proofs_to_hex(
    inclusion_proof_inputs: Vec<MerkleProofWithContext>,
) -> Vec<InclusionHexInputsForProver> {
    let mut inputs: Vec<InclusionHexInputsForProver> = Vec::new();
    for i in 0..inclusion_proof_inputs.len() {
        let input = InclusionHexInputsForProver {
            root: hash_to_hex(&inclusion_proof_inputs[i].root),
            path_index: inclusion_proof_inputs[i].leaf_index,
            path_elements: inclusion_proof_inputs[i]
                .proof
                .iter()
                .map(hash_to_hex)
                .collect(),
            leaf: hash_to_hex(&inclusion_proof_inputs[i].hash),
        };
        inputs.push(input);
    }
    inputs
}

pub fn hash_to_hex(hash: &Hash) -> String {
    let bytes = hash.to_vec();
    let hex = hex::encode(bytes);
    format!("0x{}", hex)
}

fn pubkey_to_hex(pubkey: &SerializablePubkey) -> String {
    let bytes = pubkey.to_bytes_vec();
    let hex = hex::encode(bytes);
    format!("0x{}", hex)
}

#[derive(Serialize, Deserialize, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct CompressedProofWithContext {
    pub compressedProof: CompressedProof,
    pub roots: Vec<String>,
    pub rootIndices: Vec<u64>,
    pub leafIndices: Vec<u32>,
    pub leaves: Vec<String>,
    pub merkleTrees: Vec<String>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Default)]
#[serde(rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct RootIndex {
    pub root_index: u64,
    // if prove_by_index is true, ignore root_index and use 0
    pub prove_by_index: bool,
}

impl From<RootIndex> for Option<u64> {
    fn from(val: RootIndex) -> Option<u64> {
        match val.prove_by_index {
            true => None,
            false => Some(val.root_index),
        }
    }
}

impl From<Option<u64>> for RootIndex {
    fn from(val: Option<u64>) -> RootIndex {
        match val {
            Some(root_index) => RootIndex {
                root_index,
                prove_by_index: false,
            },
            None => RootIndex {
                root_index: 0,
                prove_by_index: true,
            },
        }
    }
}

// TODO: Keep in here for API doc generation?
#[repr(u16)]
#[derive(Serialize, Deserialize, ToSchema, Debug, PartialEq, Clone, Copy, Eq)]
pub enum SerializableTreeType {
    State = 1,
    Address = 2,
    BatchedState = 3,
    BatchedAddress = 4,
    Unknown = 0, // TODO: remove this
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct MerkleContextV2 {
    pub tree_type: u16,
    pub tree: SerializablePubkey,
    // nullifier_queue in legacy trees, output_queue in V2 trees.
    pub queue: SerializablePubkey,
    pub cpi_context: Option<SerializablePubkey>,
    pub next_context: Option<ContextInfo>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Default, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct ContextInfo {
    pub tree_type: u16,
    pub merkle_tree: SerializablePubkey,
    pub queue: SerializablePubkey,
    pub cpi_context: Option<SerializablePubkey>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Default)]
#[serde(rename_all = "camelCase")]
#[allow(non_snake_case)]
pub struct CompressedProofWithContextV2 {
    pub compressedProof: Option<CompressedProof>,
    pub roots: Vec<String>,
    pub rootIndices: Vec<RootIndex>,
    pub leafIndices: Vec<u32>,
    pub leaves: Vec<String>,
    pub merkle_context: Vec<MerkleContextV2>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GnarkProofJson {
    ar: [String; 2],
    bs: [[String; 2]; 2],
    krs: [String; 2],
}

#[derive(Debug)]
pub struct ProofABC {
    a: Vec<u8>,
    b: Vec<u8>,
    c: Vec<u8>,
}

#[derive(Serialize, Deserialize, ToSchema, Default, Debug)]
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

pub fn proof_from_json_struct(json: GnarkProofJson) -> ProofABC {
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

pub fn negate_and_compress_proof(proof: ProofABC) -> CompressedProof {
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

pub fn get_public_input_hash(
    account_proofs: &[MerkleProofWithContext],
    new_address_proofs: &[MerkleContextWithNewAddressProof],
) -> [u8; 32] {
    let account_hashes: Vec<[u8; 32]> = account_proofs
        .iter()
        .map(|x| x.hash.to_vec().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let account_roots: Vec<[u8; 32]> = account_proofs
        .iter()
        .map(|x| x.root.to_vec().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let inclusion_hash_chain: [u8; 32] =
        create_two_inputs_hash_chain(&account_roots, &account_hashes).unwrap();
    let new_address_hashes: Vec<[u8; 32]> = new_address_proofs
        .iter()
        .map(|x| x.address.try_to_vec().unwrap().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let new_address_roots: Vec<[u8; 32]> = new_address_proofs
        .iter()
        .map(|x| x.root.to_vec().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let non_inclusion_hash_chain =
        create_two_inputs_hash_chain(&new_address_roots, &new_address_hashes).unwrap();
    if non_inclusion_hash_chain != [0u8; 32] {
        non_inclusion_hash_chain
    } else if inclusion_hash_chain != [0u8; 32] {
        inclusion_hash_chain
    } else {
        create_two_inputs_hash_chain(&[inclusion_hash_chain], &[non_inclusion_hash_chain]).unwrap()
    }
}
