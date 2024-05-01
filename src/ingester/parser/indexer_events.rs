/// Copied from the Light repo. We copy them instead of importing from the Light repo in order to
/// to avoid having to import all of Light's dependencies.
use anchor_lang::prelude::*;

#[derive(Debug, PartialEq, Default, Clone, AnchorSerialize, AnchorDeserialize)]
pub struct OutputCompressedAccountWithPackedContext {
    pub compressed_account: CompressedAccount,
    pub merkle_tree_index: u8,
}

#[derive(Debug, Clone, AnchorSerialize, AnchorDeserialize, Default, PartialEq)]
pub struct MerkleTreeSequenceNumber {
    pub pubkey: Pubkey,
    pub seq: u64,
}

#[derive(Debug, Clone, AnchorSerialize, AnchorDeserialize, Default, PartialEq)]
pub struct PublicTransactionEvent {
    pub input_compressed_account_hashes: Vec<[u8; 32]>,
    pub output_compressed_account_hashes: Vec<[u8; 32]>,
    pub output_compressed_accounts: Vec<OutputCompressedAccountWithPackedContext>,
    pub output_leaf_indices: Vec<u32>,
    pub sequence_numbers: Vec<MerkleTreeSequenceNumber>,
    pub relay_fee: Option<u64>,
    pub is_compress: bool,
    pub compression_lamports: Option<u64>,
    pub pubkey_array: Vec<Pubkey>,
    // TODO: remove(data can just be written into a compressed account)
    pub message: Option<Vec<u8>>,
}



#[derive(Debug, PartialEq, Default, Clone, AnchorSerialize, AnchorDeserialize)]
pub struct CompressedAccount {
    pub owner: Pubkey,
    pub lamports: u64,
    pub address: Option<[u8; 32]>,
    pub data: Option<CompressedAccountData>,
}

#[derive(Debug, PartialEq, Default, Clone, AnchorSerialize, AnchorDeserialize)]
pub struct CompressedAccountData {
    pub discriminator: [u8; 8],
    pub data: Vec<u8>,
    pub data_hash: [u8; 32],
}

/// Event containing the Merkle path of the given
/// [`StateMerkleTree`](light_merkle_tree_program::state::StateMerkleTree)
/// change. Indexers can use this type of events to re-build a non-sparse
/// version of state Merkle tree.
#[derive(AnchorDeserialize, AnchorSerialize, Debug)]
#[repr(C)]
pub enum MerkleTreeEvent {
    V1(ChangelogEvent),
    V2(NullifierEvent),
    V3(IndexedMerkleTreeEvent),
}

/// Node of the Merkle path with an index representing the position in a
/// non-sparse Merkle tree.
#[derive(AnchorDeserialize, AnchorSerialize, Debug, Eq, PartialEq)]
pub struct PathNode {
    pub node: [u8; 32],
    pub index: u32,
}

/// Version 1 of the [`ChangelogEvent`](light_merkle_tree_program::state::ChangelogEvent).
#[derive(AnchorDeserialize, AnchorSerialize, Debug)]
pub struct ChangelogEvent {
    /// Public key of the tree.
    pub id: [u8; 32],
    // Merkle paths.
    pub paths: Vec<Vec<PathNode>>,
    /// Number of successful operations on the on-chain tree.
    pub seq: u64,
    /// Changelog event index.
    pub index: u32,
}

#[derive(AnchorSerialize, AnchorDeserialize, Debug)]
pub struct NullifierEvent {
    /// Public key of the tree.
    pub id: [u8; 32],
    /// Indices of leaves that were nullified.
    /// Nullified means updated with [0u8;32].
    pub nullified_leaves_indices: Vec<u64>,
    /// Number of successful operations on the on-chain tree.
    /// seq corresponds to leaves[0].
    /// seq + 1 corresponds to leaves[1].
    pub seq: u64,
}

#[derive(Debug, Default, Clone, Copy, AnchorSerialize, AnchorDeserialize, Eq, PartialEq)]
pub struct RawIndexedElement {
    pub value: [u8; 32],
    pub next_index: usize,
    pub next_value: [u8; 32],
    pub index: usize,
}

#[derive(AnchorDeserialize, AnchorSerialize, Debug, Clone)]
pub struct IndexedMerkleTreeUpdate {
    pub new_low_element: RawIndexedElement,
    /// Leaf hash in new_low_element.index.
    pub new_low_element_hash: [u8; 32],
    pub new_high_element: RawIndexedElement,
    /// Leaf hash in new_high_element.index,
    /// is equivalent with next_index.
    pub new_high_element_hash: [u8; 32],
}

#[derive(AnchorDeserialize, AnchorSerialize, Debug)]
pub struct IndexedMerkleTreeEvent {
    /// Public key of the tree.
    pub id: [u8; 32],
    pub updates: Vec<IndexedMerkleTreeUpdate>,
    /// Number of successful operations on the on-chain tree.
    /// seq corresponds to leaves[0].
    /// seq + 1 corresponds to leaves[1].
    pub seq: u64,
}
