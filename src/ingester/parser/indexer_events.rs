/// Copied from the Light repo. We copy them instead of importing from the Light repo in order
/// to avoid having to import all of Light's dependencies.
use borsh::{BorshDeserialize, BorshSerialize};
use light_compressed_account::Pubkey;
use light_event::event::{AtaOwnerInfo, BatchNullifyContext, NewAddress};

#[derive(Debug, PartialEq, Eq, Default, Clone, BorshSerialize, BorshDeserialize)]
pub struct OutputCompressedAccountWithPackedContext {
    pub compressed_account: CompressedAccount,
    pub merkle_tree_index: u8,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Default, Eq, PartialEq)]
pub struct MerkleTreeSequenceNumberV2 {
    pub tree_pubkey: Pubkey,
    pub queue_pubkey: Pubkey,
    pub tree_type: u64,
    pub seq: u64,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Default, Eq, PartialEq)]
pub struct MerkleTreeSequenceNumberV1 {
    pub tree_pubkey: Pubkey,
    pub seq: u64,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub enum MerkleTreeSequenceNumber {
    V1(MerkleTreeSequenceNumberV1),
    V2(MerkleTreeSequenceNumberV2),
}

impl MerkleTreeSequenceNumber {
    pub fn tree_pubkey(&self) -> Pubkey {
        match self {
            MerkleTreeSequenceNumber::V1(x) => x.tree_pubkey,
            MerkleTreeSequenceNumber::V2(x) => x.tree_pubkey,
        }
    }
    pub fn seq(&self) -> u64 {
        match self {
            MerkleTreeSequenceNumber::V1(x) => x.seq,
            MerkleTreeSequenceNumber::V2(x) => x.seq,
        }
    }
}

/// Current version of PublicTransactionEvent (with ata_owners)
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Default, PartialEq, Eq)]
pub struct PublicTransactionEvent {
    pub input_compressed_account_hashes: Vec<[u8; 32]>,
    pub output_compressed_account_hashes: Vec<[u8; 32]>,
    pub output_compressed_accounts: Vec<OutputCompressedAccountWithPackedContext>,
    pub output_leaf_indices: Vec<u32>,
    pub sequence_numbers: Vec<MerkleTreeSequenceNumberV1>,
    pub relay_fee: Option<u64>,
    pub is_compress: bool,
    pub compress_or_decompress_lamports: Option<u64>,
    pub pubkey_array: Vec<Pubkey>,
    // TODO: remove(data can just be written into a compressed account)
    pub message: Option<Vec<u8>>,
    /// ATA owner info for compressed ATAs (output_index -> wallet_owner_pubkey).
    /// Only populated for compress_and_close operations where is_ata=true.
    pub ata_owners: Vec<AtaOwnerInfo>,
}

/// Legacy version of PublicTransactionEvent (without ata_owners, with compression_lamports)
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Default, PartialEq, Eq)]
pub struct PublicTransactionEventLegacy {
    pub input_compressed_account_hashes: Vec<[u8; 32]>,
    pub output_compressed_account_hashes: Vec<[u8; 32]>,
    pub output_compressed_accounts: Vec<OutputCompressedAccountWithPackedContext>,
    pub output_leaf_indices: Vec<u32>,
    pub sequence_numbers: Vec<MerkleTreeSequenceNumberV1>,
    pub relay_fee: Option<u64>,
    pub is_compress: bool,
    pub compression_lamports: Option<u64>,
    pub pubkey_array: Vec<Pubkey>,
    pub message: Option<Vec<u8>>,
}

impl From<PublicTransactionEventLegacy> for PublicTransactionEvent {
    fn from(legacy: PublicTransactionEventLegacy) -> Self {
        Self {
            input_compressed_account_hashes: legacy.input_compressed_account_hashes,
            output_compressed_account_hashes: legacy.output_compressed_account_hashes,
            output_compressed_accounts: legacy.output_compressed_accounts,
            output_leaf_indices: legacy.output_leaf_indices,
            sequence_numbers: legacy.sequence_numbers,
            relay_fee: legacy.relay_fee,
            is_compress: legacy.is_compress,
            compress_or_decompress_lamports: legacy.compression_lamports,
            pubkey_array: legacy.pubkey_array,
            message: legacy.message,
            ata_owners: Vec::new(),
        }
    }
}

impl PublicTransactionEvent {
    /// Deserialize from bytes, trying the current format first, then falling back to legacy.
    pub fn deserialize_versioned(data: &mut &[u8]) -> Result<Self, std::io::Error> {
        // Clone the slice to try multiple deserializations
        let data_copy = *data;

        // Try current version first
        if let Ok(event) = Self::deserialize(data) {
            return Ok(event);
        }

        // Fall back to legacy version
        *data = data_copy;
        let legacy = PublicTransactionEventLegacy::deserialize(data)?;
        Ok(legacy.into())
    }
}

#[derive(Debug, Clone)]
pub struct BatchPublicTransactionEvent {
    pub event: PublicTransactionEvent,
    pub new_addresses: Vec<NewAddress>,
    pub input_sequence_numbers: Vec<MerkleTreeSequenceNumberV2>,
    pub address_sequence_numbers: Vec<MerkleTreeSequenceNumberV2>,
    pub tx_hash: [u8; 32],
    pub batch_input_accounts: Vec<BatchNullifyContext>,
}

#[derive(Debug, PartialEq, Eq, Default, Clone, BorshSerialize, BorshDeserialize)]
pub struct CompressedAccount {
    pub owner: Pubkey,
    pub lamports: u64,
    pub address: Option<[u8; 32]>,
    pub data: Option<CompressedAccountData>,
}

#[derive(Debug, PartialEq, Eq, Default, Clone, BorshSerialize, BorshDeserialize)]
pub struct CompressedAccountData {
    pub discriminator: [u8; 8],
    pub data: Vec<u8>,
    pub data_hash: [u8; 32],
}

/// Event containing the Merkle path of the given
/// [`StateMerkleTree`](light_merkle_tree_program::state::StateMerkleTree)
/// change. Indexers can use this type of events to re-build a non-sparse
/// version of the state Merkle tree.
#[derive(BorshDeserialize, BorshSerialize, Clone, Eq, PartialEq, Debug)]
#[repr(C)]
pub enum MerkleTreeEvent {
    V1(ChangelogEvent),
    V2(NullifierEvent),
    V3(IndexedMerkleTreeEvent),
    BatchAppend(BatchEvent),
    BatchNullify(BatchEvent),
    BatchAddressAppend(BatchEvent),
}

/// Node of the Merkle path with an index representing the position in a
/// non-sparse Merkle tree.
#[derive(BorshDeserialize, BorshSerialize, Clone, Debug, Eq, PartialEq)]
pub struct PathNode {
    pub node: [u8; 32],
    pub index: u32,
}

/// Version 1 of the [`ChangelogEvent`](light_merkle_tree_program::state::ChangelogEvent).
#[derive(BorshDeserialize, BorshSerialize, PartialEq, Eq, Clone, Debug)]
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

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Eq, Clone, Debug)]
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

#[derive(Debug, Default, Clone, Copy, BorshSerialize, BorshDeserialize, Eq, PartialEq)]
pub struct RawIndexedElement {
    pub value: [u8; 32],
    pub next_index: usize,
    pub next_value: [u8; 32],
    pub index: usize,
}

#[derive(BorshDeserialize, BorshSerialize, PartialEq, Eq, Debug, Clone)]
pub struct IndexedMerkleTreeUpdate {
    pub new_low_element: RawIndexedElement,
    /// Leaf hash in new_low_element.index.
    pub new_low_element_hash: [u8; 32],
    pub new_high_element: RawIndexedElement,
    /// Leaf hash in new_high_element.index,
    /// is equivalent with next_index.
    pub new_high_element_hash: [u8; 32],
}

#[derive(BorshDeserialize, BorshSerialize, Clone, PartialEq, Eq, Debug)]
pub struct IndexedMerkleTreeEvent {
    /// Public key of the tree.
    pub id: [u8; 32],
    pub updates: Vec<IndexedMerkleTreeUpdate>,
    /// Number of successful operations on the on-chain tree.
    /// seq corresponds to leaves[0].
    /// seq + 1 corresponds to leaves[1].
    pub seq: u64,
}

#[repr(C)]
#[derive(BorshDeserialize, BorshSerialize, Debug, PartialEq, Clone, Eq)]
pub struct BatchEvent {
    pub merkle_tree_pubkey: [u8; 32],
    pub batch_index: u64,
    pub zkp_batch_index: u64,
    pub zkp_batch_size: u64,
    pub old_next_index: u64,
    pub new_next_index: u64,
    pub new_root: [u8; 32],
    pub root_index: u32,
    pub sequence_number: u64,
    pub output_queue_pubkey: Option<[u8; 32]>,
}
