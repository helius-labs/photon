//! V1 Tree Account structures for deserializing account-compression program accounts
//! These replace the dependency on the account-compression crate.

use borsh::BorshDeserialize;
use light_concurrent_merkle_tree::zero_copy::ConcurrentMerkleTreeZeroCopy;
use light_hasher::Poseidon;
use light_indexed_merkle_tree::zero_copy::IndexedMerkleTreeZeroCopy;
use light_merkle_tree_metadata::merkle_tree::MerkleTreeMetadata;

/// StateMerkleTreeAccount discriminator
pub const STATE_MERKLE_TREE_DISCRIMINATOR: [u8; 8] = [172, 43, 172, 186, 29, 73, 219, 84];

/// AddressMerkleTreeAccount discriminator
pub const ADDRESS_MERKLE_TREE_DISCRIMINATOR: [u8; 8] = [99, 100, 84, 45, 134, 159, 103, 73];

/// V1 State Merkle Tree Account structure (for deserialization)
/// The on-chain layout is: [8 byte discriminator][metadata][tree_bytes]
#[derive(Debug, BorshDeserialize)]
pub struct StateMerkleTreeAccount {
    pub metadata: MerkleTreeMetadata,
    /// Raw bytes containing the ConcurrentMerkleTree data
    pub tree_bytes: Vec<u8>,
}

impl StateMerkleTreeAccount {
    /// Get a zero-copy view of the merkle tree
    pub fn tree(&self) -> Result<ConcurrentMerkleTreeZeroCopy<'_, Poseidon, 26>, std::io::Error> {
        ConcurrentMerkleTreeZeroCopy::from_bytes_zero_copy(&self.tree_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }
}

/// V1 Address Merkle Tree Account structure (for deserialization)
/// The on-chain layout is: [8 byte discriminator][metadata][tree_bytes]
#[derive(Debug, BorshDeserialize)]
pub struct AddressMerkleTreeAccount {
    pub metadata: MerkleTreeMetadata,
    /// Raw bytes containing the IndexedMerkleTree data
    pub tree_bytes: Vec<u8>,
}

impl AddressMerkleTreeAccount {
    /// Get a zero-copy view of the indexed merkle tree
    pub fn tree(
        &self,
    ) -> Result<IndexedMerkleTreeZeroCopy<'_, Poseidon, usize, 26, 16>, std::io::Error> {
        IndexedMerkleTreeZeroCopy::from_bytes_zero_copy(&self.tree_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }
}

/// Check that account discriminator matches expected value
pub fn check_discriminator(data: &[u8], expected: &[u8; 8]) -> Result<(), std::io::Error> {
    if data.len() < 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Account data too short for discriminator",
        ));
    }
    if &data[..8] != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid account discriminator",
        ));
    }
    Ok(())
}
