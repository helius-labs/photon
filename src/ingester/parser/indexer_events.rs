/// Copied from the Light repo. We copy them instead of importing from the Light repo in order to
/// to avoid having to import all of Light's dependencies.
use anchor_lang::prelude::*;
use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, Clone, AnchorSerialize, AnchorDeserialize)]
pub struct PublicTransactionEvent {
    pub input_compressed_account_hashes: Vec<[u8; 32]>,
    pub output_compressed_account_hashes: Vec<[u8; 32]>,
    pub input_compressed_accounts: Vec<CompressedAccountWithMerkleContext>,
    pub output_compressed_accounts: Vec<CompressedAccount>,
    // index of Merkle tree account in remaining accounts
    pub output_state_merkle_tree_account_indices: Vec<u8>,
    pub output_leaf_indices: Vec<u32>,
    pub relay_fee: Option<u64>,
    pub is_compress: bool,
    pub de_compress_amount: Option<u64>,
    pub pubkey_array: Vec<Pubkey>,
    pub message: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq, Default, Clone, AnchorSerialize, AnchorDeserialize)]
pub struct CompressedAccountWithMerkleContext {
    pub compressed_account: CompressedAccount,
    pub merkle_tree_pubkey_index: u8,
    pub nullifier_queue_pubkey_index: u8,
    pub leaf_index: u32,
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

#[derive(BorshDeserialize, BorshSerialize, Debug)]
pub struct Changelogs {
    pub changelogs: Vec<ChangelogEvent>,
}

/// Event containing the Merkle path of the given
/// [`StateMerkleTree`](light_merkle_tree_program::state::StateMerkleTree)
/// change. Indexers can use this type of events to re-build a non-sparse
/// version of state Merkle tree.
#[derive(BorshDeserialize, BorshSerialize, Debug)]
#[repr(C)]
pub enum ChangelogEvent {
    V1(ChangelogEventV1),
}

/// Node of the Merkle path with an index representing the position in a
/// non-sparse Merkle tree.
#[derive(BorshDeserialize, BorshSerialize, Debug, Clone)]
pub struct PathNode {
    pub node: [u8; 32],
    pub index: u32,
}

/// Version 1 of the [`ChangelogEvent`](light_merkle_tree_program::state::ChangelogEvent).
#[derive(BorshDeserialize, BorshSerialize, Debug)]
pub struct ChangelogEventV1 {
    /// Public key of the tree.
    pub id: [u8; 32],
    // Merkle paths.
    pub paths: Vec<Vec<PathNode>>,
    /// Number of successful operations on the on-chain tree.
    pub seq: u64,
    /// Changelog event index.
    pub index: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, AnchorSerialize, AnchorDeserialize)]
#[repr(u8)]
pub enum AccountState {
    Uninitialized,
    Initialized,
    Frozen,
}

#[derive(Debug, PartialEq, Eq, AnchorSerialize, AnchorDeserialize, Clone, Copy)]
pub struct TokenData {
    /// The mint associated with this account
    pub mint: Pubkey,
    /// The owner of this account.
    pub owner: Pubkey,
    /// The amount of tokens this account holds.
    pub amount: u64,
    /// If `delegate` is `Some` then `delegated_amount` represents
    /// the amount authorized by the delegate
    pub delegate: Option<Pubkey>,
    /// The account's state
    pub state: AccountState,
    /// If is_some, this is a native token, and the value logs the rent-exempt
    /// reserve. An Account is required to be rent-exempt, so the value is
    /// used by the Processor to ensure that wrapped SOL accounts do not
    /// drop below this threshold.
    pub is_native: Option<u64>,
    /// The amount delegated
    pub delegated_amount: u64, // TODO: make instruction data optional
                               // TODO: validate that we don't need close authority
                               // /// Optional authority to close the account.
                               // pub close_authority: Option<Pubkey>,
}