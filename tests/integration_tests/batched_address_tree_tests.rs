use crate::utils::*;
use borsh::BorshSerialize;
use function_name::named;
use light_compressed_account::QueueType; // Correct QueueType
use light_hasher::poseidon::Poseidon;
use light_hasher::zero_bytes::poseidon::ZERO_BYTES; // Use actual ZERO_BYTES
use light_merkle_tree_reference::MerkleTree;
use photon_indexer::api::method::get_multiple_compressed_account_proofs::HashList;
use photon_indexer::api::method::get_queue_elements::GetQueueElementsRequest;
// ... other imports ... (ensure all necessary imports from previous version are present)
use photon_indexer::common::typedefs::hash::Hash;
use photon_indexer::common::typedefs::serializable_pubkey::SerializablePubkey;
use photon_indexer::ingester::index_block;
use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use sea_orm::DatabaseConnection;
use serial_test::serial;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

