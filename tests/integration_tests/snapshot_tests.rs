use crate::utils::*;
use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use function_name::named;
use futures::stream;
use photon_indexer::api::method::get_compressed_accounts_by_owner::{
    DataSlice, FilterSelector, GetCompressedAccountsByOwnerRequest, Memcmp,
};
use photon_indexer::api::method::get_compressed_balance_by_owner::GetCompressedBalanceByOwnerRequest;
use photon_indexer::api::method::get_compressed_token_balances_by_owner::GetCompressedTokenBalancesByOwnerRequest;
use photon_indexer::api::method::get_multiple_compressed_accounts::GetMultipleCompressedAccountsRequest;
use photon_indexer::api::method::get_validity_proof::{
    get_validity_proof, GetValidityProofRequest,
};
use photon_indexer::api::method::utils::{
    CompressedAccountRequest, GetCompressedTokenAccountsByDelegate,
    GetCompressedTokenAccountsByOwner,
};
use photon_indexer::common::typedefs::bs58_string::Base58String;
use photon_indexer::ingester::persist::persisted_indexed_merkle_tree::get_exclusion_range_with_proof;

use photon_indexer::common::typedefs::unsigned_integer::UnsignedInteger;
use photon_indexer::dao::generated::indexed_trees;
use photon_indexer::ingester::persist::persisted_indexed_merkle_tree::multi_append;
use photon_indexer::ingester::persist::persisted_state_tree::{
    get_multiple_compressed_leaf_proofs, ZERO_BYTES,
};
use sea_orm::TransactionTrait;

use photon_indexer::common::typedefs::account::Account;
use photon_indexer::common::typedefs::bs64_string::Base64String;
use photon_indexer::common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey};
use photon_indexer::dao::generated::accounts;
use photon_indexer::ingester::index_block;
use photon_indexer::ingester::parser::state_update::StateUpdate;
use photon_indexer::ingester::persist::persisted_state_tree::{persist_leaf_nodes, LeafNode};
use photon_indexer::ingester::persist::{
    compute_parent_hash, persist_token_accounts, EnrichedTokenAccount,
};

use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use sea_orm::{EntityTrait, Set};
use serial_test::serial;

use photon_indexer::common::typedefs::account::AccountData;
use std::collections::{HashMap, HashSet};

use photon_indexer::common::typedefs::token_data::{AccountState, TokenData};
use sqlx::types::Decimal;

use photon_indexer::api::method::utils::Limit;
use solana_sdk::pubkey::Pubkey;
use std::vec;

#[tokio::test]
async fn test_basic_snapshotting() {
    use std::{env::temp_dir, fs};

    use futures::StreamExt;
    use photon_indexer::ingester::snapshotter::{
        load_block_stream_from_snapshot_directory, update_snapshot_helper,
    };

    let blocks: Vec<BlockInfo> = (0..10)
        .map(|i| {
            let block = BlockInfo {
                metadata: BlockMetadata {
                    slot: i,
                    parent_slot: if i == 0 { 0 } else { i - 1 },
                    block_time: 0,
                    blockhash: Hash::default(),
                    parent_blockhash: Hash::default(),
                    block_height: i,
                },
                transactions: vec![],
            };
            block
        })
        .collect();
    let blocks_stream = stream::iter(blocks.clone().into_iter());
    let temp_dir = temp_dir();
    let snapshot_dir = temp_dir.as_path().join("test-snapshots");
    if snapshot_dir.exists() {
        fs::remove_dir_all(&snapshot_dir).unwrap();
    } else {
        fs::create_dir(&snapshot_dir).unwrap();
    }
    update_snapshot_helper(blocks_stream, 0, 2, 4, &snapshot_dir).await;
    let snapshot_blocks = load_block_stream_from_snapshot_directory(&snapshot_dir);
    let snapshot_blocks: Vec<BlockInfo> = snapshot_blocks.collect().await;
    assert_eq!(snapshot_blocks, blocks);
}
