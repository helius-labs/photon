use crate::utils::*;
use ::borsh::{to_vec, BorshDeserialize, BorshSerialize};
use function_name::named;
use insta::assert_json_snapshot;
use photon_indexer::api::method::get_compressed_accounts_by_owner::{
    DataSlice, FilterSelector, GetCompressedAccountsByOwnerRequest, Memcmp,
};
use photon_indexer::api::method::get_validity_proof::{
    get_validity_proof, GetValidityProofRequest,
};
use photon_indexer::api::method::utils::{
    CompressedAccountRequest, GetCompressedTokenAccountsByDelegate,
    GetCompressedTokenAccountsByOwner,
};
use photon_indexer::common::typedefs::bs58_string::Base58String;
use photon_indexer::ingester::persist::persisted_indexed_merkle_tree::{
    get_exclusion_range_with_proof, validate_tree,
};

use photon_indexer::common::typedefs::unsigned_integer::UnsignedInteger;
use photon_indexer::dao::generated::{indexed_trees, state_trees};
use photon_indexer::ingester::persist::persisted_indexed_merkle_tree::multi_append;
use photon_indexer::ingester::persist::persisted_state_tree::{
    get_multiple_compressed_leaf_proofs, ZERO_BYTES,
};
use sea_orm::{QueryFilter, TransactionTrait};

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
use solana_sdk::account::Account as SolanaAccount;
use std::collections::{HashMap, HashSet};

use photon_indexer::common::typedefs::token_data::{AccountState, TokenData};
use sqlx::types::Decimal;

use photon_indexer::api::method::utils::Limit;
use sea_orm::ColumnTrait;
use solana_sdk::pubkey::Pubkey;
use std::vec;

async fn parse_tree_account(account: SolanaAccount) -> Hash {
    
}

fn are_tree_roots_in_sync() -> bool {
    todo!()
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_validate_tree_roots(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup(name, db_backend).await;
    let tree_pubkey = Pubkey::default();
    let account = cached_fetch_account(&setup, tree_pubkey).await;
    let hash = parse_tree_account(account).await;
    assert_json_snapshot!(hash);
}
