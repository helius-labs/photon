use function_name::named;
use itertools::Itertools;
use photon_indexer::api::method::get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest;
use photon_indexer::dao::typedefs::serializable_pubkey::SerializablePubkey;
use photon_indexer::ingester::index_block;

use crate::utils::*;
use insta::assert_json_snapshot;
use photon_indexer::api::method::utils::GetCompressedTokenAccountsByAuthority;
use photon_indexer::dao::generated::blocks;
use photon_indexer::dao::typedefs::hash::Hash;
use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use sea_orm::ColumnTrait;
use sea_orm::EntityTrait;
use sea_orm::QueryFilter;
use serial_test::serial;

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_e2e_mint_and_transfer(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Localnet,
            db_backend,
        },
    )
    .await;

    // HACK: We index a block so that API methods can fetch the current slot.
    index_block(
        &setup.db_conn,
        &BlockInfo {
            metadata: BlockMetadata {
                slot: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bob_pubkey =
        SerializablePubkey::try_from("8EYKVyNCsDFHkxos7V4kr8bMouYU2nPJ1QXk2ET8FBc7").unwrap();
    let charles_pubkey =
        SerializablePubkey::try_from("FjLHdH44f8uN3kxrnxEuuLyLqeR7mp6jZ4d8NT3bk5os").unwrap();

    let mint_tx =
        "2QN6Z3AiG73zWw8KgLH9mL1emTHD1cKypzD62SBSDZzLH2UhuHsPsmzeDB9uXyR2qhxbNS6WFCF11TPFgTUux6H2";
    let transfer_tx =
        "46VP2T7zPwcFJzU3HDjCh5PWwxKP9LahgGbPQcsePXUwhFrFa2wHCEh4kDu4KCv18BtCw5RxauJt832DmbF4dUPt";
    let txs = [mint_tx, transfer_tx];

    for tx_permutation in txs.iter().permutations(txs.len()) {
        for tx in tx_permutation {
            index_transaction(&setup, tx).await;
        }
        for (person, pubkey) in [
            ("bob", bob_pubkey.clone()),
            ("charles", charles_pubkey.clone()),
        ] {
            let accounts = setup
                .api
                .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByAuthority(
                    pubkey, None,
                ))
                .await
                .unwrap();
            assert_json_snapshot!(format!("{}-{}-accounts", name.clone(), person), accounts);
            let proofs = setup
                .api
                .get_multiple_compressed_account_proofs(
                    accounts
                        .value
                        .items
                        .iter()
                        .map(|x| x.hash.clone())
                        .collect(),
                )
                .await
                .unwrap();
            assert_json_snapshot!(format!("{}-{}-proofs", name.clone(), person), proofs);
        }
    }
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_e2e_lamport_transfer(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Localnet,
            db_backend,
        },
    )
    .await;

    // HACK: We index a block so that API methods can fetch the current slot.
    index_block(
        &setup.db_conn,
        &BlockInfo {
            metadata: BlockMetadata {
                slot: 0,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let bob_pubkey =
        SerializablePubkey::try_from("H9umPt6hyzWn5pXDRMzy3ZuJXDqVMTK4Dvps4qY5XeTX").unwrap();

    let transfer_tx =
        "4iXfgDbgAEQU8hcto67feZsNHxFGm3JKKo97zDgnT84VAUTTjiSCEyi1ZSMFdiybS5XmRHy3nW1qDtKkTPEBpsV9";

    index_transaction(&setup, transfer_tx).await;
    let accounts = setup
        .api
        .get_compressed_accounts_by_owner(GetCompressedAccountsByOwnerRequest(bob_pubkey, None))
        .await
        .unwrap();
    assert_json_snapshot!(name.clone(), accounts);
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_index_block_metadata(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Mainnet,
            db_backend,
        },
    )
    .await;

    let slot = 254170887;
    let block = cached_fetch_block(&setup, slot).await;
    index_block(&setup.db_conn, &block).await.unwrap();
    let filter = blocks::Column::Slot.eq(block.metadata.slot);

    let block_model = blocks::Entity::find()
        .filter(filter)
        .one(setup.db_conn.as_ref())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(block_model.slot, 254170887);
    assert_eq!(block_model.parent_slot, 254170886);
    assert_eq!(
        Hash::try_from(block_model.parent_blockhash)
            .unwrap()
            .to_string(),
        "9BMHTdybcGah8PWtCzu8tVFDBXmiEHZZDf3FaZ651Nf"
    );
    assert_eq!(
        Hash::try_from(block_model.blockhash).unwrap().to_string(),
        "5GG5pzTbH6KgZM54M9XWfBNmBupQuZXdQxoZRRQXHcpM"
    );
    assert_eq!(block_model.block_height, 234724352);
    assert_eq!(block_model.block_time, 1710441678);

    // Verify that we don't get an error if we try to index the same block again
    index_block(&setup.db_conn, &block).await.unwrap();
    assert_eq!(setup.api.get_slot().await.unwrap(), slot);

    // Verify that get_slot() gets updated a new block is indexed.
    let block = cached_fetch_block(&setup, slot + 1).await;
    index_block(&setup.db_conn, &block).await.unwrap();
    assert_eq!(setup.api.get_slot().await.unwrap(), slot + 1);
}
