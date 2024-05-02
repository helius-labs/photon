use function_name::named;
use itertools::Itertools;
use photon_indexer::api::method::get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest;
use photon_indexer::api::method::get_transaction_with_compression_info::get_transaction_helper;
use photon_indexer::common::typedefs::serializable_pubkey::SerializablePubkey;
use photon_indexer::ingester::index_block;
use solana_sdk::pubkey::Pubkey;

use crate::utils::*;
use insta::assert_json_snapshot;
use photon_indexer::api::method::utils::GetCompressedTokenAccountsByOwner;
use photon_indexer::common::typedefs::hash::Hash;
use photon_indexer::dao::generated::blocks;
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
    use std::str::FromStr;

    use log::info;
    use photon_indexer::{
        api::method::{
            get_compression_signatures_for_token_owner::GetCompressionSignaturesForTokenOwnerRequest,
            get_multiple_compressed_account_proofs::HashList, utils::Limit,
        },
        common::typedefs::serializable_signature::SerializableSignature,
    };
    use solana_sdk::signature::Signature;

    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Localnet,
            db_backend,
        },
    )
    .await;

    let payer_pubkey =
        SerializablePubkey::try_from("Fg5TtbkvcqLedmwFqqbWty9NRqMKKMqy9zNLZZRQXSDc").unwrap();
    let bob_pubkey =
        SerializablePubkey::try_from("4rANawp9ahHmvaVi3CCeJSam8JTA4DDY3htJKnhYxbHN").unwrap();
    let charles_pubkey =
        SerializablePubkey::try_from("GyXNDaUUBwzYp24vEzang6SyCzcA8L3csZkiALqF6KRs").unwrap();

    let mint_tx =
        "2DcgLz1KwPaMoiRAAmYAirx4213swLbaSM1YYuCkbgzngq61XSWaFCRGuFoMgqW5htsEZRKBm6uQQPAPCXUVL8eH";
    let transfer_tx =
        "DFE7shhwCWd5P14NfA1ELSpd96FaJhBkjSet2Y7t7CtM4oLhP17AYKtyMBjZWi4hgFKvqET8zcAQUDuAC6sARfk";
    let transfer_txn2: &str =
        "5FDV6xrPRyfEUP2Kg7mwZVj34LVEfq9gsrquyFkvmmdPiiEn3Bhd8sTJTe6UhszNHLgwWfV6j9X4rsceL9mNNyRZ";
    let transfer_txn3 =
        "2pGQ7MgWuhrKpeSAzs58CXttKDiws33X68hQv4tRm6NUH6JCsCBk5uteNcjt2fXdToYhC6s8b6SDzoXV9CLJTo94";

    let txs = [mint_tx, transfer_tx, transfer_txn2, transfer_txn3];

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

    for tx in txs {
        info!("{}", tx);
        index_transaction(&setup, tx).await;
    }
    for (person, pubkey) in [
        ("bob", bob_pubkey.clone()),
        ("charles", charles_pubkey.clone()),
        ("payer", payer_pubkey.clone()),
    ] {
        let accounts = setup
            .api
            .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwner {
                owner: pubkey.clone(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_json_snapshot!(format!("{}-{}-accounts", name.clone(), person), accounts);
        let proofs = setup
            .api
            .get_multiple_compressed_account_proofs(HashList(
                accounts
                    .value
                    .items
                    .iter()
                    .map(|x| x.account.hash.clone())
                    .collect(),
            ))
            .await
            .unwrap();
        assert_json_snapshot!(format!("{}-{}-proofs", name.clone(), person), proofs);

        let mut cursor = None;
        let limit = Limit::new(1).unwrap();
        let mut signatures = Vec::new();
        loop {
            let res = setup
                .api
                .get_compression_signatures_for_token_owner(
                    GetCompressionSignaturesForTokenOwnerRequest {
                        owner: pubkey.clone(),
                        cursor,
                        limit: Some(limit.clone()),
                    },
                )
                .await
                .unwrap()
                .value;
            signatures.extend(res.items);
            cursor = res.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_json_snapshot!(
            format!("{}-{}-token-signatures", name.clone(), person),
            signatures
        );

        let token_balances = setup
                .api
                .get_compressed_token_balances_by_owner(photon_indexer::api::method::get_compressed_token_balances_by_owner::GetCompressedTokenBalancesByOwnerRequest {
                    owner: pubkey.clone(),
                    ..Default::default()
                })
                .await
                .unwrap();

        assert_json_snapshot!(
            format!("{}-{}-token-balances", name.clone(), person),
            token_balances
        );
    }
    for (txn_name, txn_signature) in [("mint", mint_tx), ("transfer", transfer_tx)] {
        let txn = cached_fetch_transaction(&setup, txn_signature).await;
        let txn_signature = SerializableSignature(Signature::from_str(txn_signature).unwrap());
        // Test get transaction
        let parsed_transaction: photon_indexer::api::method::get_transaction_with_compression_info::GetTransactionResponse = get_transaction_helper(&setup.db_conn, txn_signature, txn).await.unwrap();
        assert_json_snapshot!(
            format!("{}-{}-transaction", name.clone(), txn_name),
            parsed_transaction
        );
    }
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_lamport_transfers(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use photon_indexer::api::method::get_multiple_compressed_account_proofs::HashList;

    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Localnet,
            db_backend,
        },
    )
    .await;

    let compress_tx =
        "3ZmBETHr8tHPr7iKdgW4NjTzcUCUHtSBFQD2JwSniShBV9fix6xb6FxRyR1cMRW83WerRYU5b1uxDLM3GqomCkYx";
    let transfer_tx1 =
        "64tDNpAnQHZsuiH8UG8vXDzXbwGHdghfLYerPdaVdgkQ2kSTE65kDT7ajzzhd2ZDW8nVoKYztnjxCaV8bWJxNCXR";
    let transfer_tx2 =
        "yK3j6TkABz2SVBtvsVwJV5ZoLv1tJBrBR8Z8JsosHQGbeizt7yx8Qx1Nva1ytcKa7hAXsT7R6WyQUiCH43Cwtk5";

    let payer_pubkey = SerializablePubkey(
        Pubkey::try_from("8wSvtG3dvL7Nso8E3wx6L9SwTRiZuR8ojRX5D6mwx7hJ").unwrap(),
    );
    let receiver_pubkey = SerializablePubkey(
        Pubkey::try_from("G1zCvDhtSdV6m92q3tXBEE4d531fBx1vD7EUpeeQZrNp").unwrap(),
    );
    let txs = [compress_tx, transfer_tx1, transfer_tx2];
    let number_of_indexes = [1, 2];

    for individually in [true, false] {
        reset_tables(&setup.db_conn).await.unwrap();
        for number in number_of_indexes.iter() {
            for _ in 0..*number {
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

                match individually {
                    true => {
                        for tx in txs.iter() {
                            index_transaction(&setup, tx).await;
                        }
                    }
                    false => {
                        index_multiple_transactions(&setup, txs.to_vec()).await;
                    }
                }
            }
        }
        for (owner, owner_name) in [
            (payer_pubkey.clone(), "payer"),
            (receiver_pubkey.clone(), "receiver"),
        ] {
            let accounts = setup
                .api
                .get_compressed_accounts_by_owner(GetCompressedAccountsByOwnerRequest {
                    owner,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_json_snapshot!(
                format!("{}-{}-accounts", name.clone(), owner_name),
                accounts
            );
            let proofs = setup
                .api
                .get_multiple_compressed_account_proofs(HashList(
                    accounts
                        .value
                        .items
                        .iter()
                        .map(|x| x.hash.clone())
                        .collect(),
                ))
                .await
                .unwrap();
            assert_json_snapshot!(format!("{}-{}-proofs", name.clone(), owner_name), proofs);

            let signatures = setup
                .api
                .get_compression_signatures_for_owner(photon_indexer::api::method::get_compression_signatures_for_owner::GetCompressionSignaturesForOwnerRequest {
                    owner,
                    ..Default::default()
                })
                .await
                .unwrap();

            let limit = photon_indexer::api::method::utils::Limit::new(1).unwrap();
            let mut cursor = None;
            let mut paginated_signatures = Vec::new();
            loop {
                let res = setup
                    .api
                    .get_compression_signatures_for_owner(photon_indexer::api::method::get_compression_signatures_for_owner::GetCompressionSignaturesForOwnerRequest {
                        owner: owner.clone(),
                        cursor,
                        limit: Some(limit.clone()),
                    })
                    .await
                    .unwrap()
                    .value;
                paginated_signatures.extend(res.items);
                cursor = res.cursor;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(signatures.value.items, paginated_signatures);
            assert_json_snapshot!(
                format!("{}-{}-signatures", owner_name, name.clone()),
                signatures
            );

            let balance = setup
                .api
                .get_compressed_balance_by_owner(photon_indexer::api::method::get_compressed_balance_by_owner::GetCompressedBalanceByOwnerRequest {
                    owner,
                    ..Default::default()
                })
                .await
                .unwrap();

            assert_eq!(
                accounts
                    .value
                    .items
                    .iter()
                    .map(|x| x.lamports.0)
                    .sum::<u64>(),
                balance.value.0,
            );
        }
    }
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
    assert_eq!(setup.api.get_indexer_slot().await.unwrap().0, slot);

    // Verify that get_indexer_slot() gets updated a new block is indexed.
    let block = cached_fetch_block(&setup, slot + 1).await;
    index_block(&setup.db_conn, &block).await.unwrap();
    assert_eq!(setup.api.get_indexer_slot().await.unwrap().0, slot + 1);
}
