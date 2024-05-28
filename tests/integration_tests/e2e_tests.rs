use function_name::named;
use photon_indexer::api::method::get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest;
use photon_indexer::api::method::get_transaction_with_compression_info::get_transaction_helper;
use photon_indexer::api::method::get_validity_proof::CompressedProof;
use photon_indexer::common::typedefs::serializable_pubkey::SerializablePubkey;
use photon_indexer::ingester::index_block;
use solana_sdk::pubkey::Pubkey;

use crate::utils::*;
use insta::assert_json_snapshot;
use photon_indexer::api::method::utils::{
    GetCompressedTokenAccountsByOwner, GetLatestSignaturesRequest,
};
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

    let bob_pubkey =
        SerializablePubkey::try_from("6hct2FBjZeQZ7Sscip1Xep3b94jCh7FvtaKqpfhne1Ak").unwrap();
    let charles_pubkey =
        SerializablePubkey::try_from("GF94vdJvzB28bSSEy7utrYkisNFuZR3ispD97W7Pd6LS").unwrap();

    let mint_tx =
        "67P7sjss5EV1eTzeRSFEejwohTGYvtxgrReAURrEnQJ1JiKzofFt7HdQX8zzyDqymxuggeK31DQRCDLxffc2fHwr";
    let transfer_tx =
        "2FrvajC55EwHptaGyuBTjBUm4XkE2c5KHo6AVez2qy8jXbH1m7gRUXaEVh9NJjBkpugU6QPHweQHkdiq6NP2eu2f";
    let transfer_txn2: &str =
        "62LLe2o1pSdcmos5NH2A1L7LAXU6FnRRHRJ224ZcN1kT5bWuiEYf3DgyS2DxytSm7RkDN6f24n1XCZJudZKQT6MC";
    let transfer_txn3 =
        "3H2R6UT3Pw9ejNmYTphb27jNoRdXmuLXjjxDaSs87uyuq25YJZtLQgACG9ANsvzfZN1s9d6UPZMabAYYsMoZ6GsG";

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
        index_transaction(&setup, tx).await;
    }
    for (person, pubkey) in [
        ("bob", bob_pubkey.clone()),
        ("charles", charles_pubkey.clone()),
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
        let hash_list = HashList(
            accounts
                .value
                .items
                .iter()
                .map(|x| x.account.hash.clone())
                .collect(),
        );
        let proofs = setup
            .api
            .get_multiple_compressed_account_proofs(hash_list.clone())
            .await
            .unwrap();
        assert_json_snapshot!(format!("{}-{}-proofs", name.clone(), person), proofs);

        let mut validity_proof = setup.api.get_validity_proof(hash_list).await.unwrap();
        // The Gnark prover has some randomness.
        validity_proof.compressed_proof = CompressedProof::default();

        assert_json_snapshot!(
            format!("{}-{}-validity-proof", name.clone(), person),
            validity_proof
        );

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

    let mut cursor = None;
    let limit = Limit::new(1).unwrap();
    let mut signatures = Vec::new();
    loop {
        let res = setup
            .api
            .get_latest_compression_signatures(GetLatestSignaturesRequest {
                cursor,
                limit: Some(limit.clone()),
            })
            .await
            .unwrap()
            .value;
        signatures.extend(res.items);
        cursor = res.cursor;
        if cursor.is_none() {
            break;
        }
    }
    let all_signatures = setup
        .api
        .get_latest_compression_signatures(GetLatestSignaturesRequest {
            cursor: None,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(signatures, all_signatures.value.items);

    let all_non_voting_transactions = setup
        .api
        .get_latest_non_voting_signatures(GetLatestSignaturesRequest {
            cursor: None,
            limit: None,
        })
        .await
        .unwrap();

    assert_eq!(
        all_non_voting_transactions.value.items,
        all_signatures.value.items
    );

    assert_json_snapshot!(format!("{}-latest-signatures", name.clone()), signatures);
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
        "5Mi1rJzJskdDqnyXS1k8rw91Gm9qsnFuAEmF5G2f21PFXGuoDHpdV4iLjvo9VjDp2kWBBxFnugLAdioyeHRcz5JA";
    let transfer_tx1 =
        "2jLuZajWysScmEeuW7egrnDLK5m15dxECAvFGVqy9B8LNxsQES4Qn2MRykjXrf1SKU9sg9cyUzsCYPcd9GdjqND2";
    let transfer_tx2 =
        "3DGUfF1qnMcXE8er6U6Yaqh83punr2FBK2oW2v6MVpdPRVzPF6K67Z298LiPeech4kBb677HFnYpxqSX3y8PmqtU";

    let payer_pubkey = SerializablePubkey(
        Pubkey::try_from("xJVLBBTaxGx9FXW56R3xvhasYck3jpsBhyRJmW52X5z").unwrap(),
    );
    let receiver_pubkey = SerializablePubkey(
        Pubkey::try_from("2EnRYUeRy6AXWNhpQrcwb39LeWknBJ2F8q3ofnuti6fN").unwrap(),
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
            let hash_list = HashList(
                accounts
                    .value
                    .items
                    .iter()
                    .map(|x| x.hash.clone())
                    .collect(),
            );
            let proofs = setup
                .api
                .get_multiple_compressed_account_proofs(hash_list.clone())
                .await
                .unwrap();
            assert_json_snapshot!(format!("{}-{}-proofs", name.clone(), owner_name), proofs);

            let mut validity_proof = setup.api.get_validity_proof(hash_list).await.unwrap();
            // The Gnark prover has some randomness.
            validity_proof.compressed_proof = CompressedProof::default();

            assert_json_snapshot!(
                format!("{}-{}-validity-proof", name.clone(), owner_name),
                validity_proof
            );

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

#[named]
#[rstest]
#[tokio::test]
#[serial]
<<<<<<< HEAD
async fn test_validity_proof(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use photon_indexer::api::method::get_multiple_compressed_account_proofs::HashList;

=======
async fn test_get_latest_non_voting_signatures(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
>>>>>>> ba5b50bd07a7bbe73161dcc3eb464566bcf671cd
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
<<<<<<< HEAD
            network: Network::Localnet,
=======
            network: Network::Devnet,
>>>>>>> ba5b50bd07a7bbe73161dcc3eb464566bcf671cd
            db_backend,
        },
    )
    .await;

<<<<<<< HEAD
    let compress_tx =
        "N955JL3hSckkfpaB8r2W6vpMQCGXfuzcsYVdkj15zxUxSNUGnArM8KQyjirY1xxfK8QF9tdS79ANdjFfHCDmdR7";

    let payer_pubkey = SerializablePubkey(
        Pubkey::try_from("DDjXmGVKYfNfLH2dWp9GCMbXeFwv9CAGkcb45nCaL64w").unwrap(),
    );
    let txs = [compress_tx];

    reset_tables(&setup.db_conn).await.unwrap();
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

    index_multiple_transactions(&setup, txs.to_vec()).await;

    for (owner, owner_name) in [(payer_pubkey.clone(), "payer")] {
        let accounts = setup
            .api
            .get_compressed_accounts_by_owner(GetCompressedAccountsByOwnerRequest {
                owner,
                ..Default::default()
            })
            .await
            .unwrap();
        let hash_list = HashList(
            accounts
                .value
                .items
                .iter()
                .map(|x| x.hash.clone())
                .collect(),
        );

        let mut validity_proof = setup.api.get_validity_proof(hash_list).await.unwrap();
        // The Gnark prover has some randomness.
        validity_proof.compressed_proof = CompressedProof::default();

        assert_json_snapshot!(
            format!("{}-{}-validity-proof", name.clone(), owner_name),
            validity_proof
        );
    }
=======
    let slot = 270893658;
    let block = cached_fetch_block(&setup, slot).await;
    index_block(&setup.db_conn, &block).await.unwrap();
    let all_nonvoting_transactions = setup
        .api
        .get_latest_non_voting_signatures(GetLatestSignaturesRequest {
            cursor: None,
            limit: None,
        })
        .await
        .unwrap();
    assert_eq!(all_nonvoting_transactions.value.items.len(), 46);
>>>>>>> ba5b50bd07a7bbe73161dcc3eb464566bcf671cd
}
