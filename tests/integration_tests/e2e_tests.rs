use function_name::named;
use photon_indexer::api::method::get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest;
use photon_indexer::api::method::get_multiple_new_address_proofs::AddressList;
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
use photon_indexer::api::method::{
    get_multiple_compressed_account_proofs::HashList, get_validity_proof::GetValidityProofRequest,
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
async fn test_e2e_mint_and_transfer_legacy_transactions(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use std::str::FromStr;

    use photon_indexer::{
        api::method::{
            get_compression_signatures_for_token_owner::GetCompressionSignaturesForTokenOwnerRequest,
            get_multiple_compressed_account_proofs::HashList,
            utils::{Limit, SignatureInfo},
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
        SerializablePubkey::try_from("D6B941apqSHVU5TV2n7JevkpAVQ4sdhiT6g1fNoEMfYG").unwrap();
    let charles_pubkey =
        SerializablePubkey::try_from("8UMPheZvcShkq7DwPpJN5NusYDKTwfx9FVTyZXnVLSaj").unwrap();

    let mint_tx =
        "4uLjV2xNmD6LD76XeSfDZoqxnjfY7bzcnpj7cqG4ZDAhVKqtUbAxajuvs4qSdJxLxbxcAjStvUnY5DhhSPgKQFcB";
    let transfer_tx =
        "4Ld9WjbPxzhDsiTfiPfkw1v9xoAXwEUHWLxnKR9sazxRxWeGCeWHgQHpvABwssQUb8srwdFfgFF2sUxn8Toq8hCM";
    let transfer_txn2: &str =
        "5Ea6iQQZ1JYfiDq8H6qMKYdFcut1cUfd9pstCGc3BsQztTbgKA4LhsEx6XKQ1bMhKp5KMyyw5BuuooxEcpGLyHFB";
    let transfer_txn3 =
        "41rBbMrNtZveMdPXPrHJLaoEzZ9WvJdgTbvcDhLM39THw7v51FjbCdUyWYFNswr1k3pRYbKvAcYb84bft2NbWgP2";

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

        let mut validity_proof = setup
            .api
            .get_validity_proof(GetValidityProofRequest {
                hashes: hash_list.0.clone(),
                newAddresses: vec![],
            })
            .await
            .unwrap();
        // The Gnark prover has some randomness.
        validity_proof.value.compressedProof = CompressedProof::default();

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
        all_non_voting_transactions
            .value
            .items
            .into_iter()
            .map(Into::<SignatureInfo>::into)
            .collect::<Vec<_>>(),
        all_signatures.value.items
    );

    assert_json_snapshot!(format!("{}-latest-signatures", name.clone()), signatures);
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_e2e_mint_and_transfer_new_transactions(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use std::str::FromStr;

    use photon_indexer::{
        api::method::{
            get_compression_signatures_for_token_owner::GetCompressionSignaturesForTokenOwnerRequest,
            get_multiple_compressed_account_proofs::HashList,
            utils::{Limit, SignatureInfo},
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
        SerializablePubkey::try_from("HGkKWL7Cfm4YAqb6wtCfC6PsAKQ1DL2uccGSyJgzVSKV").unwrap();
    let charles_pubkey =
        SerializablePubkey::try_from("5GkBcTAGLJ2nU7WEbaecndeV5HXzp8LicqkGf6DKKR97").unwrap();

    let mint_tx =
        "2M63FRn8tYUCDxuxo4fJLkwQfu7FQp5RecNVPK42TMdAbq45BiWjuz1P9nNy6uRrcMpXph63eAUW71GzQE1HmEGs";
    let transfer_tx =
        "5MqYh45rQf1x8D1EpeiWh4XXM7WBeDsE3MCpqvstTaCMQVBXuk3wMiUoQGY9Y2t83oGhBKoFG8zWUFoHpEB77C1g";
    let transfer_txn2: &str =
        "3xB9XfSrNHov4FDtgQdJMZtKvbDcqvhkpyXN72ZurXvRTKbGXpw4XbUGAnbtCuNiHixf7qoSDQskTcXdA6n412yW";
    let transfer_txn3 =
        "P3NmVFAosVot31wa7oyJ3e4s4zFPeUwGnm6HhFmPukjXQ14xUQvrxJSuEy2VS8gutuejoRFzPPoga3ZeBRxDJFa";

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

        let mut validity_proof = setup
            .api
            .get_validity_proof(GetValidityProofRequest {
                hashes: hash_list.0.clone(),
                newAddresses: vec![],
            })
            .await
            .unwrap();
        // The Gnark prover has some randomness.
        validity_proof.value.compressedProof = CompressedProof::default();

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
        all_non_voting_transactions
            .value
            .items
            .into_iter()
            .map(Into::<SignatureInfo>::into)
            .collect::<Vec<_>>(),
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
        "4dqYnWSm8qKxCyeADsc1eqQBSQQ7BneXbeNxmw8P6Fu5TVv26Roud4UwtvBfy4bV7NLFe1NK97ytqwPqwyGRGVT3";
    let transfer_tx1 =
        "kNXkK3aXPDurmMfPbcfPNwNEWbtqdmFLcE8ZrH3oV7tbHBdH3xzUYso7icJZ4sT7MNjemsH8MdK9WW5CTHaUM59";
    let transfer_tx2 =
        "akFrcAJwhAeK6AyrMvtcMXF13Cd451NLmCmGteL3WbZnzCrW25GUr7Ku5thfwCT28M8TJbRa5p854gVr1cVvDgN";

    let payer_pubkey = SerializablePubkey(
        Pubkey::try_from("G2TkKakizZKiAgkMkwWNZzvxdhMZSVDggmysaVuckjmB").unwrap(),
    );
    let receiver_pubkey = SerializablePubkey(
        Pubkey::try_from("4Y9xHaihpNo3H8tM9CAkBj6sCKS8mdirWArNGomXWS7m").unwrap(),
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

            let mut validity_proof = setup
                .api
                .get_validity_proof(GetValidityProofRequest {
                    hashes: hash_list.0.clone(),
                    newAddresses: vec![],
                })
                .await
                .expect(&format!(
                    "Failed to get validity proof for owner with hash list len: {} {}",
                    owner_name,
                    hash_list.0.len()
                ));
            // The Gnark prover has some randomness.
            validity_proof.value.compressedProof = CompressedProof::default();

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
async fn test_get_latest_non_voting_signatures(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Devnet,
            db_backend,
        },
    )
    .await;

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
    assert_json_snapshot!(
        format!("{}-non-voting-transactions", name.clone()),
        all_nonvoting_transactions
    );
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_get_latest_non_voting_signatures_with_failures(
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

    let slot = 279620356;
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
    assert_json_snapshot!(
        format!("{}-non-voting-transactions", name.clone()),
        all_nonvoting_transactions
    );
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_nullifier_queue(
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

    let user_pubkey = "5SHZSriNACrYm32eXQFXythrkBM8sWxoFmsBS5hkZ76k";

    let compress_tx =
        "3z5aGksRbQCFvNvutWcz1qAnC1fzTAsa6Mz6NpbveKZfVcAuQ1xE1vkoFYoJLwEjRbgv5CwLrDPEP1KmVTpJ9skD";
    let transfer_tx =
        "wRPnjQc4ydZJWNVQ1ws9fSWCyPNJaYgVWmbJch64HgbbjPey1B5mPRQzqMF46dqphtjoyeuzL9pWKj9kvwGX1kr";
    let nullifier_transaction =
        "9LWs9tHoLNPpEVhJojECoDtVEHkmtNJ2VTb2fAEjE6Jqk51pJ5icqgneajXZ128F6isYyrAunu3FxMfvT8AMYt7";

    let txs = [compress_tx, transfer_tx, nullifier_transaction];

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

    let accounts = setup
        .api
        .get_compressed_accounts_by_owner(GetCompressedAccountsByOwnerRequest {
            owner: SerializablePubkey::try_from(user_pubkey).unwrap(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_json_snapshot!(format!("{}-accounts", name.clone()), accounts);
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
    assert_json_snapshot!(format!("{}-proofs", name.clone()), proofs);
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_address_with_nullifiers(
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

    let address = "14LNC6QgTME7wBRWqnWLc2TW28YUVTZS5BUhYurVjuMU";
    let address_2 = "1111111ogCyDbaRMvkdsHB3qfdyFYaG1WtRUAfdh";

    let compress_tx =
        "3TEXdBWMdMt676EdSHfPKoP2tVWrenpQpQYuD4Cziufm6LMQ5ABsPSYix77ox9U6Vc1CtcgUSciZhsxpKCFHw9JT";
    let address_tree_signature =
        "5n531J7yLGAykHPtX12E4sXPr6GMzvZJFmxyJd9z9ZpS965nGmBCM3nthRJ3LpXSVsgc4Rxgr55tLPo5w6dpW18y";

    let txs = [compress_tx, address_tree_signature];

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

    let address_list = AddressList(vec![SerializablePubkey::try_from(address_2).unwrap()]);

    let proof = setup
        .api
        .get_multiple_new_address_proofs(address_list)
        .await
        .unwrap();

    assert_json_snapshot!(format!("{}-proof", name.clone()), proof);

    let transactions  = setup
        .api
        .get_compression_signatures_for_address(photon_indexer::api::method::get_compression_signatures_for_address::GetCompressionSignaturesForAddressRequest {
            address: SerializablePubkey::try_from(address).unwrap(),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_json_snapshot!(format!("{}-transactions", name.clone()), transactions);
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_indexing_bug(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use photon_indexer::api::method::get_multiple_new_address_proofs::ADDRESS_TREE_ADDRESS;

    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::ZkTesnet,
            db_backend,
        },
    )
    .await;

    let tx1 =
        "5wXw7jheUt9zWQKmhyhS8DaopKGjaFTda47FrBQXVhqyeDfwGVD8hfeJcpVyPzEmmb45FJtLxo2xydKPJFAtJ1j6";

    let tx2 =
        "5UEYDJUmBZKxNfGTBrnVCZVbjGPA2XLw3ezTQg88zVCJePTbXXpu7s9ncpA7HwfGFxqCNz14HkQqEZwfVtbW3SLK";
    let tx3 =
        "Y9FZwudKniSE8ycCbMzTkmU8bzQHRSyv6v67FFbY37yxhrU1Jsyd2RE1XKnf9wrPXCYQ1682bVTdKTLA9125rc6";
    let tx4 =
        "53HtJ3XiztjuKAJtY2cX8pRavaw7MGnwsvihcNtXDiSfrAghhj8YhSMwgzRbkPftYY7cQGDcm8W1PA8D2fr4H3wo";

    let txs = [tx1, tx2, tx3, tx4];

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

    let tree = SerializablePubkey::from(ADDRESS_TREE_ADDRESS);

    for tx in txs {
        index_transaction(&setup, tx).await;
        verify_tree(setup.db_conn.as_ref(), tree.clone()).await;
    }

    let proof_address = "12prJNGB6sfTMrZM1Udv2Aamv9fLzpm5YfMqssTmGrWy";

    let address_list = AddressList(vec![SerializablePubkey::try_from(proof_address).unwrap()]);

    let proof = setup
        .api
        .get_multiple_new_address_proofs(address_list)
        .await
        .unwrap();

    assert_json_snapshot!(format!("{}-proof", name.clone()), proof);
}
