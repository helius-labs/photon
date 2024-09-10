use function_name::named;
use photon_indexer::api::method::get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest;
use photon_indexer::api::method::get_latest_non_voting_signatures::MAX_LATEST_NON_VOTING_SIGNATURES;
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
async fn test_e2e_mint_and_transfer_transactions(
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
        SerializablePubkey::try_from("EU57rQxcmFhJ24ApVdUy3y4MxFXcTUN3uiVeWvGgtWXu").unwrap();
    let charles_pubkey =
        SerializablePubkey::try_from("CHvwuTvTiwRSNBwAnrCG14V8YTJ6wwhHQrifxdFzHzsX").unwrap();

    let mint_tx =
        "64jFxW4xxife8UqpEQyYdA589rzFKWx8rT4vun6soCpWXmDermr588UKX2261YYZ8ZSzcu2NUJY2aCfee64FvTW4";
    let transfer_tx =
        "5UZuijpSnqBpgMpqnggxZ58bzZ8aJtqqVjtvzx2Pg2C58jXph1rdrXyHdTxJfsHawNJyr8syU4U1MEYccDUQeTSV";
    let transfer_txn2: &str =
        "2J9pSeLvXnyawUPKdks98ZibQWU8v38AEhmZp9g9P3WzY9dJSMQ3RmoUaGN1zVCF3tTQy6q2YbD94jq8uzSZsay7";
    let transfer_txn3 =
        "5oTNJvc5WWJacEdpZLimVjGu1uGkXoMth8dNKaAymVKA7arwhccxeJPRWDuYEvQHNkGnkRNJcwUbtV4KnwebCKj5";

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
    for (person, pubkey) in [("bob", bob_pubkey), ("charles", charles_pubkey)] {
        let accounts = setup
            .api
            .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwner {
                owner: pubkey,
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
                newAddressesWithTrees: vec![],
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
                        owner: pubkey,
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
                    owner: pubkey,
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
        "5NLdbqznXqmTPTN8JBLquriDggb9qaRszVGLSvt6t5esy2Q8Z1iqAuXF4qoLK7HM6oGLySUNUkzhnSocwArpAqmV";
    let transfer_tx1 =
        "4TFBPyvatWgjTdNesfaTo3YkbP2spvGmgZgLn6CvTeqRZSi1ZuPCkK7fLaDbPKskMSF4Azge6QPvtZt9VUV7KBF8";
    let transfer_tx2 =
        "QBrbAZFq12LCbnv5dByn8vB8Znam4ieGQVzybapgPL5LCa9KHfuYZKV6Nah6UGsa6FUptmT6tSpexWZDrbp82iP";

    let payer_pubkey = SerializablePubkey(
        Pubkey::try_from("J6ULJViDpQTRntYTmfkpUZZ71KTVkZNJFSDcQhFCo5Ef").unwrap(),
    );
    let receiver_pubkey = SerializablePubkey(
        Pubkey::try_from("FLkMEA7eA82Cvp7MqamqFKsRN3E2Vfg3Xa8NRyVARq5n").unwrap(),
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
        for (owner, owner_name) in [(payer_pubkey, "payer"), (receiver_pubkey, "receiver")] {
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
                    newAddressesWithTrees: vec![],
                })
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "Failed to get validity proof for owner with hash list len: {} {}",
                        owner_name,
                        hash_list.0.len()
                    )
                });
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
                        owner,
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
    use itertools::Itertools;

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
    assert_eq!(
        MAX_LATEST_NON_VOTING_SIGNATURES as usize,
        all_nonvoting_transactions.value.items.len()
    );
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_nullfiier_and_address_queue_transactions(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    use photon_indexer::api::method::get_multiple_new_address_proofs::{
        AddressListWithTrees, AddressWithTree, ADDRESS_TREE_ADDRESS,
    };

    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Localnet,
            db_backend,
        },
    )
    .await;

    let user_pubkey = "Ecs89dz8NsNoSxyUtp54G2HPbmvJr8qZnxnWUqiLT92r";

    let compress_tx =
        "35zJYUMreV5BRzuzSSfqSVWzLtMqPtWtLBkkH2CP24gFWijju46Vi3ARawzxs22GqZPbXo6uzSosaUXGLgRA9Hth";
    let transfer_tx =
        "gXhNzuJHcVz6k625LkeLB9qLU7D56WnHXH6hHBCQfdv7eYPbZPqWpqEjpz86qWa23megQofz8PPBYtPqEHCicbF";
    let forester_tx1 =
        "3pmqSXTSzdbbgmp63v1Dz1NGM5Z187wisdsonL8gDcPAimfFdnCB3H5agtcHg1fmhkkWk4PgGTjS2GjzQPknuGcg";
    let forester_tx2 =
        "3qSTxmtPen9HtjEhKGpzYdLvRqEQaxSeLaGeav2GC5c2PU8ZEVq84HSeMCxN3jrt6NWB5ZoWPAGn1dv27y3zyG75";

    let txs = [compress_tx, transfer_tx, forester_tx1, forester_tx2];

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

    let address = "1111111ogCyDbaRMvkdsHB3qfdyFYaG1WtRUAfdh";

    let address_list = AddressList(vec![SerializablePubkey::try_from(address).unwrap()]);

    let proof_v1 = setup
        .api
        .get_multiple_new_address_proofs(address_list)
        .await
        .unwrap();

    let address_list_with_trees = AddressListWithTrees(vec![AddressWithTree {
        address: SerializablePubkey::try_from(address).unwrap(),
        tree: SerializablePubkey::from(ADDRESS_TREE_ADDRESS),
    }]);

    let proof_v2 = setup
        .api
        .get_multiple_new_address_proofs_v2(address_list_with_trees)
        .await
        .unwrap();
    assert_json_snapshot!(format!("{}-proof-address", name.clone()), proof_v1);
    assert_json_snapshot!(format!("{}-proof-address", name.clone()), proof_v2);
}
