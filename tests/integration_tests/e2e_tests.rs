use function_name::named;
use photon::api::method::get_compressed_token_accounts_by_owner::GetCompressedTokenAccountsByOwnerRequest;
use photon::api::method::get_utxos::GetUtxosRequest;
use photon::dao::typedefs::serializable_pubkey::SerializablePubkey;
use photon::ingester::index_block;
use photon::ingester::parser::parse_transaction;

use crate::utils::*;
use insta::assert_json_snapshot;
use photon::api::method::utils::TokenUxto;
use photon::dao::generated::blocks;
use photon::dao::typedefs::hash::Hash;
use sea_orm::ColumnTrait;
use sea_orm::EntityTrait;
use sea_orm::QueryFilter;
use serial_test::serial;
use solana_sdk::pubkey::Pubkey;

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_e2e_utxo_parsing(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name,
        TestSetupOptions {
            network: Network::Localnet,
            db_backend,
        },
    )
    .await;

    let tx = cached_fetch_transaction(
        &setup,
        "2y27eTCZ53DiFubUqBtstcrFrDCvr1sqCJFYDnjFVuZrrCGXvQPfVVosBv7mYF3LeJRy73EiGzqPX2vWHDg4iRCk",
    )
    .await;

    let events = parse_transaction(&tx, 0).unwrap();
    assert_eq!(events.len(), 1);
    for event in events {
        persist_bundle_using_connection(setup.db_conn.as_ref(), event)
            .await
            .unwrap();
    }
    let utxos = setup
        .api
        .get_utxos(GetUtxosRequest {
            owner: "8uxi3FheruZNcPfq4WKGQD19xB44QMfUGuFLij9JWeJ"
                .try_into()
                .unwrap(),
        })
        .await
        .unwrap();

    assert_eq!(utxos.items.len(), 1);
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_e2e_token_mint(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] db_backend: DatabaseBackend,
) {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name,
        TestSetupOptions {
            network: Network::Localnet,
            db_backend,
        },
    )
    .await;

    let tx = cached_fetch_transaction(
        &setup,
        "kSEwLwJmJASMLycq52gKpNe7VtZdvuYaK9Vro2JZU4LHDt9GxgybWXtv7q9SRFvWVkCp7SEmWr51AdmKHD5N5wo",
    )
    .await;

    let events = parse_transaction(&tx, 0).unwrap();
    assert_eq!(events.len(), 1);
    for event in events {
        persist_bundle_using_connection(&setup.db_conn.as_ref(), event)
            .await
            .unwrap();
    }

    let owner = "GTP6qbHeRne8doYekYmzzYMTXAtHMtpzzguLg2LLNMeD";

    let token_accounts = setup
        .api
        .get_compressed_token_accounts_by_owner(GetCompressedTokenAccountsByOwnerRequest {
            owner: owner.try_into().unwrap(),
            mint: None,
        })
        .await
        .unwrap();

    assert_eq!(token_accounts.items.len(), 1);
    let token_utxo = token_accounts.items.get(0).unwrap();
    let expected_token_utxo = TokenUxto {
        owner: owner.try_into().unwrap(),
        mint: SerializablePubkey::try_from("GDvagojL2e9B7Eh7CHwHjQwcJAAtiMpbvCvtzDTCpogP").unwrap(),
        amount: 200,
        delegate: None,
        is_native: false,
        close_authority: None,
        frozen: false,
    };
    assert_eq!(token_utxo, &expected_token_utxo);
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

    let tx = cached_fetch_transaction(
        &setup,
        "2FWS4xBAHiZKfZ9gwcq6i1wqvoYX2acj49wAHoP6CtqshQaFjgnSxcMUPANPMU3q8qoESUacmYNHG9LCCh3X59fB",
    )
    .await;

    let events = parse_transaction(&tx, 0).unwrap();
    assert_eq!(events.len(), 1);
    for event in events {
        persist_bundle_using_connection(&setup.db_conn, event)
            .await
            .unwrap();
    }

    let owner1 = "A79DKmTDe8VzfRLti3wTi7mbJ9ENZtK7eBHtJ4QYCR2Y";
    let owner2 = "K75mYjtM3zrpBqBwEtQ1RsM39YULMMMoP1pm5ZQkF2h";

    for owner in [owner1, owner2] {
        let utxos = setup
            .api
            .get_utxos(GetUtxosRequest {
                owner: SerializablePubkey::from(Pubkey::try_from(owner).unwrap()),
            })
            .await
            .unwrap();
        assert_json_snapshot!(format!("{}-{}-utxos", name, owner), utxos);
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

    let block = cached_fetch_block(&setup, 254170887).await;
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
}
