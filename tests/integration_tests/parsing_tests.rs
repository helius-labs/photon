use function_name::named;
use photon::api::api::ApiContract;
use photon::api::method::get_compressed_token_accounts_by_owner::GetCompressedTokenAccountsByOwnerRequest;
use photon::api::method::get_utxos::GetUtxosRequest;
use photon::dao::generated::token_owners;
use photon::dao::typedefs::serializable_pubkey::SerializablePubkey;
use photon::ingester::{parser::parse_transaction, persist::persist_bundle};

use crate::utils::*;
use photon::api::method::get_compressed_token_accounts_by_owner::TokenUxto;
use serial_test::serial;
use solana_sdk::pubkey::Pubkey;

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_e2e(
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

    let events = parse_transaction(tx).unwrap();
    assert_eq!(events.len(), 1);
    for event in events {
        persist_bundle(&setup.db_conn, event).await.unwrap();
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

    assert_eq!(utxos.total, 1);
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_e2e_token_transfer(
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

    let events = parse_transaction(tx).unwrap();
    assert_eq!(events.len(), 1);
    for event in events {
        persist_bundle(&setup.db_conn, event).await.unwrap();
    }

    <token_owners::Entity as sea_orm::EntityTrait>::find()
        .all(setup.db_conn.as_ref())
        .await
        .unwrap()
        .iter()
        .for_each(|token_owner: &token_owners::Model| {
            println!("{:?}", Pubkey::try_from(token_owner.owner.clone()).unwrap());
        });

    let owner = "GTP6qbHeRne8doYekYmzzYMTXAtHMtpzzguLg2LLNMeD";

    let token_accounts = setup
        .api
        .get_compressed_account_token_accounts_by_owner(GetCompressedTokenAccountsByOwnerRequest {
            owner: owner.try_into().unwrap(),
            mint: None,
        })
        .await
        .unwrap();

    assert_eq!(token_accounts.total, 1);
    let token_utxo = token_accounts.items.get(0).unwrap();
    let expected_token_utxo = TokenUxto {
        owner: owner.try_into().unwrap(),
        mint: SerializablePubkey::try_from("GDvagojL2e9B7Eh7CHwHjQwcJAAtiMpbvCvtzDTCpogP").unwrap(),
        amount: 200,
        delegate: None,
        is_native: false,
        close_authority: None,
    };
    assert_eq!(token_utxo, &expected_token_utxo);
}
