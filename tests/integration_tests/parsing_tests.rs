use function_name::named;
use photon::api::api::ApiContract;
use photon::api::method::get_utxos::GetUtxosRequest;
use photon::dao::generated::token_owners;
use photon::ingester::{parser::parse_transaction, persist::persist_bundle};

use serial_test::serial;
use solana_sdk::pubkey::Pubkey;

use crate::utils::*;

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

    // let utxos = setup
    //     .api
    //     .get_utxos(GetUtxosRequest {
    //         owner: "8uxi3FheruZNcPfq4WKGQD19xB44QMfUGuFLij9JWeJ"
    //             .try_into()
    //             .unwrap(),
    //     })
    //     .await
    //     .unwrap();

    // assert_eq!(utxos.total, 1);
}
