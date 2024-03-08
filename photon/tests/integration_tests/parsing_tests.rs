use function_name::named;
use photon::ingester::{parser::parse_transaction, persist::persist_bundle};
use photon::api::api::ApiContract;
use photon::api::method::get_utxos::GetUtxosRequest;

use serial_test::serial;

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
