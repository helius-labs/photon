use api::{api::ApiContract, method::get_utxos::GetUtxosRequest};
use function_name::named;
use ingester::{parser::parse_transaction, persist::persist_bundle};
use serial_test::serial;

use crate::utils::*;

#[tokio::test]
#[serial]
#[named]
async fn test_e2e() {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name,
        TestSetupOptions {
            network: Network::Localnet,
        },
    )
    .await;

    let tx = cached_fetch_transaction(
        &setup,
        "2y27eTCZ53DiFubUqBtstcrFrDCvr1sqCJFYDnjFVuZrrCGXvQPfVVosBv7mYF3LeJRy73EiGzqPX2vWHDg4iRCk",
    )
    .await;

    let events = parse_transaction(tx).unwrap();
    assert_eq!(events.len(), 2);
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

    assert_eq!(utxos.total, 2);
}

// Transferred 420000000 lamports from Alice (GK75mYjtM3zrpBqBwEtQ1RsM39YULMMMoP1pm5ZQkF2h) to Bob (8uxi3FheruZNcPfq4WKGQD19xB44QMfUGuFLij9JWeJ)
// Input mock-utxos (consumed):
// ┌─────────┬────────────────────────────────────────────────┬─────────────┬──────┐
// │ (index) │ owner                                          │ lamports    │ data │
// ├─────────┼────────────────────────────────────────────────┼─────────────┼──────┤
// │ 0       │ 'GK75mYjtM3zrpBqBwEtQ1RsM39YULMMMoP1pm5ZQkF2h' │ '500000000' │ null │
// │ 1       │ 'GK75mYjtM3zrpBqBwEtQ1RsM39YULMMMoP1pm5ZQkF2h' │ '500000000' │ null │
// └─────────┴────────────────────────────────────────────────┴─────────────┴──────┘
// Blinding for input utxo 1: 4,182,255,224,249,190,224,212,33,99,168,204,28,16,0,252,173,220,49,163,152,128,112,223,13,233,17,1,105,83,139,23
// Blinding for input utxo 2: 16,0,24,134,43,165,193,20,1,222,232,25,221,236,79,105,176,217,184,40,146,241,32,144,187,120,123,158,214,4,226,199

// Output utxos (created):
// ┌─────────┬────────────────────────────────────────────────┬─────────────┬──────┐
// │ (index) │ owner                                          │ lamports    │ data │
// ├─────────┼────────────────────────────────────────────────┼─────────────┼──────┤
// │ 0       │ '8uxi3FheruZNcPfq4WKGQD19xB44QMfUGuFLij9JWeJ'  │ '420000000' │ null │
// │ 1       │ 'GK75mYjtM3zrpBqBwEtQ1RsM39YULMMMoP1pm5ZQkF2h' │ '580000000' │ null │
