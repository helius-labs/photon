// use api::{api::ApiContract, method::get_utxos::GetUtxosRequest};
// use function_name::named;
// use ingester::{parser::parse_transaction, persist::persist_bundle};
// use serial_test::serial;

// use crate::utils::*;

// #[tokio::test]
// #[serial]
// #[named]
// async fn test_e2e() {
//     let name = trim_test_name(function_name!());
//     let setup = setup_with_options(
//         name,
//         TestSetupOptions {
//             network: Network::Localnet,
//         },
//     )
//     .await;

//     let tx = cached_fetch_transaction(
//         &setup,
//         "2y27eTCZ53DiFubUqBtstcrFrDCvr1sqCJFYDnjFVuZrrCGXvQPfVVosBv7mYF3LeJRy73EiGzqPX2vWHDg4iRCk",
//     )
//     .await;

//     let events = parse_transaction(tx).unwrap();
//     assert_eq!(events.len(), 1);
//     for event in events {
//         persist_bundle(&setup.db_conn, event).await.unwrap();
//     }
//     let utxos = setup
//         .api
//         .get_utxos(GetUtxosRequest {
//             owner: "8uxi3FheruZNcPfq4WKGQD19xB44QMfUGuFLij9JWeJ"
//                 .try_into()
//                 .unwrap(),
//         })
//         .await
//         .unwrap();

//     assert_eq!(utxos.total, 1);
// }
