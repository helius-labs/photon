use function_name::named;
use ingester::parser::parse_transaction;
use serial_test::serial;

use crate::utils::*;

#[tokio::test]
#[serial]
#[named]
async fn test_parsing() {
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
    assert_eq!(events.len(), 2)
}
