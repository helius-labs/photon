use function_name::named;
use serial_test::serial;

use crate::utils::*;

#[tokio::test]
#[serial]
#[named]
async fn test_indexing() {
    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name,
        TestSetupOptions {
            network: Network::Localnet,
        },
    )
    .await;

    let txns = [
        "eRVpMHy9PYrCBGmfuDxPY8EkkCQSi9bgaQsCzsLMWDEbpeKsLRmQ2U1chpzsvjVKdcUzhV6joRZ5p5NyWbm8fs5",
    ];
    index_transactions(&setup, &txns).await;
}
