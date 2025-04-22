use photon_indexer::ingester::parser::parse_transaction;
use photon_indexer::ingester::typedefs::block_info::TransactionInfo;
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use solana_transaction_status::UiTransactionEncoding;
use std::str::FromStr;

#[test]
fn test_mainnet_overflow() {
    let rpc_client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());

    let signature = Signature::from_str(
        "4Namsu4qun8Xmm6sSH6yXuWbGYYkGNokVHWbSGCXiE97NhRJp2tGKRgZdr3xPesYNJAGrWUFFQw2WUJqHvG3jDC",
    )
    .unwrap();

    let tx_data = rpc_client
        .get_transaction_with_config(
            &signature,
            solana_client::rpc_config::RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                commitment: Some(CommitmentConfig::finalized()),
                max_supported_transaction_version: Some(0),
            },
        )
        .expect("Failed to fetch transaction");

    let transaction_info =
        TransactionInfo::try_from(tx_data).expect("Failed to convert transaction");

    println!("Transaction info: {:?}", transaction_info);

    let state_update = parse_transaction(&transaction_info, 321624901).unwrap();
    state_update.out_accounts.iter().for_each(|account| {
        if let Some(account_data) = &account.account.data {
            println!("Account data: {:?}", account_data);
            // assert!(account_data.discriminator == [0, 0, 0, 0, 0, 0, 0, 0]);
        }
    });
}
