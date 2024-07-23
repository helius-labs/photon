use std::collections::HashSet;

use function_name::named;
use insta::assert_json_snapshot;
use photon_indexer::{
    api::method::get_multiple_compressed_account_proofs::HashList,
    dao::generated::transactions,
    ingester::{
        index_block,
        parser::parse_transaction,
        typedefs::block_info::{BlockInfo, BlockMetadata},
    },
};

use crate::utils::*;
use sea_orm::ColumnTrait;
use sea_orm::{DbBackend, EntityTrait, QueryFilter, SqlxPostgresConnector};
use serial_test::serial;
use solana_sdk::signature::Signature;

#[tokio::test]
#[serial]
#[ignore]
#[named]
async fn test_incorrect_root_bug() {
    let name = trim_test_name(function_name!());
    let setup_options = TestSetupOptions {
        network: Network::Devnet,
        db_backend: DbBackend::Postgres,
    };
    let setup = setup_with_options(name.clone(), setup_options).await;

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

    let readonly_devnet_db_url = std::env::var("READONLY_DEVNET_DB_URL").unwrap();

    let pool = setup_pg_pool(readonly_devnet_db_url.to_string()).await;
    let devnet_db = SqlxPostgresConnector::from_sqlx_postgres_pool(pool);

    let account_transactions = transactions::Entity::find()
        .filter(transactions::Column::UsesCompression.eq(true))
        .all(&devnet_db)
        .await
        .unwrap();

    let tx_ids = account_transactions
        .iter()
        .map(|tx| (Signature::try_from(tx.signature.clone()).unwrap()))
        .collect::<HashSet<Signature>>();

    let mut txs = vec![];
    for id in tx_ids {
        println!("Fetching transaction: {}", id);
        let tx = cached_fetch_transaction(&setup, id.to_string().as_str()).await;
        txs.push(tx);
    }

    txs.sort_by(|a, b| a.slot.cmp(&b.slot));
    println!("Fetched {} transactions", txs.len());
    let mut account = None;

    for tx in txs {
        let state_update = parse_transaction(&tx.try_into().unwrap(), 0).unwrap();
        if state_update.out_accounts.len() >= 1 {
            account = Some(state_update.out_accounts[0].clone());
        }
        persist_state_update_using_connection(&setup.db_conn, state_update)
            .await
            .unwrap();
    }
    println!("Account: {:?}", account);
    let hash_list = HashList(vec![account.unwrap().hash]);
    let proofs = setup
        .api
        .get_multiple_compressed_account_proofs(hash_list.clone())
        .await
        .unwrap();

    assert_json_snapshot!(format!("{}-proofs", name.clone()), proofs);
}
