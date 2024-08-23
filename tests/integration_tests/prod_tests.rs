use std::{collections::HashSet, sync::Arc};

use function_name::named;
use insta::assert_json_snapshot;
use photon_indexer::{
    api::{
        api::PhotonApi,
        method::{
            get_compressed_accounts_by_owner::GetCompressedAccountsByOwnerRequest,
            get_multiple_compressed_account_proofs::HashList,
        },
    },
    common::typedefs::serializable_pubkey::SerializablePubkey,
    dao::generated::transactions,
    ingester::{
        index_block,
        parser::parse_transaction,
        typedefs::block_info::{BlockInfo, BlockMetadata},
    },
};
use solana_client::nonblocking::rpc_client::RpcClient;

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

    let readonly_devnet_db_url = std::env::var("READONLY_DEVNET_DB_URL").unwrap();
    let pool = setup_pg_pool(readonly_devnet_db_url.to_string()).await;
    let devnet_db = Arc::new(SqlxPostgresConnector::from_sqlx_postgres_pool(pool));
    let rpc_client = Arc::new(RpcClient::new("https://api.devnet.solana.com".to_string()));
    let prover_url = "http://localhost:3001";
    let api = PhotonApi::new(devnet_db.clone(), rpc_client, prover_url.to_string());

    let response = api
        .get_compressed_accounts_by_owner(GetCompressedAccountsByOwnerRequest {
            owner: SerializablePubkey::try_from("11111116EPqoQskEM2Pddp8KTL9JdYEBZMGF3aq7V")
                .unwrap(),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_json_snapshot!(name, response);
}
