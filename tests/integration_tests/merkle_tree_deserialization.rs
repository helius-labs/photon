use function_name::named;
use photon_indexer::monitor::v1_tree_accounts::{AddressMerkleTreeAccount, StateMerkleTreeAccount};
use serial_test::serial;
use solana_pubkey::Pubkey;
use std::env;
use std::str::FromStr;

use crate::utils::*;

const STATE_MERKLE_TREE_V1: &str = "smt1NamzXdq4AMqS2fS2F1i5KTYPZRhoHgWx38d8WsT";
const ADDRESS_MERKLE_TREE_V1: &str = "amt1Ayt45jfbdw5YSo7iz6WZxUmnZsQTYXy82hVwyC2";

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_v1_state_tree_deserialization_from_devnet(
    #[values(DatabaseBackend::Sqlite)] db_backend: DatabaseBackend,
) {
    if env::var("DEVNET_RPC_URL").is_err() {
        return;
    }

    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Devnet,
            db_backend,
        },
    )
    .await;

    let tree_pubkey = Pubkey::from_str(STATE_MERKLE_TREE_V1).unwrap();
    let account = setup.client.get_account(&tree_pubkey).await.unwrap();

    let tree_account = StateMerkleTreeAccount::from_account_bytes(&account.data).unwrap();
    let merkle_tree = tree_account.tree().unwrap();

    assert_eq!(merkle_tree.height, 26);
    assert_eq!(merkle_tree.roots.capacity(), 2400);
    assert_eq!(
        tree_account.metadata.access_metadata.owner,
        [
            15, 216, 212, 71, 211, 163, 62, 85, 44, 152, 241, 31, 23, 118, 174, 50, 226, 14, 194,
            135, 20, 8, 57, 68, 15, 93, 48, 198, 231, 87, 72, 216
        ]
    );
}

#[named]
#[rstest]
#[tokio::test]
#[serial]
async fn test_v1_address_tree_deserialization_from_devnet(
    #[values(DatabaseBackend::Sqlite)] db_backend: DatabaseBackend,
) {
    if env::var("DEVNET_RPC_URL").is_err() {
        return;
    }

    let name = trim_test_name(function_name!());
    let setup = setup_with_options(
        name.clone(),
        TestSetupOptions {
            network: Network::Devnet,
            db_backend,
        },
    )
    .await;

    let tree_pubkey = Pubkey::from_str(ADDRESS_MERKLE_TREE_V1).unwrap();
    let account = setup.client.get_account(&tree_pubkey).await.unwrap();

    let tree_account = AddressMerkleTreeAccount::from_account_bytes(&account.data).unwrap();
    let indexed_tree = tree_account.tree().unwrap();

    assert_eq!(indexed_tree.merkle_tree.height, 26);
    assert_eq!(indexed_tree.merkle_tree.roots.capacity(), 2400);
    assert_eq!(
        tree_account.metadata.access_metadata.owner,
        [
            15, 216, 212, 71, 211, 163, 62, 85, 44, 152, 241, 31, 23, 118, 174, 50, 226, 14, 194,
            135, 20, 8, 57, 68, 15, 93, 48, 198, 231, 87, 72, 216
        ]
    );
}
