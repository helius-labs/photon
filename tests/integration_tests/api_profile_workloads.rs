use crate::utils::{
    persist_state_update_using_connection, setup_with_options, DatabaseBackend, Network,
    TestSetupOptions, TEST_STATE_TREE_V1_1,
};
use async_trait::async_trait;
use light_compressed_account::{QueueType, TreeType};
use light_hasher::{Hasher, Poseidon};
use photon_indexer::{
    api::{
        api::PhotonApi,
        method::{
            get_compressed_account_proof::{
                GetCompressedAccountProofResponseValue, GetCompressedAccountProofResponseValueV2,
            },
            get_compression_signatures_for_address::GetCompressionSignaturesForAddressRequest,
            get_multiple_compressed_account_proofs::HashList,
            get_queue_info::GetQueueInfoRequest,
            get_transaction_with_compression_info::{
                get_transaction_helper, get_transaction_helper_v2, GetTransactionRequest,
            },
            get_validity_proof::TreeContextInfo,
            utils::{HashRequest, SignatureInfo},
        },
    },
    common::{
        relative_project_path,
        typedefs::{
            account::{Account, AccountContext, AccountWithContext},
            hash::Hash,
            serializable_pubkey::SerializablePubkey,
            serializable_signature::SerializableSignature,
            unix_timestamp::UnixTimestamp,
            unsigned_integer::UnsignedInteger,
        },
    },
    ingester::{
        index_block,
        parser::{
            parse_transaction,
            state_update::{AccountTransaction, AddressQueueUpdate, StateUpdate, Transaction},
            TreeResolver,
        },
        typedefs::block_info::{BlockInfo, BlockMetadata, TransactionInfo},
    },
};
use rstest::rstest;
use serde_json::Value;
use serial_test::serial;
use solana_client::{
    client_error::{ClientErrorKind, Result as RpcResult},
    nonblocking::rpc_client::RpcClient,
    rpc_client::RpcClientConfig,
    rpc_request::RpcRequest,
    rpc_sender::{RpcSender, RpcTransportStats},
};
use solana_commitment_config::CommitmentConfig;
use solana_signature::Signature;
use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;
use std::{collections::HashMap, str::FromStr, sync::Arc};

const STATE_QUEUE_TREE: &str = "HLKs5NJ8FXkJg8BrzJt56adFYYuwg5etzDtBbQYTsixu";
const STATE_QUEUE: &str = "6L7SzhYB3anwEQ9cphpJ1U7Scwj57bx2xueReg7R9cKU";
const ADDRESS_QUEUE_TREE: &str = "EzKE84aVTkCUhDHLELqyJaq1Y7UVVmqxXqZjVHwHY3rK";
const V1_QUEUE: &str = "nfq1NvQDJ2GEgnS8zt9prAe8rjjpAW1zFkrvZoBR148";

fn repetitions() -> usize {
    let count = std::env::var("PHOTON_PROFILE_REPETITIONS")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(20);
    assert!(count > 0, "Profiling workloads must execute at least once");
    count
}

// Responses repeat without network access or one-shot mock depletion.
struct FixtureRpcSender {
    slot: u64,
    transactions: HashMap<String, Value>,
}

#[async_trait]
impl RpcSender for FixtureRpcSender {
    async fn send(&self, request: RpcRequest, params: Value) -> RpcResult<Value> {
        match request {
            RpcRequest::GetSlot => Ok(self.slot.into()),
            RpcRequest::GetTransaction => params
                .get(0)
                .and_then(Value::as_str)
                .and_then(|signature| self.transactions.get(signature))
                .cloned()
                .ok_or_else(|| {
                    ClientErrorKind::Custom(format!(
                        "Transaction missing from profiling fixtures: {params}"
                    ))
                    .into()
                }),
            other => Err(ClientErrorKind::Custom(format!(
                "Unexpected RPC request in offline profiling workload: {other}"
            ))
            .into()),
        }
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        RpcTransportStats::default()
    }

    fn url(&self) -> String {
        "fixture-rpc://offline".to_string()
    }
}

fn fixture_client(transactions: HashMap<String, Value>) -> Arc<RpcClient> {
    Arc::new(RpcClient::new_sender(
        FixtureRpcSender {
            slot: 0,
            transactions,
        },
        RpcClientConfig::with_commitment(CommitmentConfig::confirmed()),
    ))
}

fn assert_merkle_root(proof: &GetCompressedAccountProofResponseValue) {
    let mut node = proof.hash.0;
    let mut index = proof.leaf_index;
    for sibling in &proof.proof {
        node = if index & 1 == 0 {
            Poseidon::hashv(&[node.as_slice(), sibling.0.as_slice()]).unwrap()
        } else {
            Poseidon::hashv(&[sibling.0.as_slice(), node.as_slice()]).unwrap()
        };
        index >>= 1;
    }
    assert_eq!(Hash(node), proof.root, "Merkle path must produce its root");
}

#[rstest]
#[tokio::test]
#[serial]
async fn api_profile_missing_database_wrappers(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
) {
    let setup = setup_with_options(
        "api_profile_missing_database_wrappers".to_string(),
        TestSetupOptions {
            network: Network::Localnet,
            db_backend: backend,
        },
    )
    .await;
    let rpc = fixture_client(HashMap::new());
    index_block(
        setup.db_conn.as_ref(),
        &BlockInfo {
            metadata: BlockMetadata {
                slot: 0,
                ..Default::default()
            },
            ..Default::default()
        },
        rpc.as_ref(),
    )
    .await
    .unwrap();

    let tree = SerializablePubkey::try_from(TEST_STATE_TREE_V1_1).unwrap();
    let queue = SerializablePubkey::try_from(V1_QUEUE).unwrap();
    let address = SerializablePubkey::from([3; 32]);
    let signature = Signature::from([9; 64]);
    let mut update = StateUpdate::default();
    for (leaf_index, hash) in [(0, Hash::from([1; 32])), (1, Hash::from([4; 32]))] {
        update.out_accounts.push(AccountWithContext {
            account: Account {
                hash: hash.clone(),
                address: (leaf_index == 0).then_some(address),
                data: None,
                owner: [2; 32].into(),
                lamports: UnsignedInteger(1000),
                tree,
                leaf_index: UnsignedInteger(leaf_index),
                seq: Some(UnsignedInteger(leaf_index + 1)),
                slot_created: UnsignedInteger(0),
            },
            context: AccountContext {
                queue,
                tree_type: TreeType::StateV1 as u16,
                ..Default::default()
            },
        });
        update
            .account_transactions
            .insert(AccountTransaction { hash, signature });
    }
    update.transactions.insert(Transaction {
        signature,
        slot: 0,
        uses_compression: true,
        error: None,
    });
    update.out_accounts.push(AccountWithContext {
        account: Account {
            hash: [5; 32].into(),
            address: None,
            data: None,
            owner: [2; 32].into(),
            lamports: UnsignedInteger(1000),
            tree: SerializablePubkey::try_from(STATE_QUEUE_TREE).unwrap(),
            leaf_index: UnsignedInteger(0),
            seq: None,
            slot_created: UnsignedInteger(0),
        },
        context: AccountContext {
            queue: SerializablePubkey::try_from(STATE_QUEUE).unwrap(),
            tree_type: TreeType::StateV2 as u16,
            in_output_queue: true,
            ..Default::default()
        },
    });
    update.batch_new_addresses.push(AddressQueueUpdate {
        tree: SerializablePubkey::try_from(ADDRESS_QUEUE_TREE).unwrap(),
        address: [6; 32],
        queue_index: 0,
    });
    persist_state_update_using_connection(setup.db_conn.as_ref(), update)
        .await
        .unwrap();
    let api = PhotonApi::new(setup.db_conn.clone(), rpc, setup.prover_url.clone(), None);
    let expected_signature = SignatureInfo {
        signature: SerializableSignature(signature),
        slot: UnsignedInteger(0),
        block_time: UnixTimestamp(0),
    };

    for _ in 0..repetitions() {
        api.liveness().await.unwrap();
        api.readiness().await.unwrap();
        assert_eq!(api.get_indexer_health().await.unwrap(), "ok");
        let proof = api
            .get_compressed_account_proof(HashRequest {
                hash: [1; 32].into(),
            })
            .await
            .unwrap();
        assert_eq!(
            (
                proof.context.slot,
                proof.value.hash.clone(),
                proof.value.merkle_tree,
                proof.value.leaf_index,
                proof.value.root_seq
            ),
            (0, Hash::from([1; 32]), tree, 0, 2)
        );
        assert_merkle_root(&proof.value);
        let proof_v2 = api
            .get_compressed_account_proof_v2(HashRequest {
                hash: [1; 32].into(),
            })
            .await
            .unwrap();
        let expected_v2 = GetCompressedAccountProofResponseValueV2 {
            proof: proof.value.proof,
            root: proof.value.root,
            leaf_index: proof.value.leaf_index,
            hash: proof.value.hash,
            root_seq: proof.value.root_seq,
            prove_by_index: false,
            tree_context: TreeContextInfo {
                tree_type: TreeType::StateV1 as u16,
                tree,
                queue,
                cpi_context: None,
            },
        };
        assert_eq!(proof_v2.value, expected_v2);
        let multiple_v2 = api
            .get_multiple_compressed_account_proofs_v2(HashList(vec![[1; 32].into()]))
            .await
            .unwrap();
        assert_eq!(multiple_v2.value, vec![expected_v2]);
        let account_signatures = api
            .get_compression_signatures_for_account(HashRequest {
                hash: [1; 32].into(),
            })
            .await
            .unwrap();
        assert_eq!(
            account_signatures.value.items,
            vec![expected_signature.clone()]
        );
        let address_signatures = api
            .get_compression_signatures_for_address(GetCompressionSignaturesForAddressRequest {
                address,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            address_signatures.value.items,
            vec![expected_signature.clone()]
        );
        let queues = api
            .get_queue_info(GetQueueInfoRequest {
                trees: Some(vec![
                    STATE_QUEUE_TREE.to_string(),
                    ADDRESS_QUEUE_TREE.to_string(),
                ]),
            })
            .await
            .unwrap();
        let mut actual = queues
            .queues
            .into_iter()
            .map(|queue| (queue.tree, queue.queue_type, queue.queue_size))
            .collect::<Vec<_>>();
        actual.sort();
        let mut expected = vec![
            (
                STATE_QUEUE_TREE.to_string(),
                QueueType::OutputStateV2 as u8,
                1,
            ),
            (
                STATE_QUEUE_TREE.to_string(),
                QueueType::InputStateV2 as u8,
                0,
            ),
            (
                ADDRESS_QUEUE_TREE.to_string(),
                QueueType::AddressV2 as u8,
                1,
            ),
        ];
        expected.sort();
        assert_eq!((queues.slot, actual), (0, expected));
    }
}

fn transaction_fixture(name: &str, signature: &str) -> Value {
    let path = relative_project_path(&format!("tests/data/transactions/{name}/{signature}"));
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[rstest]
#[tokio::test]
#[serial]
async fn api_profile_transaction_wrappers_from_cached_fixtures(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
) {
    let setup = setup_with_options(
        "api_profile_transaction_wrappers_from_cached_fixtures".to_string(),
        TestSetupOptions {
            network: Network::Localnet,
            db_backend: backend,
        },
    )
    .await;
    let mint =
        "64jFxW4xxife8UqpEQyYdA589rzFKWx8rT4vun6soCpWXmDermr588UKX2261YYZ8ZSzcu2NUJY2aCfee64FvTW4";
    let transfer =
        "5UZuijpSnqBpgMpqnggxZ58bzZ8aJtqqVjtvzx2Pg2C58jXph1rdrXyHdTxJfsHawNJyr8syU4U1MEYccDUQeTSV";
    let name = "e2e_mint_and_transfer_transactions";
    let transactions = HashMap::from([
        (mint.to_string(), transaction_fixture(name, mint)),
        (transfer.to_string(), transaction_fixture(name, transfer)),
    ]);
    let rpc = fixture_client(transactions.clone());
    index_block(
        setup.db_conn.as_ref(),
        &BlockInfo {
            metadata: BlockMetadata {
                slot: 0,
                ..Default::default()
            },
            ..Default::default()
        },
        rpc.as_ref(),
    )
    .await
    .unwrap();
    let mut resolver = TreeResolver::new(rpc.as_ref());
    for signature in [mint, transfer] {
        let transaction: EncodedConfirmedTransactionWithStatusMeta =
            serde_json::from_value(transactions.get(signature).unwrap().clone()).unwrap();
        let info: TransactionInfo = transaction.try_into().unwrap();
        let update = parse_transaction(setup.db_conn.as_ref(), &info, 0, &mut resolver)
            .await
            .unwrap();
        assert!(
            !update.out_accounts.is_empty(),
            "Fixture replay must retain compression state"
        );
        persist_state_update_using_connection(setup.db_conn.as_ref(), update)
            .await
            .unwrap();
    }
    let signature = SerializableSignature(Signature::from_str(transfer).unwrap());
    let transaction = || {
        serde_json::from_value::<EncodedConfirmedTransactionWithStatusMeta>(
            transactions.get(transfer).unwrap().clone(),
        )
        .unwrap()
    };
    let expected = get_transaction_helper(
        setup.db_conn.as_ref(),
        signature.clone(),
        transaction(),
        rpc.as_ref(),
    )
    .await
    .unwrap();
    let expected_v2 = get_transaction_helper_v2(
        setup.db_conn.as_ref(),
        signature.clone(),
        transaction(),
        rpc.as_ref(),
    )
    .await
    .unwrap();
    assert!(!expected.compressionInfo.openedAccounts.is_empty());
    assert!(!expected.compressionInfo.closedAccounts.is_empty());
    let api = PhotonApi::new(setup.db_conn.clone(), rpc, setup.prover_url.clone(), None);
    for _ in 0..repetitions() {
        let request = GetTransactionRequest {
            signature: signature.clone(),
        };
        assert_eq!(
            api.get_transaction_with_compression_info(request)
                .await
                .unwrap(),
            expected
        );
        let request = GetTransactionRequest {
            signature: signature.clone(),
        };
        assert_eq!(
            api.get_transaction_with_compression_info_v2(request)
                .await
                .unwrap(),
            expected_v2
        );
    }
}
