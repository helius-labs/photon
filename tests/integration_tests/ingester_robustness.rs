use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use borsh::BorshSerialize;
use futures::StreamExt;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Response, Server, StatusCode,
};
use light_compressed_account::TreeType;
use photon_indexer::{
    common::{rpc::with_fallbacks, typedefs::hash::Hash},
    dao::generated::{accounts, blocks},
    ingester::{
        error::IngesterError,
        fetchers::poller::get_block_poller_stream,
        index_block_batch,
        parser::{
            get_compression_program_id,
            indexer_events::{
                CompressedAccount, OutputCompressedAccountWithPackedContext, PublicTransactionEvent,
            },
            TreeResolver, EXPECTED_TREE_OWNER, NOOP_PROGRAM_ID,
        },
        retry::{rpc_error_is_retryable, Backoff},
        typedefs::block_info::{
            BlockInfo, BlockMetadata, Instruction, InstructionGroup, TransactionInfo,
        },
    },
    migration::{Migrator, MigratorTrait},
    monitor::tree_metadata_sync::{upsert_tree_metadata, TreeAccountData},
};
use sea_orm::{ConnectOptions, Database, DatabaseConnection, EntityTrait};
use serde_json::{json, Value};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_pubkey::Pubkey;
use solana_signature::Signature;

struct Reply {
    status: StatusCode,
    body: Value,
    delay: Duration,
}
impl Reply {
    fn result(result: Value) -> Self {
        Self {
            status: StatusCode::OK,
            body: json!({"jsonrpc":"2.0", "result":result}),
            delay: Duration::ZERO,
        }
    }
    fn error(code: i64) -> Self {
        Self {
            body: json!({"jsonrpc":"2.0", "error":{"code":code, "message":"injected failure"}}),
            ..Self::result(Value::Null)
        }
    }
    fn http_error(status: StatusCode) -> Self {
        Self {
            status,
            body: json!({"error":"injected failure"}),
            ..Self::result(Value::Null)
        }
    }
}

struct MockRpc {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    handle: tokio::task::JoinHandle<()>,
}
impl Drop for MockRpc {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
impl MockRpc {
    fn start(handler: impl Fn(&Value) -> Reply + Send + Sync + 'static) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handler = Arc::new(handler);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let services = make_service_fn({
            let requests = requests.clone();
            move |_| {
                let (handler, requests) = (handler.clone(), requests.clone());
                async move {
                    Ok::<_, Infallible>(service_fn(move |request| {
                        let (handler, requests) = (handler.clone(), requests.clone());
                        async move {
                            let request: Value = serde_json::from_slice(
                                &hyper::body::to_bytes(request.into_body()).await.unwrap(),
                            )
                            .unwrap();
                            requests.lock().unwrap().push(request.clone());
                            let mut reply = handler(&request);
                            reply.body["id"] = request["id"].clone();
                            tokio::time::sleep(reply.delay).await;
                            let response = Response::builder().status(reply.status);
                            Ok::<_, Infallible>(
                                response.body(Body::from(reply.body.to_string())).unwrap(),
                            )
                        }
                    }))
                }
            }
        });
        let server = Server::from_tcp(listener).unwrap().serve(services);
        let handle = tokio::spawn(async move {
            server.await.unwrap();
        });
        Self {
            url,
            requests,
            handle,
        }
    }
    fn client(&self) -> Arc<RpcClient> {
        Arc::new(RpcClient::new_with_timeout(
            self.url.clone(),
            Duration::from_secs(3),
        ))
    }
    fn fetched_slots(&self) -> Vec<u64> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "getBlock")
            .map(|r| r["params"][0].as_u64().unwrap())
            .collect()
    }
}

fn failover(urls: Vec<String>) -> Arc<RpcClient> {
    let primary = Arc::new(RpcClient::new_with_timeout(
        urls[0].clone(),
        Duration::from_secs(3),
    ));
    with_fallbacks(primary, &urls[1..])
}
fn block_json(slot: u64) -> Value {
    json!({"blockhash": hash(slot).to_base58(), "previousBlockhash":hash(slot-1).to_base58(), "parentSlot":slot-1,
        "blockTime":1700000000, "blockHeight":slot, "transactions":[], "rewards":[]})
}
fn hash(slot: u64) -> Hash {
    Hash([slot as u8; 32])
}
fn block(slot: u64) -> BlockInfo {
    BlockInfo {
        metadata: BlockMetadata {
            slot,
            parent_slot: slot - 1,
            blockhash: hash(slot),
            parent_blockhash: hash(slot - 1),
            block_height: slot,
            block_time: 1700000000,
        },
        transactions: vec![],
    }
}
async fn database() -> DatabaseConnection {
    let mut options = ConnectOptions::new("sqlite::memory:".to_string());
    options.max_connections(1);
    let db = Database::connect(options).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    db
}

fn poller_rpc(
    list: impl Fn() -> Vec<u64> + Send + Sync + 'static,
    fetch: impl Fn(u64) -> Reply + Send + Sync + 'static,
) -> MockRpc {
    MockRpc::start(move |r| match r["method"].as_str().unwrap() {
        "getSlot" => Reply::result(json!(10000)),
        "getBlocks" => Reply::result(json!(list())),
        "getBlock" => fetch(r["params"][0].as_u64().unwrap()),
        _ => panic!("unexpected RPC request"),
    })
}

async fn next_polled_slots(rpc: &MockRpc) -> Vec<u64> {
    let stream = get_block_poller_stream(rpc.client(), 100, 4);
    futures::pin_mut!(stream);
    tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .unwrap()
        .unwrap()
        .iter()
        .map(|block| block.metadata.slot)
        .collect()
}

fn event_block(slot: u64, event: &impl BorshSerialize, public: bool) -> BlockInfo {
    let mut update = block(slot);
    update.transactions.push(TransactionInfo {
        signature: Signature::from([1; 64]),
        error: None,
        instruction_groups: vec![InstructionGroup {
            outer_instruction: Instruction {
                program_id: get_compression_program_id(),
                data: vec![],
                accounts: vec![],
            },
            inner_instructions: {
                let mut instructions = Vec::new();
                if public {
                    instructions.push(Instruction {
                        program_id: Pubkey::default(),
                        data: vec![],
                        accounts: vec![],
                    });
                }
                instructions.push(Instruction {
                    program_id: NOOP_PROGRAM_ID,
                    data: event.try_to_vec().unwrap(),
                    accounts: vec![],
                });
                instructions
            },
        }],
    });
    update
}

#[tokio::test]
async fn unresolved_tree_does_not_commit_and_retry_preserves_output() {
    let rpc = MockRpc::start(|_| Reply::http_error(StatusCode::SERVICE_UNAVAILABLE));
    let client = rpc.client();
    let db = database().await;
    index_block_batch(&db, &vec![block(10)], &client)
        .await
        .unwrap();
    let tree = Pubkey::new_unique();
    let event = PublicTransactionEvent {
        output_compressed_accounts: vec![OutputCompressedAccountWithPackedContext {
            compressed_account: CompressedAccount {
                owner: [4; 32].into(),
                lamports: 100,
                ..Default::default()
            },
            merkle_tree_index: 0,
        }],
        output_compressed_account_hashes: vec![[5; 32]],
        output_leaf_indices: vec![0],
        pubkey_array: vec![tree.to_bytes().into()],
        ..Default::default()
    };
    let update = event_block(11, &event, true);
    let result = index_block_batch(&db, &vec![update.clone()], &client).await;
    assert!(
        matches!(result, Err(IngesterError::RpcError { .. })),
        "unresolved metadata must fail the batch: {result:?}"
    );
    assert_eq!(
        (
            blocks::Entity::find().all(&db).await.unwrap().len(),
            accounts::Entity::find().all(&db).await.unwrap().len()
        ),
        (1, 0),
        "checkpoint and account changes must remain atomic"
    );
    // Simulate a successful metadata refresh after the outage. A retry must
    // still index the output, rather than resuming beyond its block.
    upsert_tree_metadata(
        &db,
        tree,
        TreeType::StateV2,
        &TreeAccountData {
            queue_pubkey: Pubkey::new_unique(),
            height: 32,
            root_history_capacity: 2400,
            sequence_number: 0,
            next_index: 0,
            owner: EXPECTED_TREE_OWNER.unwrap(),
        },
        11,
    )
    .await
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        index_block_batch(&db, &vec![update.clone()], &client),
    )
    .await
    .unwrap()
    .unwrap();
    index_block_batch(&db, &vec![update], &client)
        .await
        .unwrap();
    let outputs = accounts::Entity::find().all(&db).await.unwrap();
    assert_eq!(
        (
            blocks::Entity::find().all(&db).await.unwrap().len(),
            outputs.len(),
            outputs[0].hash.clone()
        ),
        (2, 1, vec![5; 32]),
        "retry/replay must preserve exactly one output"
    );
}

#[tokio::test]
async fn transport_failure_is_not_a_negative_discovery_cache_entry() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let calls = attempts.clone();
    let rpc = MockRpc::start(move |_| {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::http_error(StatusCode::SERVICE_UNAVAILABLE);
        }
        Reply::result(
            json!({"context":{"slot":10}, "value":{"lamports":1,"owner":Pubkey::default().to_string(),"data":["","base64"],"executable":false,"rentEpoch":0}}),
        )
    });
    let client = rpc.client();
    let db = database().await;
    let mut resolver = TreeResolver::new(&client);
    let pubkey = Pubkey::new_unique();
    assert!(resolver.discover_tree(&db, &pubkey, 11).await.is_err());
    assert!(resolver
        .discover_tree(&db, &pubkey, 11)
        .await
        .unwrap()
        .is_none());
    assert!(resolver
        .discover_tree(&db, &pubkey, 11)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "retry failed transport, but cache a confirmed non-tree account"
    );
}

#[tokio::test]
async fn missing_history_fails_over_to_a_provider_that_has_the_block() {
    let primary = MockRpc::start(|_| Reply::error(-32009));
    let archive = MockRpc::start(|r| {
        if r["method"] == "getSlot" {
            Reply::result(json!(10000))
        } else {
            Reply::result(block_json(11))
        }
    });
    let client = failover(vec![primary.url.clone(), archive.url.clone()]);
    assert_eq!(client.get_block(11).await.unwrap().parent_slot, 10);
    assert_eq!(client.get_slot().await.unwrap(), 10000);
    assert_eq!(
        primary.requests.lock().unwrap().len(),
        1,
        "subsequent requests should prefer the healthy endpoint"
    );
}

#[tokio::test]
async fn authentication_errors_are_classified_as_permanent() {
    let rpc = MockRpc::start(|_| Reply::http_error(StatusCode::UNAUTHORIZED));
    let client = failover(vec![rpc.url.clone()]);
    assert!(!rpc_error_is_retryable(
        &client.get_slot().await.unwrap_err()
    ));
}

#[tokio::test]
async fn unavailable_predecessor_bounds_fetch_ahead() {
    let rpc = poller_rpc(
        || (101..=1000).collect(),
        |slot| Reply {
            delay: if slot == 101 {
                Duration::from_secs(5)
            } else {
                Duration::ZERO
            },
            ..Reply::result(block_json(slot))
        },
    );
    let stream = get_block_poller_stream(rpc.client(), 100, 4);
    futures::pin_mut!(stream);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), stream.next())
            .await
            .is_err()
    );
    let mut slots = rpc.fetched_slots();
    slots.sort_unstable();
    assert_eq!(
        slots,
        vec![101, 102, 103, 104],
        "a stalled predecessor must bound fetch-ahead"
    );
}

#[tokio::test]
async fn omitted_predecessors_are_recovered_from_parent_links() {
    let rpc = poller_rpc(|| vec![103], |slot| Reply::result(block_json(slot)));
    assert_eq!(next_polled_slots(&rpc).await, vec![101, 102, 103]);
}

#[tokio::test]
async fn produced_block_is_retried_after_a_skipped_response() {
    let attempts = AtomicUsize::new(0);
    let rpc = poller_rpc(
        || vec![101, 102],
        move |slot| {
            if slot == 101 && attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Reply::error(-32009)
            } else {
                Reply::result(block_json(slot))
            }
        },
    );
    assert_eq!(next_polled_slots(&rpc).await, vec![101, 102]);
    assert_eq!(
        rpc.fetched_slots()
            .iter()
            .filter(|slot| **slot == 101)
            .count(),
        2
    );
}

#[tokio::test]
async fn replay_rejects_a_different_hash_at_a_committed_slot() {
    let db = database().await;
    let rpc = RpcClient::new("http://127.0.0.1:1".into());
    index_block_batch(&db, &vec![block(10)], &rpc)
        .await
        .unwrap();
    let mut conflicting = block(10);
    conflicting.metadata.blockhash = hash(99);
    assert!(matches!(
        index_block_batch(&db, &vec![conflicting], &rpc).await,
        Err(IngesterError::ParserError(_))
    ));
    let mut child = block(11);
    child.metadata.parent_blockhash = hash(99);
    assert!(index_block_batch(&db, &vec![child], &rpc).await.is_err());
    assert_eq!(blocks::Entity::find().all(&db).await.unwrap().len(), 1);
}

#[test]
fn retry_delays_are_bounded_and_grow_after_failures() {
    let mut backoff = Backoff::default();
    let delays = (0..20).map(|_| backoff.next_delay()).collect::<Vec<_>>();
    assert!((Duration::from_millis(250)..=Duration::from_millis(500)).contains(&delays[0]));
    assert!(delays.iter().all(|delay| *delay <= Duration::from_secs(30)));
    assert!(delays[19] >= Duration::from_secs(15));
}

#[tokio::test]
async fn malformed_rpc_block_recovers_without_panicking_or_skipping() {
    let attempts = AtomicUsize::new(0);
    let rpc = poller_rpc(
        || vec![101],
        move |slot| {
            let mut block = block_json(slot);
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                block["transactions"] = Value::Null;
            }
            Reply::result(block)
        },
    );
    assert_eq!(next_polled_slots(&rpc).await, vec![101]);
    assert_eq!(rpc.fetched_slots(), vec![101, 101]);
}

#[tokio::test]
async fn truncated_compression_accounts_are_errors_without_panicking() {
    let rpc = MockRpc::start(|_| {
        Reply::result(
            json!({"context":{"slot":10}, "value":{"lamports":1,"owner":get_compression_program_id().to_string(),"data":["","base64"],"executable":false,"rentEpoch":0}}),
        )
    });
    let client = rpc.client();
    let db = database().await;
    let mut resolver = TreeResolver::new(&client);
    let pubkey = Pubkey::new_unique();
    assert!(resolver.discover_tree(&db, &pubkey, 11).await.is_err());
    assert!(resolver.discover_tree(&db, &pubkey, 11).await.is_err());
    assert_eq!(
        rpc.requests.lock().unwrap().len(),
        2,
        "malformed program data must not be cached as an excluded tree"
    );
}

#[tokio::test]
async fn database_constraints_are_permanent_but_connection_failures_are_retryable() {
    use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
    let db = Database::connect("sqlite::memory:").await.unwrap();
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "CREATE TABLE t (id INTEGER PRIMARY KEY)".into(),
    ))
    .await
    .unwrap();
    db.execute(Statement::from_string(
        DatabaseBackend::Sqlite,
        "INSERT INTO t VALUES (1)".into(),
    ))
    .await
    .unwrap();
    let error = db
        .execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "INSERT INTO t VALUES (1)".into(),
        ))
        .await
        .unwrap_err();
    assert!(!IngesterError::from(error).is_retryable());
    assert!(IngesterError::from(sea_orm::DbErr::ConnectionAcquire).is_retryable());
}

#[tokio::test]
async fn database_outage_retries_the_batch_after_connection_recovery() {
    use photon_indexer::ingester::index_block_batch_with_infinite_retries;
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};
    let db = MockDatabase::new(DatabaseBackend::Postgres)
        .append_query_errors(vec![sea_orm::DbErr::ConnectionAcquire])
        .append_query_results(vec![
            Vec::<blocks::Model>::new(),
            Vec::<blocks::Model>::new(),
        ])
        .append_exec_results(vec![MockExecResult {
            last_insert_id: 0,
            rows_affected: 1,
        }])
        .into_connection();
    let rpc = RpcClient::new("http://127.0.0.1:1".into());
    tokio::time::timeout(
        Duration::from_secs(3),
        index_block_batch_with_infinite_retries(&db, vec![block(10)], &rpc),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn concurrent_retries_commit_a_block_only_once() {
    use photon_indexer::ingester::index_block_batch_with_infinite_retries;
    let db = database().await;
    let rpc = RpcClient::new("http://127.0.0.1:1".into());
    let (first, second) = tokio::join!(
        index_block_batch_with_infinite_retries(&db, vec![block(10)], &rpc),
        index_block_batch_with_infinite_retries(&db, vec![block(10)], &rpc),
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(blocks::Entity::find().all(&db).await.unwrap().len(), 1);
}

#[tokio::test]
async fn incomplete_full_block_fails_over_without_poisoning_the_checkpoint() {
    let primary = MockRpc::start(|_| {
        let mut block = block_json(11);
        block["transactions"] = Value::Null;
        Reply::result(block)
    });
    let archive = MockRpc::start(|_| Reply::result(block_json(11)));
    let client = failover(vec![primary.url.clone(), archive.url.clone()]);
    assert_eq!(client.get_block(11).await.unwrap().parent_slot, 10);
    assert_eq!(
        (primary.fetched_slots(), archive.fetched_slots()),
        (vec![11], vec![11])
    );
}

#[tokio::test]
async fn transport_errors_do_not_include_endpoint_credentials() {
    let client = failover(vec![
        "http://127.0.0.1:1/?api-key=secret-test-key".into(),
        "http://127.0.0.1:1/".into(),
    ]);
    let error = client.get_slot().await.unwrap_err().to_string();
    assert!(
        !error.contains("secret-test-key"),
        "credential-bearing URLs must be removed from errors"
    );
}

#[tokio::test]
async fn missing_grpc_token_uses_rpc_in_the_running_indexer() {
    use std::{
        fs::File,
        path::PathBuf,
        process::{Child, Command, Stdio},
        time::SystemTime,
    };
    struct RunningIndexer {
        child: Child,
        path: PathBuf,
    }
    impl Drop for RunningIndexer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_file(&self.path);
        }
    }
    let rpc = MockRpc::start(|request| match request["method"].as_str().unwrap() {
        "getSlot" => Reply::result(json!(2)),
        "getGenesisHash" => Reply::result(json!(hash(0).to_base58())),
        "getProgramAccounts" => Reply::result(json!([])),
        "getBlocks" => {
            let start = request["params"][0].as_u64().unwrap();
            Reply::result(json!((start.max(1)..=2).collect::<Vec<_>>()))
        }
        "getBlock" => Reply::result(block_json(request["params"][0].as_u64().unwrap())),
        _ => panic!("unexpected RPC request: {}", request["method"]),
    });
    let name = format!(
        "photon-robustness-{}-{}.db",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let path = std::env::temp_dir().join(name);
    File::create(&path).unwrap();
    let db_url = format!("sqlite://{}", path.display());
    let db = Database::connect(db_url.clone()).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_photon"))
        .args([
            "--rpc-url",
            &rpc.url,
            "--grpc-url",
            "http://127.0.0.1:1",
            "--db-url",
            &db_url,
            "--disable-api",
        ])
        .env_remove("GRPC_X_TOKEN")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut indexer = RunningIndexer { child, path };
    let started = Instant::now();
    let mut indexed = false;
    while started.elapsed() < Duration::from_secs(5) {
        if indexer.child.try_wait().unwrap().is_some() {
            break;
        }
        if blocks::Entity::find().all(&db).await.unwrap().len() == 2 {
            indexed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        indexed,
        "the indexer must commit blocks via RPC when the gRPC token is absent"
    );
}

#[tokio::test]
async fn persistent_large_parent_gaps_yield_a_bounded_prefix() {
    let rpc = poller_rpc(|| vec![200], |slot| Reply::result(block_json(slot)));
    assert_eq!(
        next_polled_slots(&rpc).await,
        (101..=104).collect::<Vec<_>>()
    );
    let starts: Vec<_> = rpc
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r["method"] == "getBlocks")
        .map(|r| r["params"][0].as_u64().unwrap())
        .collect();
    assert_eq!(
        starts,
        vec![101],
        "repair must work even if the slot listing never improves"
    );
}

#[tokio::test]
async fn concurrent_metadata_claim_is_retried_before_applying_state() {
    use sea_orm::{DatabaseBackend, MockDatabase, MockExecResult};
    let db = MockDatabase::new(DatabaseBackend::Postgres)
        .append_query_results(vec![
            Vec::<blocks::Model>::new(),
            Vec::<blocks::Model>::new(),
        ])
        .append_exec_results(vec![MockExecResult {
            last_insert_id: 0,
            rows_affected: 0,
        }])
        .into_connection();
    let rpc = RpcClient::new("http://127.0.0.1:1".into());
    assert!(
        matches!(index_block_batch(&db, &vec![block(10)], &rpc).await,
        Err(IngesterError::DatabaseError(message)) if message.contains("concurrently committed")),
        "an insert conflict after a stale lookup must abort before state effects are replayed"
    );
}

#[tokio::test]
async fn unresolved_tree_only_events_cannot_advance_the_checkpoint() {
    use photon_indexer::ingester::parser::indexer_events::{MerkleTreeEvent, NullifierEvent};
    let rpc = MockRpc::start(|_| Reply::http_error(StatusCode::SERVICE_UNAVAILABLE));
    let db = database().await;
    index_block_batch(&db, &vec![block(10)], &rpc.client())
        .await
        .unwrap();
    let event = MerkleTreeEvent::V2(NullifierEvent {
        id: Pubkey::new_unique().to_bytes(),
        nullified_leaves_indices: vec![0],
        seq: 1,
    });
    let result = index_block_batch(&db, &vec![event_block(11, &event, false)], &rpc.client()).await;
    assert!(
        matches!(result, Err(IngesterError::RpcError { .. })),
        "tree events without outputs must resolve their dependencies"
    );
    assert_eq!(blocks::Entity::find().all(&db).await.unwrap().len(), 1);
}

#[tokio::test]
async fn output_queue_discovery_follows_its_tree_and_retries_rpc_failures() {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use light_batched_merkle_tree::queue::{BatchedQueueAccount, BatchedQueueMetadata};
    use light_merkle_tree_metadata::queue::QueueMetadata;
    let (queue, tree) = (Pubkey::new_unique(), Pubkey::new_unique());
    let metadata = QueueMetadata {
        associated_merkle_tree: tree.to_bytes().into(),
        queue_type: light_compressed_account::OUTPUT_STATE_QUEUE_TYPE_V2,
        ..Default::default()
    };
    let mut queue_meta = BatchedQueueMetadata::default();
    queue_meta
        .init(metadata, 2, 1, 0, 0, &queue.to_bytes().into(), 1 << 32)
        .unwrap();
    let mut data = vec![
        0;
        queue_meta
            .batch_metadata
            .queue_account_size(metadata.queue_type)
            .unwrap()
    ];
    BatchedQueueAccount::init(
        &mut data,
        metadata,
        2,
        1,
        0,
        0,
        queue.to_bytes().into(),
        1 << 32,
    )
    .unwrap();
    let rpc = MockRpc::start(move |request| {
        if request["params"][0] == queue.to_string() {
            Reply::result(
                json!({"context":{"slot":10}, "value":{"lamports":1,"owner":get_compression_program_id().to_string(),"data":[STANDARD.encode(&data),"base64"],"executable":false,"rentEpoch":0}}),
            )
        } else {
            Reply::http_error(StatusCode::SERVICE_UNAVAILABLE)
        }
    });
    let client = rpc.client();
    let db = database().await;
    let mut resolver = TreeResolver::new(&client);
    for _ in 0..2 {
        assert!(matches!(
            resolver.discover_tree(&db, &queue, 11).await,
            Err(IngesterError::RpcError { .. })
        ));
    }
    let requested: Vec<_> = rpc
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|r| r["params"][0].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        requested,
        vec![
            queue.to_string(),
            tree.to_string(),
            queue.to_string(),
            tree.to_string()
        ],
        "a queue must not be cached as an excluded tree"
    );
}

#[tokio::test]
async fn failover_does_not_replay_mutating_requests() {
    use solana_client::rpc_request::RpcRequest;
    let primary = MockRpc::start(|_| Reply::http_error(StatusCode::SERVICE_UNAVAILABLE));
    let secondary = MockRpc::start(|_| Reply::result(json!("unused")));
    let client = failover(vec![primary.url.clone(), secondary.url.clone()]);
    assert!(client
        .send::<Value>(
            RpcRequest::RequestAirdrop,
            json!([Pubkey::default().to_string(), 1])
        )
        .await
        .is_err());
    assert_eq!(
        (
            primary.requests.lock().unwrap().len(),
            secondary.requests.lock().unwrap().len()
        ),
        (1, 0)
    );
}

#[tokio::test]
async fn configured_grpc_connection_failure_keeps_rpc_fallback_available() {
    use photon_indexer::ingester::fetchers::grpc::get_grpc_stream_with_rpc_fallback;
    let rpc = poller_rpc(|| vec![101], |slot| Reply::result(block_json(slot)));
    let source = get_grpc_stream_with_rpc_fallback(
        "http://127.0.0.1:1".into(),
        "test-token".into(),
        rpc.client(),
        100,
        4,
    );
    futures::pin_mut!(source);
    let blocks = tokio::time::timeout(Duration::from_secs(3), source.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        blocks
            .iter()
            .map(|block| block.metadata.slot)
            .collect::<Vec<_>>(),
        vec![101]
    );
}
