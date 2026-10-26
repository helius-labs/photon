use light_compressed_account::{QueueType, TreeType};
use photon_indexer::api::{
    error::PhotonApiError,
    method::get_queue_info::{get_queue_info, GetQueueInfoRequest, GetQueueInfoResponse},
};
use rstest::rstest;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    SqlxPostgresConnector, Statement,
};
use solana_pubkey::Pubkey;
use sqlx::postgres::PgPoolOptions;

fn key(value: u64) -> Pubkey {
    let mut bytes = [0; 32];
    bytes
        .get_mut(..8)
        .unwrap()
        .copy_from_slice(&value.to_le_bytes());
    Pubkey::new_from_array(bytes)
}

async fn database(backend: DatabaseBackend) -> anyhow::Result<DatabaseConnection> {
    let (db, temporary, binary) = match backend {
        DatabaseBackend::Postgres => {
            let url = std::env::var("TEST_DATABASE_URL")?;
            let parsed = reqwest::Url::parse(&url)?;
            assert!(matches!(parsed.host_str(), Some("127.0.0.1" | "localhost")));
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            (
                SqlxPostgresConnector::from_sqlx_postgres_pool(pool),
                "TEMP ",
                "BYTEA",
            )
        }
        DatabaseBackend::Sqlite => {
            let mut options = ConnectOptions::new("sqlite::memory:".to_string());
            options.max_connections(1);
            (Database::connect(options).await?, "", "BLOB")
        }
        _ => panic!("Unsupported test backend"),
    };
    let (false_literal, true_literal) = if backend == DatabaseBackend::Sqlite {
        ("0", "1")
    } else {
        ("FALSE", "TRUE")
    };
    for sql in [
        format!("CREATE {temporary}TABLE blocks (slot BIGINT PRIMARY KEY)"),
        format!("CREATE {temporary}TABLE tree_metadata (tree_pubkey {binary} PRIMARY KEY, queue_pubkey {binary} NOT NULL, tree_type INTEGER NOT NULL, height INTEGER NOT NULL DEFAULT 32, root_history_capacity BIGINT NOT NULL DEFAULT 1000, sequence_number BIGINT NOT NULL DEFAULT 0, next_index BIGINT NOT NULL DEFAULT 0, last_synced_slot BIGINT NOT NULL DEFAULT 0)"),
        format!("CREATE {temporary}TABLE accounts (hash {binary} PRIMARY KEY, data {binary}, data_hash {binary}, address {binary}, owner {binary}, tree {binary} NOT NULL, leaf_index BIGINT DEFAULT 0, seq BIGINT, slot_created BIGINT DEFAULT 0, spent BOOLEAN NOT NULL, prev_spent BOOLEAN, lamports NUMERIC DEFAULT 0, tree_type INTEGER, nullified_in_tree BOOLEAN NOT NULL, nullifier_queue_index BIGINT, in_output_queue BOOLEAN NOT NULL, queue {binary}, nullifier {binary}, tx_hash {binary}, onchain_pubkey {binary}, discriminator_v2 {binary}, discriminator NUMERIC)"),
        format!("CREATE {temporary}TABLE address_queues (tree {binary} NOT NULL, address {binary} PRIMARY KEY, queue_index BIGINT NOT NULL)"),
        format!("CREATE INDEX queue_info_input_idx ON accounts (tree, nullifier_queue_index) WHERE nullified_in_tree = {false_literal} AND nullifier_queue_index IS NOT NULL"),
        format!("CREATE INDEX queue_info_output_idx ON accounts (tree, in_output_queue, leaf_index) WHERE in_output_queue = {true_literal}"),
        "CREATE INDEX queue_info_address_idx ON address_queues (tree, queue_index)".to_string(),
        "INSERT INTO blocks VALUES (17), (41)".to_string(),
    ] {
        db.execute(Statement::from_string(backend, sql)).await?;
    }
    Ok(db)
}

async fn tree(db: &DatabaseConnection, id: u64, tree_type: TreeType) -> anyhow::Result<()> {
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO tree_metadata (tree_pubkey, queue_pubkey, tree_type) VALUES ($1, $2, $3)",
        [
            key(id).to_bytes().to_vec().into(),
            key(10_000 + id).to_bytes().to_vec().into(),
            (tree_type as i32).into(),
        ],
    ))
    .await?;
    Ok(())
}

async fn account(
    db: &DatabaseConnection,
    id: u64,
    tree_id: u64,
    spent: bool,
    input_index: Option<i64>,
    nullified: bool,
    output: bool,
) -> anyhow::Result<()> {
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO accounts (hash, owner, tree, spent, nullifier_queue_index, nullified_in_tree, in_output_queue) VALUES ($1, $1, $2, $3, $4, $5, $6)",
        [key(id).to_bytes().to_vec().into(), key(tree_id).to_bytes().to_vec().into(), spent.into(), input_index.into(), nullified.into(), output.into()],
    )).await?;
    Ok(())
}

async fn address(db: &DatabaseConnection, id: u64, tree_id: u64) -> anyhow::Result<()> {
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO address_queues (tree, address, queue_index) VALUES ($1, $2, $3)",
        [
            key(tree_id).to_bytes().to_vec().into(),
            key(id).to_bytes().to_vec().into(),
            (id as i64).into(),
        ],
    ))
    .await?;
    Ok(())
}

type Queue = (String, String, u8, u64);

fn expected_queue(tree: u64, queue_type: QueueType, size: u64) -> Queue {
    (
        key(tree).to_string(),
        key(10_000 + tree).to_string(),
        queue_type as u8,
        size,
    )
}

fn normalized(response: GetQueueInfoResponse) -> (u64, Vec<Queue>) {
    let mut queues = response
        .queues
        .into_iter()
        .map(|queue| (queue.tree, queue.queue, queue.queue_type, queue.queue_size))
        .collect::<Vec<_>>();
    queues.sort();
    (response.slot, queues)
}

async fn assert_queues(
    db: &DatabaseConnection,
    trees: Option<Vec<String>>,
    slot: u64,
    mut expected: Vec<Queue>,
) -> anyhow::Result<()> {
    expected.sort();
    assert_eq!(
        normalized(get_queue_info(db, GetQueueInfoRequest { trees }).await?),
        (slot, expected)
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn queue_info_preserves_counts_filters_and_freshness(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
) -> anyhow::Result<()> {
    let db = database(backend).await?;
    for (id, kind) in [
        (1, TreeType::StateV2),
        (2, TreeType::AddressV2),
        (3, TreeType::StateV2),
        (4, TreeType::AddressV2),
        (5, TreeType::StateV1),
        (6, TreeType::AddressV1),
    ] {
        tree(&db, id, kind).await?;
    }
    account(&db, 101, 1, false, Some(1), false, true).await?;
    account(&db, 102, 1, true, Some(2), true, true).await?;
    account(&db, 103, 1, true, None, false, false).await?;
    account(&db, 104, 1, true, Some(3), false, false).await?;
    account(&db, 105, 1, false, Some(4), false, false).await?;
    account(&db, 106, 5, true, Some(5), false, true).await?;
    address(&db, 201, 2).await?;
    address(&db, 202, 2).await?;
    address(&db, 203, 6).await?;

    let initial = vec![
        expected_queue(1, QueueType::InputStateV2, 3),
        expected_queue(1, QueueType::OutputStateV2, 2),
        expected_queue(2, QueueType::AddressV2, 2),
        expected_queue(3, QueueType::InputStateV2, 0),
        expected_queue(3, QueueType::OutputStateV2, 0),
        expected_queue(4, QueueType::AddressV2, 0),
    ];
    assert_queues(&db, None, 41, initial.clone()).await?;
    assert_queues(
        &db,
        Some(vec![
            key(2).to_string(),
            key(1).to_string(),
            key(1).to_string(),
            key(99).to_string(),
        ]),
        41,
        initial
            .into_iter()
            .filter(|queue| queue.0 == key(1).to_string() || queue.0 == key(2).to_string())
            .collect(),
    )
    .await?;
    for filter in [
        vec![],
        vec![key(5).to_string(), key(6).to_string()],
        vec![key(99).to_string()],
    ] {
        assert_queues(&db, Some(filter), 41, vec![]).await?;
    }

    db.execute(Statement::from_sql_and_values(
        backend,
        "UPDATE accounts SET nullified_in_tree = TRUE, in_output_queue = FALSE WHERE hash = $1",
        [key(101).to_bytes().to_vec().into()],
    ))
    .await?;
    address(&db, 204, 2).await?;
    db.execute(Statement::from_string(
        backend,
        "INSERT INTO blocks VALUES (42)".to_string(),
    ))
    .await?;
    assert_queues(
        &db,
        Some(vec![key(1).to_string(), key(2).to_string()]),
        42,
        vec![
            expected_queue(1, QueueType::InputStateV2, 2),
            expected_queue(1, QueueType::OutputStateV2, 1),
            expected_queue(2, QueueType::AddressV2, 3),
        ],
    )
    .await?;

    let error = get_queue_info(
        &db,
        GetQueueInfoRequest {
            trees: Some(vec!["invalid pubkey".to_string()]),
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, PhotonApiError::ValidationError(message) if message.starts_with("Invalid pubkey:"))
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn queue_info_batches_large_mixed_tree_sets(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
) -> anyhow::Result<()> {
    let db = database(backend).await?;
    let mut expected = Vec::new();
    for id in 1..=503 {
        if id % 2 == 0 {
            tree(&db, id, TreeType::AddressV2).await?;
            expected.push(expected_queue(
                id,
                QueueType::AddressV2,
                u64::from(id == 502),
            ));
        } else {
            tree(&db, id, TreeType::StateV2).await?;
            let count = u64::from(id == 503);
            expected.push(expected_queue(id, QueueType::InputStateV2, count));
            expected.push(expected_queue(id, QueueType::OutputStateV2, count));
        }
    }
    account(&db, 1001, 503, true, Some(1), false, true).await?;
    address(&db, 1002, 502).await?;
    assert_queues(&db, None, 41, expected).await?;
    Ok(())
}

#[tokio::test]
async fn queue_info_sqlite_uses_existing_partial_indexes() -> anyhow::Result<()> {
    use std::sync::{Arc, Mutex};

    let mut db = database(DatabaseBackend::Sqlite).await?;
    tree(&db, 1, TreeType::StateV2).await?;
    account(&db, 101, 1, false, Some(1), false, true).await?;
    let statements = Arc::new(Mutex::new(Vec::new()));
    let observed = statements.clone();
    db.set_metric_callback(move |info| {
        observed.lock().unwrap().push(info.statement.clone());
    });
    get_queue_info(&db, GetQueueInfoRequest { trees: None }).await?;
    let mut statement = statements
        .lock()
        .unwrap()
        .iter()
        .find(|statement| statement.sql.contains("UNION ALL"))
        .cloned()
        .unwrap();
    statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
    let plan = db
        .query_all(statement)
        .await?
        .into_iter()
        .map(|row| row.try_get::<String>("", "detail"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        (
            plan.iter()
                .any(|step| step.contains("queue_info_input_idx")),
            plan.iter()
                .any(|step| step.contains("queue_info_output_idx")),
        ),
        (true, true),
        "Queue counts must use the numeric partial indexes created by SQLite migrations: {plan:?}"
    );
    Ok(())
}
