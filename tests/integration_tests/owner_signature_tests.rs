use photon_indexer::api::{
    error::PhotonApiError,
    method::utils::{search_for_signatures, SignatureFilter, SignatureSearchType},
};
use photon_indexer::{
    api::method::utils::{PaginatedSignatureInfoListWithError, SignatureInfoWithError},
    common::typedefs::{
        limit::Limit, serializable_signature::SerializableSignature, unix_timestamp::UnixTimestamp,
        unsigned_integer::UnsignedInteger,
    },
};
use rstest::rstest;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, DbErr,
    RuntimeErr, SqlxPostgresConnector, Statement,
};
use solana_signature::Signature;
use sqlx::{postgres::PgPoolOptions, Executor, PgPool};

async fn setup_owner_search() -> anyhow::Result<(PgPool, DatabaseConnection)> {
    let url = std::env::var("TEST_DATABASE_URL")?;
    assert!(
        url.contains("localhost") || url.contains("127.0.0.1"),
        "Owner signature tests require a local PostgreSQL database"
    );
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await?;
    pool.execute(
        "CREATE TEMP TABLE blocks (slot bigint PRIMARY KEY, block_time bigint NOT NULL);
         CREATE TEMP TABLE transaction_data (signature bytea PRIMARY KEY, slot bigint NOT NULL);
         CREATE TEMP TABLE accounts (
             hash bytea PRIMARY KEY, owner bytea NOT NULL, spent boolean NOT NULL, address bytea
         );
         CREATE TEMP TABLE token_accounts (
             hash bytea PRIMARY KEY, owner bytea NOT NULL, spent boolean NOT NULL, mint bytea NOT NULL
         );
         CREATE TEMP TABLE account_transactions (hash bytea, signature bytea, PRIMARY KEY(hash, signature));
         CREATE INDEX ON accounts (spent, owner, hash);
         CREATE INDEX ON token_accounts (spent, owner, mint, hash);
         CREATE TEMP VIEW transactions AS
             SELECT signature, slot, true AS uses_compression,
                    current_setting('plan_cache_mode') AS error FROM transaction_data;
         INSERT INTO blocks VALUES (10, 100), (9, 99);
         INSERT INTO transaction_data VALUES
             (decode(repeat('01', 64), 'hex'), 10),
             (decode(repeat('02', 64), 'hex'), 9),
             (decode(repeat('03', 64), 'hex'), 10);
         INSERT INTO accounts VALUES
             (decode(repeat('01', 32), 'hex'), decode(repeat('01', 32), 'hex'), false, decode(repeat('09', 32), 'hex')),
             (decode(repeat('02', 32), 'hex'), decode(repeat('01', 32), 'hex'), true, decode(repeat('09', 32), 'hex')),
             (decode(repeat('03', 32), 'hex'), decode(repeat('02', 32), 'hex'), false, NULL);
         INSERT INTO token_accounts SELECT hash, owner, spent, decode(repeat('05', 32), 'hex') FROM accounts;
         INSERT INTO account_transactions VALUES
             (decode(repeat('01', 32), 'hex'), decode(repeat('01', 64), 'hex')),
             (decode(repeat('02', 32), 'hex'), decode(repeat('01', 64), 'hex')),
             (decode(repeat('02', 32), 'hex'), decode(repeat('02', 64), 'hex')),
             (decode(repeat('03', 32), 'hex'), decode(repeat('03', 64), 'hex'));
         SET plan_cache_mode = force_generic_plan;",
    )
    .await?;
    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
    Ok((pool, conn))
}

fn owner_statement(search_type: &SignatureSearchType) -> String {
    let table = match search_type {
        SignatureSearchType::Standard => "accounts",
        SignatureSearchType::Token => "token_accounts",
    };
    format!(
        "SELECT name, generic_plans, custom_plans FROM pg_prepared_statements
         WHERE regexp_replace(statement, '^[[:space:]]+', '') LIKE 'SELECT transactions.signature,%'
           AND statement LIKE '%WHERE EXISTS (%'
           AND statement LIKE '%AND {table}.spent IN (FALSE, TRUE)%'"
    )
}

#[rstest]
#[tokio::test]
async fn owner_search_overrides_cached_generic_plans_and_restores_session_mode(
    #[values(SignatureSearchType::Standard, SignatureSearchType::Token)]
    search_type: SignatureSearchType,
) -> anyhow::Result<()> {
    let (pool, conn) = setup_owner_search().await?;
    let statement = owner_statement(&search_type);
    let first = search_for_signatures(
        &conn,
        search_type.clone(),
        Some(SignatureFilter::Owner([1; 32].into())),
        true,
        None,
        None,
    )
    .await?;
    // The fixture view exposes the setting during SELECT, rather than after the transaction.
    let mut modes = first
        .items
        .into_iter()
        .map(|item| item.error)
        .collect::<Vec<_>>();
    let (name, _, _): (String, i64, i64) = sqlx::query_as(&statement).fetch_one(&pool).await?;
    let execute = format!(
        "EXECUTE \"{}\" (decode(repeat('01', 32), 'hex'))",
        name.replace('"', "\"\"")
    );
    // Select a generic plan for the exact statement already cached by SQLx.
    pool.execute(execute.as_str()).await?;
    let (_, generic, custom): (String, i64, i64) =
        sqlx::query_as(&statement).fetch_one(&pool).await?;
    assert_eq!((generic, custom), (1, 1), "Generic plan must be primed");

    for owner in [1, 1, 1, 1, 1, 1, 2, 2] {
        let response = search_for_signatures(
            &conn,
            search_type.clone(),
            Some(SignatureFilter::Owner([owner; 32].into())),
            true,
            None,
            None,
        )
        .await?;
        modes.extend(response.items.into_iter().map(|item| item.error));
    }
    let (_, generic, custom): (String, i64, i64) =
        sqlx::query_as(&statement).fetch_one(&pool).await?;
    let mode: String = sqlx::query_scalar("SHOW plan_cache_mode")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        (modes, generic, custom, mode),
        (
            vec![Some("force_custom_plan".to_string()); 16],
            1,
            9,
            "force_generic_plan".to_string()
        ),
        "Owner searches must use custom plans without changing the pooled session setting"
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn owner_search_restores_session_mode_after_sql_error(
    #[values(SignatureSearchType::Standard, SignatureSearchType::Token)]
    search_type: SignatureSearchType,
) -> anyhow::Result<()> {
    let (pool, conn) = setup_owner_search().await?;
    pool.execute(
        "DROP VIEW transactions;
         CREATE TEMP VIEW transactions AS
             SELECT signature, slot, true AS uses_compression,
                    (1 / (slot - slot))::text AS error FROM transaction_data;",
    )
    .await?;
    let error = search_for_signatures(
        &conn,
        search_type,
        Some(SignatureFilter::Owner([1; 32].into())),
        true,
        None,
        None,
    )
    .await
    .unwrap_err();
    match error {
        PhotonApiError::DatabaseError(DbErr::Query(RuntimeErr::SqlxError(
            sqlx::Error::Database(error),
        ))) => {
            assert_eq!(
                error.code().as_deref(),
                Some("22012"),
                "Expected division by zero"
            );
        }
        other => panic!("Unexpected error: {other}"),
    }
    let mode: String = sqlx::query_scalar("SHOW plan_cache_mode")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        mode, "force_generic_plan",
        "Rollback must restore the session setting"
    );
    Ok(())
}

#[tokio::test]
async fn other_signature_searches_keep_session_plan_mode() -> anyhow::Result<()> {
    let (pool, conn) = setup_owner_search().await?;
    let mut modes = Vec::new();
    for (search_type, filter, only_compressed) in [
        (
            SignatureSearchType::Standard,
            Some(SignatureFilter::Account([1; 32].into())),
            true,
        ),
        (
            SignatureSearchType::Standard,
            Some(SignatureFilter::Address([9; 32].into())),
            true,
        ),
        (SignatureSearchType::Standard, None, true),
        (SignatureSearchType::Standard, None, false),
    ] {
        let response =
            search_for_signatures(&conn, search_type, filter, only_compressed, None, None).await?;
        modes.extend(response.items.into_iter().map(|item| item.error));
    }
    let mode: String = sqlx::query_scalar("SHOW plan_cache_mode")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        (modes, mode),
        (
            vec![Some("force_generic_plan".to_string()); 9],
            "force_generic_plan".to_string()
        ),
        "Other signature searches must preserve their normal planning behavior"
    );
    Ok(())
}

async fn history_fixture(backend: DatabaseBackend) -> anyhow::Result<DatabaseConnection> {
    let url = if backend == DatabaseBackend::Postgres {
        let url = std::env::var("TEST_DATABASE_URL")?;
        assert!(url.contains("127.0.0.1") || url.contains("localhost"));
        url
    } else {
        "sqlite::memory:".to_string()
    };
    let mut options = ConnectOptions::new(url);
    options.max_connections(1);
    let conn = Database::connect(options).await?;
    let binary = if backend == DatabaseBackend::Postgres {
        "BYTEA"
    } else {
        "BLOB"
    };
    for sql in [
        "CREATE TEMP TABLE blocks (slot BIGINT PRIMARY KEY, block_time BIGINT NOT NULL)".to_string(),
        format!("CREATE TEMP TABLE transactions (signature {binary} PRIMARY KEY, slot BIGINT NOT NULL, error TEXT)"),
        format!("CREATE TEMP TABLE accounts (hash {binary} PRIMARY KEY, owner {binary} NOT NULL, spent BOOLEAN NOT NULL)"),
        format!("CREATE TEMP TABLE token_accounts (hash {binary} PRIMARY KEY, owner {binary} NOT NULL, spent BOOLEAN NOT NULL, mint {binary} NOT NULL)"),
        format!("CREATE TEMP TABLE account_transactions (hash {binary}, signature {binary}, PRIMARY KEY(hash,signature))"),
        "CREATE INDEX owner_history_fixture_idx ON accounts (spent,owner,hash)".to_string(),
        "CREATE INDEX token_owner_history_fixture_idx ON token_accounts (spent,owner,mint,hash)".to_string(),
    ] {
        conn.execute(Statement::from_string(backend, sql)).await?;
    }
    Ok(conn)
}

fn history_account_hash(ordinal: u64) -> Vec<u8> {
    let mut hash = [0_u8; 32];
    hash.get_mut(..8)
        .unwrap()
        .copy_from_slice(&ordinal.to_be_bytes());
    hash.to_vec()
}

fn history_cursor(item: &SignatureInfoWithError) -> String {
    bs58::encode(
        [
            item.slot.0.to_le_bytes().to_vec(),
            item.signature.0.as_ref().to_vec(),
        ]
        .concat(),
    )
    .into_string()
}

async fn history_page(
    conn: &DatabaseConnection,
    search_type: &SignatureSearchType,
    owner: u8,
    cursor: Option<String>,
    limit: Option<Limit>,
) -> anyhow::Result<PaginatedSignatureInfoListWithError> {
    Ok(search_for_signatures(
        conn,
        search_type.clone(),
        Some(SignatureFilter::Owner([owner; 32].into())),
        true,
        cursor,
        limit,
    )
    .await?)
}

#[rstest]
#[tokio::test]
async fn owner_history_preserves_default_pages_and_complete_cursor_traversal(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
    #[values(SignatureSearchType::Standard, SignatureSearchType::Token)]
    search_type: SignatureSearchType,
) -> anyhow::Result<()> {
    let conn = history_fixture(backend).await?;
    let owner = match search_type {
        SignatureSearchType::Standard => 9,
        SignatureSearchType::Token => 3,
    };
    let mut expected = Vec::new();
    for ordinal in 1_u64..=1006 {
        let mut signature = [0_u8; 64];
        signature
            .get_mut(..8)
            .unwrap()
            .copy_from_slice(&ordinal.to_be_bytes());
        let slot = 10 + ordinal / 3;
        let error = (ordinal % 7 == 0).then(|| format!("fixture error {ordinal}"));
        conn.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO blocks VALUES ($1,$2) ON CONFLICT DO NOTHING",
            [(slot as i64).into(), (1000_i64 + slot as i64).into()],
        ))
        .await?;
        conn.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO token_accounts VALUES ($1,$2,$3,$4)",
            [
                history_account_hash(ordinal).into(),
                vec![if ordinal == 1006 { 4_u8 } else { 3 }; 32].into(),
                (ordinal < 1005).into(),
                vec![5_u8; 32].into(),
            ],
        ))
        .await?;
        conn.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO transactions VALUES ($1,$2,$3)",
            [
                signature.to_vec().into(),
                (slot as i64).into(),
                error.clone().into(),
            ],
        ))
        .await?;
        conn.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO accounts VALUES ($1,$2,$3)",
            [
                history_account_hash(ordinal).into(),
                vec![if ordinal == 1006 { 8_u8 } else { 9 }; 32].into(),
                (ordinal < 1005).into(),
            ],
        ))
        .await?;
        conn.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO account_transactions VALUES ($1,$2)",
            [
                history_account_hash(ordinal).into(),
                signature.to_vec().into(),
            ],
        ))
        .await?;
        if ordinal > 1 && ordinal <= 1005 {
            // Each transaction consumes the previous account and creates a new one.
            // Each account therefore has only its creation and spending signatures.
            conn.execute(Statement::from_sql_and_values(
                backend,
                "INSERT INTO account_transactions VALUES ($1,$2)",
                [
                    history_account_hash(ordinal - 1).into(),
                    signature.to_vec().into(),
                ],
            ))
            .await?;
        }
        if ordinal <= 1005 {
            expected.push(SignatureInfoWithError {
                signature: SerializableSignature(Signature::from(signature)),
                slot: UnsignedInteger(slot),
                block_time: UnixTimestamp(1000 + slot),
                error,
            });
        }
    }
    expected.reverse();
    let first_items = expected.iter().take(1000).cloned().collect::<Vec<_>>();
    let first_cursor = history_cursor(first_items.last().unwrap());
    assert_eq!(
        history_page(&conn, &search_type, owner, None, None).await?,
        PaginatedSignatureInfoListWithError {
            items: first_items,
            cursor: Some(first_cursor.clone())
        },
        "Default pages must contain 1000 unique transactions across spent and unspent history"
    );
    assert_eq!(
        history_page(&conn, &search_type, owner, Some(first_cursor), None).await?,
        PaginatedSignatureInfoListWithError {
            items: expected.iter().skip(1000).cloned().collect(),
            cursor: None
        },
        "Default pagination must retain the remaining history and its errors"
    );
    let mut cursor = None;
    let mut actual = Vec::new();
    for _ in 0..=expected.len() {
        let response = history_page(
            &conn,
            &search_type,
            owner,
            cursor,
            Some(Limit::new(2).unwrap()),
        )
        .await?;
        actual.extend(response.items);
        cursor = response.cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(
        (actual, cursor),
        (expected.clone(), None),
        "Small pages must preserve strict same-slot ordering without duplicate transactions"
    );
    let exhausted = history_cursor(expected.last().unwrap());
    for (owner, cursor, limit) in [
        (255_u8, None, None),
        (owner, Some(exhausted), None),
        (owner, None, Some(Limit::new(0).unwrap())),
    ] {
        assert_eq!(
            history_page(&conn, &search_type, owner, cursor, limit).await?,
            PaginatedSignatureInfoListWithError::default(),
            "Absent, exhausted and zero-limit requests must keep empty-page behavior"
        );
    }
    Ok(())
}
