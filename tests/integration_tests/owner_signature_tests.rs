use photon_indexer::api::{
    error::PhotonApiError,
    method::utils::{search_for_signatures, SignatureFilter, SignatureSearchType},
};
use sea_orm::{DatabaseConnection, DbErr, RuntimeErr, SqlxPostgresConnector};
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
         CREATE TEMP TABLE token_accounts (hash bytea PRIMARY KEY, owner bytea NOT NULL);
         CREATE TEMP TABLE account_transactions (hash bytea, signature bytea, PRIMARY KEY(hash, signature));
         CREATE INDEX ON accounts (spent, owner, hash);
         CREATE TEMP VIEW transactions AS
             SELECT signature, slot, current_setting('plan_cache_mode') AS error FROM transaction_data;
         INSERT INTO blocks VALUES (10, 100), (9, 99);
         INSERT INTO transaction_data VALUES
             (decode(repeat('01', 64), 'hex'), 10),
             (decode(repeat('02', 64), 'hex'), 9),
             (decode(repeat('03', 64), 'hex'), 10);
         INSERT INTO accounts VALUES
             (decode(repeat('01', 32), 'hex'), decode(repeat('01', 32), 'hex'), false, decode(repeat('09', 32), 'hex')),
             (decode(repeat('02', 32), 'hex'), decode(repeat('01', 32), 'hex'), true, decode(repeat('09', 32), 'hex')),
             (decode(repeat('03', 32), 'hex'), decode(repeat('02', 32), 'hex'), false, NULL);
         INSERT INTO token_accounts SELECT hash, owner FROM accounts;
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

const OWNER_STATEMENT: &str = "
    SELECT name, generic_plans, custom_plans FROM pg_prepared_statements
    WHERE regexp_replace(statement, '^[[:space:]]+', '') LIKE 'SELECT DISTINCT transactions.signature,%'
      AND statement LIKE '%AND accounts.spent IN (FALSE, TRUE)%'
";

#[tokio::test]
async fn owner_search_overrides_cached_generic_plans_and_restores_session_mode(
) -> anyhow::Result<()> {
    let (pool, conn) = setup_owner_search().await?;
    let first = search_for_signatures(
        &conn,
        SignatureSearchType::Standard,
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
    let (name, _, _): (String, i64, i64) = sqlx::query_as(OWNER_STATEMENT).fetch_one(&pool).await?;
    let execute = format!(
        "EXECUTE \"{}\" (decode(repeat('01', 32), 'hex'))",
        name.replace('"', "\"\"")
    );
    // Select a generic plan for the exact statement already cached by SQLx.
    pool.execute(execute.as_str()).await?;
    let (_, generic, custom): (String, i64, i64) =
        sqlx::query_as(OWNER_STATEMENT).fetch_one(&pool).await?;
    assert_eq!((generic, custom), (1, 1), "Generic plan must be primed");

    for owner in [1, 1, 1, 1, 1, 1, 2, 2] {
        let response = search_for_signatures(
            &conn,
            SignatureSearchType::Standard,
            Some(SignatureFilter::Owner([owner; 32].into())),
            true,
            None,
            None,
        )
        .await?;
        modes.extend(response.items.into_iter().map(|item| item.error));
    }
    let (_, generic, custom): (String, i64, i64) =
        sqlx::query_as(OWNER_STATEMENT).fetch_one(&pool).await?;
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

#[tokio::test]
async fn owner_search_restores_session_mode_after_sql_error() -> anyhow::Result<()> {
    let (pool, conn) = setup_owner_search().await?;
    pool.execute(
        "DROP VIEW transactions;
         CREATE TEMP VIEW transactions AS
             SELECT signature, slot, (1 / (slot - slot))::text AS error FROM transaction_data;",
    )
    .await?;
    let error = search_for_signatures(
        &conn,
        SignatureSearchType::Standard,
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
    for (search_type, filter) in [
        (
            SignatureSearchType::Token,
            SignatureFilter::Owner([1; 32].into()),
        ),
        (
            SignatureSearchType::Standard,
            SignatureFilter::Address([9; 32].into()),
        ),
    ] {
        let response =
            search_for_signatures(&conn, search_type, Some(filter), true, None, None).await?;
        modes.extend(response.items.into_iter().map(|item| item.error));
    }
    let mode: String = sqlx::query_scalar("SHOW plan_cache_mode")
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        (modes, mode),
        (
            vec![Some("force_generic_plan".to_string()); 4],
            "force_generic_plan".to_string()
        ),
        "Other signature searches must preserve their normal planning behavior"
    );
    Ok(())
}
