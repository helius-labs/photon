use std::{env, sync::Mutex};

use lazy_static::lazy_static;
use parser::bundle::Hash;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbBackend, DbErr, ExecResult, SqlxPostgresConnector,
    Statement,
};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use tracing_subscriber::fmt;

lazy_static! {
    static ref ONE_TIME_SETUP: Mutex<bool> = {
        // One time test setup goes here:
        let env_filter = env::var("RUST_LOG")
            .unwrap_or("debug,sqlx::off"
            .to_string());
        let t = tracing_subscriber::fmt().with_env_filter(env_filter);
        t.event_format(fmt::format::json()).init();
        Mutex::new(true)
    };
}

pub struct TestSetup {
    pub db_conn: DatabaseConnection,
}

pub async fn setup() -> TestSetup {
    // Accessing the SETUP for the first time triggers the setup code
    let _ = &ONE_TIME_SETUP;

    let local_db = "postgres://postgres@localhost/postgres";
    let pool = setup_pg_pool(local_db.to_string()).await;
    let db_conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool);
    reset_tables(&db_conn).await.unwrap();
    TestSetup { db_conn }
}

pub async fn setup_pg_pool(database_url: String) -> PgPool {
    let options: PgConnectOptions = database_url.parse().unwrap();
    PgPoolOptions::new()
        .min_connections(1)
        .connect_with(options)
        .await
        .unwrap()
}

pub async fn reset_tables(conn: &DatabaseConnection) -> Result<(), DbErr> {
    for table in vec!["state_trees", "utxos"] {
        truncate_table(conn, table.to_string()).await?;
    }
    Ok(())
}

pub async fn truncate_table(conn: &DatabaseConnection, table: String) -> Result<ExecResult, DbErr> {
    conn.execute(Statement::from_string(
        DbBackend::Postgres,
        format!("TRUNCATE TABLE {} CASCADE", table),
    ))
    .await
}

pub fn mock_str_to_hash(input: &str) -> Hash {
    let mut array = [0u8; 32];
    let bytes = input.as_bytes();

    for (i, &byte) in bytes.iter().enumerate().take(32) {
        array[i] = byte;
    }

    Hash::new(array)
}
