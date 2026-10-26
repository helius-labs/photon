use photon_indexer::{
    api::method::utils::{
        search_for_signatures, PaginatedSignatureInfoListWithError, SignatureFilter,
        SignatureInfoWithError, SignatureSearchType,
    },
    common::typedefs::{
        limit::Limit, serializable_pubkey::SerializablePubkey,
        serializable_signature::SerializableSignature, unix_timestamp::UnixTimestamp,
        unsigned_integer::UnsignedInteger,
    },
    migration::{MigrationTrait, Migrator, MigratorTrait, SchemaManager},
};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, DbErr,
    RuntimeErr, Statement, TransactionTrait,
};
use solana_pubkey::Pubkey;
use solana_signature::Signature;

const HISTORY_LENGTH: u64 = 1005;

fn owner() -> SerializablePubkey {
    SerializablePubkey(Pubkey::new_from_array([1; 32]))
}

fn owner_index_migration() -> Box<dyn MigrationTrait> {
    Migrator::migrations()
        .into_iter()
        .find(|migration| migration.name() == "m20261026_000001_add_accounts_owner_history_index")
        .unwrap()
}

async fn populate_history(connection: &DatabaseConnection) -> Vec<SignatureInfoWithError> {
    let backend = connection.get_database_backend();
    let binary = if backend == DatabaseBackend::Postgres {
        "BYTEA"
    } else {
        "BLOB"
    };
    for sql in [
        "CREATE TABLE blocks (slot BIGINT PRIMARY KEY, block_time BIGINT NOT NULL)".to_string(),
        format!("CREATE TABLE transactions (signature {binary} PRIMARY KEY, slot BIGINT NOT NULL REFERENCES blocks(slot), uses_compression BOOLEAN NOT NULL, error TEXT)"),
        format!("CREATE TABLE accounts (hash {binary} PRIMARY KEY, owner {binary} NOT NULL, spent BOOLEAN NOT NULL)"),
        "CREATE UNIQUE INDEX accounts_owner_hash_idx ON accounts (spent, owner, hash)".to_string(),
        format!("CREATE TABLE account_transactions (hash {binary} NOT NULL REFERENCES accounts(hash), signature {binary} NOT NULL REFERENCES transactions(signature), PRIMARY KEY (hash, signature))"),
        "CREATE INDEX account_transactions_signature_idx ON account_transactions (signature)".to_string(),
        "CREATE UNIQUE INDEX transactions_slot_signature_idx ON transactions (slot, signature)".to_string(),
    ] {
        connection.execute(Statement::from_string(backend, sql)).await.unwrap();
    }

    let transaction = connection.begin().await.unwrap();
    for (hash, account_owner, spent) in [
        (10u8, 1u8, true),
        (11, 1, false),
        (12, 1, false),
        (13, 2, false),
    ] {
        transaction
            .execute(Statement::from_sql_and_values(
                backend,
                "INSERT INTO accounts (hash, owner, spent) VALUES ($1, $2, $3)",
                [
                    vec![hash; 32].into(),
                    vec![account_owner; 32].into(),
                    spent.into(),
                ],
            ))
            .await
            .unwrap();
    }

    let mut expected = Vec::new();
    for ordinal in 1..=HISTORY_LENGTH + 1 {
        let mut signature = [0u8; 64];
        signature
            .get_mut(..8)
            .unwrap()
            .copy_from_slice(&ordinal.to_be_bytes());
        let slot = 10 + ordinal / 3;
        let block_time = 1_600_000_000 + slot;
        let error = (ordinal % 7 == 0).then(|| "fixture transaction error".to_string());
        transaction
            .execute(Statement::from_sql_and_values(
                backend,
                "INSERT INTO blocks (slot, block_time) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                [(slot as i64).into(), (block_time as i64).into()],
            ))
            .await
            .unwrap();
        transaction.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO transactions (signature, slot, uses_compression, error) VALUES ($1, $2, $3, $4)",
            [signature.to_vec().into(), (slot as i64).into(), true.into(), error.clone().into()],
        )).await.unwrap();

        let hash = if ordinal > HISTORY_LENGTH {
            13
        } else if ordinal % 2 == 0 {
            12
        } else {
            10
        };
        transaction
            .execute(Statement::from_sql_and_values(
                backend,
                "INSERT INTO account_transactions (hash, signature) VALUES ($1, $2)",
                [vec![hash; 32].into(), signature.to_vec().into()],
            ))
            .await
            .unwrap();
        if ordinal % 10 == 0 {
            // The same transaction touches a spent and an unspent account owned by this owner.
            transaction
                .execute(Statement::from_sql_and_values(
                    backend,
                    "INSERT INTO account_transactions (hash, signature) VALUES ($1, $2)",
                    [vec![10u8; 32].into(), signature.to_vec().into()],
                ))
                .await
                .unwrap();
        }
        if ordinal <= HISTORY_LENGTH {
            expected.push(SignatureInfoWithError {
                signature: SerializableSignature(Signature::from(signature)),
                slot: UnsignedInteger(slot),
                block_time: UnixTimestamp(block_time),
                error,
            });
        }
    }
    transaction.commit().await.unwrap();
    expected.reverse();
    expected
}

async fn page(
    connection: &DatabaseConnection,
    account_owner: SerializablePubkey,
    cursor: Option<String>,
    limit: Option<Limit>,
) -> PaginatedSignatureInfoListWithError {
    search_for_signatures(
        connection,
        SignatureSearchType::Standard,
        Some(SignatureFilter::Owner(account_owner)),
        true,
        cursor,
        limit,
    )
    .await
    .unwrap()
}

async fn assert_history(connection: &DatabaseConnection, expected: Vec<SignatureInfoWithError>) {
    let first = page(connection, owner(), None, None).await;
    let expected_first: Vec<_> = expected.iter().take(1000).cloned().collect();
    assert_eq!(
        first.items, expected_first,
        "Default page must retain 1000 unique transactions, including spent history and errors"
    );
    let last_on_page = expected_first.last().unwrap();
    let mut cursor_bytes = last_on_page.slot.0.to_le_bytes().to_vec();
    cursor_bytes.extend_from_slice(last_on_page.signature.0.as_ref());
    let expected_cursor = bs58::encode(cursor_bytes).into_string();
    assert_eq!(
        first.cursor,
        Some(expected_cursor.clone()),
        "Cursor encoding must remain compatible"
    );
    assert_eq!(
        page(connection, owner(), Some(expected_cursor), None).await,
        PaginatedSignatureInfoListWithError {
            items: expected.iter().skip(1000).cloned().collect(),
            cursor: None
        },
        "The next page must include all remaining history exactly once"
    );

    let mut cursor = None;
    let mut paginated = Vec::new();
    for _ in 0..=HISTORY_LENGTH {
        let response = page(connection, owner(), cursor, Some(Limit::new(2).unwrap())).await;
        paginated.extend(response.items);
        cursor = response.cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(cursor, None, "Pagination must terminate");
    assert_eq!(
        paginated, expected,
        "Small pages must preserve ordering across same-slot ties without omissions or duplicates"
    );
    assert_eq!(
        page(
            connection,
            SerializablePubkey(Pubkey::new_from_array([3; 32])),
            None,
            None
        )
        .await,
        PaginatedSignatureInfoListWithError::default(),
        "An absent owner must have empty history"
    );
    assert_eq!(
        page(connection, owner(), None, Some(Limit::new(0).unwrap())).await,
        PaginatedSignatureInfoListWithError::default(),
        "Zero limit behavior must remain unchanged"
    );
}

#[tokio::test]
async fn owner_signature_history_and_index_sqlite() {
    let connection = Database::connect("sqlite::memory:").await.unwrap();
    let expected = populate_history(&connection).await;
    let migration = owner_index_migration();
    let manager = SchemaManager::new(&connection);
    migration.up(&manager).await.unwrap();
    migration.up(&manager).await.unwrap();
    let columns = connection
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA index_info('accounts_owner_history_idx')".to_string(),
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|column| column.try_get::<String>("", "name").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        columns,
        vec!["owner", "hash"],
        "Owner must lead the covering history index"
    );
    assert_history(&connection, expected).await;
    migration.down(&manager).await.unwrap();
    let indexes = connection
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA index_info('accounts_owner_history_idx')".to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(indexes.len(), 0, "Rollback must remove the new index");
}

#[tokio::test]
#[ignore = "Requires a disposable local PostgreSQL database via TEST_DATABASE_URL"]
async fn owner_signature_history_and_index_postgres() {
    let database_url = std::env::var("TEST_DATABASE_URL").unwrap();
    let parsed_url = reqwest::Url::parse(&database_url).unwrap();
    assert!(
        matches!(parsed_url.host_str(), Some("127.0.0.1" | "localhost")),
        "Use a disposable local database"
    );
    let mut options = ConnectOptions::new(database_url);
    options.max_connections(1);
    let connection = Database::connect(options).await.unwrap();
    let schema = format!("owner_signature_test_{}", rand::random::<u64>());
    connection
        .execute(Statement::from_string(
            DatabaseBackend::Postgres,
            format!("CREATE SCHEMA {schema}"),
        ))
        .await
        .unwrap();
    connection
        .execute(Statement::from_string(
            DatabaseBackend::Postgres,
            format!("SET search_path TO {schema}"),
        ))
        .await
        .unwrap();
    let expected = populate_history(&connection).await;
    let migration = owner_index_migration();
    let manager = SchemaManager::new(&connection);
    migration.up(&manager).await.unwrap();
    migration.up(&manager).await.unwrap();
    assert_history(&connection, expected).await;
    migration.down(&manager).await.unwrap();

    let failed_build = connection
        .execute(Statement::from_string(
            DatabaseBackend::Postgres,
            "CREATE UNIQUE INDEX CONCURRENTLY accounts_owner_history_idx ON accounts (owner)"
                .to_string(),
        ))
        .await
        .unwrap_err();
    let failure_code = match &failed_build {
        DbErr::Exec(RuntimeErr::SqlxError(sqlx::Error::Database(error))) => error.code(),
        _ => panic!("Expected a PostgreSQL uniqueness violation: {failed_build}"),
    };
    assert_eq!(
        failure_code.as_deref(),
        Some("23505"),
        "Fixture must leave an invalid concurrent index"
    );
    assert_eq!(migration.up(&manager).await.unwrap_err(),
        DbErr::Custom("Index accounts_owner_history_idx is invalid; drop it concurrently before retrying this migration".to_string()),
        "An invalid index must prevent the migration from being marked successful");
    migration.down(&manager).await.unwrap();
    migration.up(&manager).await.unwrap();
    connection
        .execute(Statement::from_string(
            DatabaseBackend::Postgres,
            "SET search_path TO public".to_string(),
        ))
        .await
        .unwrap();
    connection
        .execute(Statement::from_string(
            DatabaseBackend::Postgres,
            format!("DROP SCHEMA {schema} CASCADE"),
        ))
        .await
        .unwrap();
}
