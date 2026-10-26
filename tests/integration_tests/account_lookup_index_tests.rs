use photon_indexer::{
    api::method::{
        get_compressed_account::{get_compressed_account, get_compressed_account_v2},
        get_compressed_account_balance::get_compressed_account_balance,
        get_compressed_accounts_by_owner::{
            get_compressed_accounts_by_owner, get_compressed_accounts_by_owner_v2,
            GetCompressedAccountsByOwnerRequest,
        },
        get_multiple_compressed_accounts::{
            get_multiple_compressed_accounts, get_multiple_compressed_accounts_v2,
            GetMultipleCompressedAccountsRequest,
        },
        utils::CompressedAccountRequest,
    },
    migration::{Migrator, MigratorTrait, SchemaManager},
};
use rstest::rstest;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, Value};

use crate::keyset_cursor_tests::{add_account, fixture};

const INDEX_NAMES: [&str; 2] = [
    "accounts_active_owner_leaf_hash_idx",
    "accounts_address_spent_lookup_idx",
];

async fn responses(conn: &DatabaseConnection) -> anyhow::Result<Vec<serde_json::Value>> {
    let owner_request = GetCompressedAccountsByOwnerRequest {
        owner: [1; 32].into(),
        ..Default::default()
    };
    let address_request = CompressedAccountRequest {
        address: Some([9; 32].into()),
        hash: None,
    };
    let multiple_request = GetMultipleCompressedAccountsRequest {
        hashes: None,
        addresses: Some(vec![
            [9; 32].into(),
            [10; 32].into(),
            [99; 32].into(),
            [9; 32].into(),
        ]),
    };
    Ok(vec![
        serde_json::to_value(get_compressed_accounts_by_owner(conn, owner_request.clone()).await?)?,
        serde_json::to_value(get_compressed_accounts_by_owner_v2(conn, owner_request).await?)?,
        serde_json::to_value(get_compressed_account(conn, address_request.clone()).await?)?,
        serde_json::to_value(get_compressed_account_v2(conn, address_request.clone()).await?)?,
        serde_json::to_value(get_compressed_account_balance(conn, address_request).await?)?,
        serde_json::to_value(
            get_multiple_compressed_accounts(conn, multiple_request.clone()).await?,
        )?,
        serde_json::to_value(get_multiple_compressed_accounts_v2(conn, multiple_request).await?)?,
    ])
}

async fn index_columns(conn: &DatabaseConnection, name: &str) -> anyhow::Result<Vec<String>> {
    let backend = conn.get_database_backend();
    let statement = if backend == DatabaseBackend::Postgres {
        Statement::from_sql_and_values(
            backend,
            "SELECT a.attname::text AS name FROM pg_index i
             CROSS JOIN LATERAL unnest(i.indkey::smallint[]) WITH ORDINALITY AS k(attnum,position)
             JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
             WHERE i.indexrelid=to_regclass($1) ORDER BY k.position",
            [name.into()],
        )
    } else {
        Statement::from_string(backend, format!("PRAGMA index_info('{name}')"))
    };
    Ok(conn
        .query_all(statement)
        .await?
        .into_iter()
        .map(|row| row.try_get("", "name"))
        .collect::<Result<Vec<_>, _>>()?)
}

#[rstest]
#[tokio::test]
async fn lookup_indexes_preserve_full_api_results_and_reverse_cleanly(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
) -> anyhow::Result<()> {
    let conn = fixture(backend).await?;
    for (hash, owner, spent, address) in [
        (1_u8, 1_u8, false, 9_u8),
        (2, 1, false, 10),
        (3, 1, true, 9),
        (4, 2, false, 11),
        (5, 1, false, 12),
    ] {
        add_account(&conn, hash, owner, spent).await?;
        conn.execute(Statement::from_sql_and_values(backend,
            "UPDATE accounts SET address=$1, leaf_index=$2, data=$3, data_hash=$4, discriminator=$5 WHERE hash=$6",
            [vec![address;32].into(),(10_i64-hash as i64).into(),vec![7_u8;500].into(),vec![6_u8;32].into(),1_i64.into(),vec![hash;32].into()],
        )).await?;
    }
    let before = responses(&conn).await?;
    let migration = Migrator::migrations()
        .into_iter()
        .find(|migration| migration.name() == "m20261026_000002_account_lookup_indexes")
        .ok_or_else(|| anyhow::anyhow!("Account lookup index migration is not registered"))?;
    let manager = SchemaManager::new(&conn);
    migration.up(&manager).await?;
    migration.up(&manager).await?;
    let mut columns = Vec::new();
    let mut index_oids = Vec::new();
    for name in INDEX_NAMES {
        columns.push(index_columns(&conn, name).await?);
        if backend == DatabaseBackend::Postgres {
            let row = conn
                .query_one(Statement::from_sql_and_values(
                    backend,
                    "SELECT to_regclass($1)::oid::bigint AS index_id",
                    [name.into()],
                ))
                .await?
                .ok_or_else(|| anyhow::anyhow!("Index metadata missing"))?;
            index_oids.push(row.try_get::<i64>("", "index_id")?);
        }
    }
    assert_eq!(
        columns,
        vec![
            vec!["owner", "leaf_index", "hash"],
            vec!["address", "spent"]
        ],
        "Lookup indexes must match the existing ordering and current-address predicates"
    );
    assert_eq!(responses(&conn).await?,before,
        "Indexes must retain full response data, missing/duplicate address positions, spent exclusion and cursor bytes");
    migration.down(&manager).await?;
    migration.down(&manager).await?;
    let remaining = if backend == DatabaseBackend::Postgres {
        conn.query_one(Statement::from_sql_and_values(
            backend,
            "SELECT count(*)::bigint AS count FROM pg_class WHERE oid::bigint IN ($1,$2)",
            index_oids.into_iter().map(Value::from),
        ))
        .await?
        .ok_or_else(|| anyhow::anyhow!("Index count missing"))?
        .try_get::<i64>("", "count")?
    } else {
        conn.query_one(Statement::from_sql_and_values(backend,
            "SELECT count(*) AS count FROM sqlite_temp_master WHERE type='index' AND name IN ($1,$2)",
                INDEX_NAMES.into_iter().map(Value::from),
        )).await?.ok_or_else(|| anyhow::anyhow!("Index count missing"))?.try_get::<i64>("","count")?
    };
    assert_eq!(
        remaining, 0,
        "Migration rollback must remove exactly the new lookup indexes"
    );
    assert_eq!(
        responses(&conn).await?,
        before,
        "Rollback must preserve API behavior"
    );
    Ok(())
}
