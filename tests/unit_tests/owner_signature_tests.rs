use super::{compute_raw_sql_query_and_args, SignatureFilter, SignatureSearchType};
use crate::{
    common::typedefs::serializable_pubkey::SerializablePubkey,
    migration::{Migrator, MigratorTrait},
};
use sea_orm::{ConnectionTrait, Database, Statement};
use solana_pubkey::Pubkey;

#[tokio::test]
async fn sqlite_absent_owner_signatures_avoid_scanning_transactions() {
    let connection = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&connection, None).await.unwrap();
    let backend = connection.get_database_backend();
    let (sql, args) = compute_raw_sql_query_and_args(
        backend,
        SignatureSearchType::Standard,
        Some(SignatureFilter::Owner(SerializablePubkey(
            Pubkey::new_from_array([1; 32]),
        ))),
        true,
        None,
        1000,
    )
    .unwrap();
    let plan: Vec<String> = connection
        .query_all(Statement::from_sql_and_values(
            backend,
            &format!("EXPLAIN QUERY PLAN {sql}"),
            args,
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get("", "detail").unwrap())
        .collect();

    assert!(
        !plan
            .iter()
            .any(|step| step.starts_with("SCAN transactions")),
        "An absent owner's history must not scan unrelated transactions: {plan:?}"
    );
}
