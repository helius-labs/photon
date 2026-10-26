use light_hasher::Poseidon;
use light_merkle_tree_reference::MerkleTree;
use photon_indexer::{
    api::error::PhotonApiError,
    common::typedefs::{hash::Hash, serializable_pubkey::SerializablePubkey},
    ingester::persist::{
        get_multiple_compressed_leaf_proofs, get_multiple_compressed_leaf_proofs_by_indices,
        persist_leaf_nodes, LeafNode, MerkleProofWithContext,
    },
};
use rstest::rstest;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection,
    SqlxPostgresConnector, Statement, TransactionTrait,
};
use sqlx::postgres::PgPoolOptions;

const TREE_DEPTH: usize = 32;

fn tree_key(id: u64) -> SerializablePubkey {
    let mut bytes = [0; 32];
    bytes
        .get_mut(..8)
        .unwrap()
        .copy_from_slice(&id.to_le_bytes());
    bytes.into()
}

fn leaf_hash(tree: u64, index: u64) -> Hash {
    let mut bytes = [0; 32];
    bytes
        .get_mut(16..24)
        .unwrap()
        .copy_from_slice(&tree.to_be_bytes());
    bytes
        .get_mut(24..)
        .unwrap()
        .copy_from_slice(&index.to_be_bytes());
    Hash::from(bytes)
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
    for sql in [
        format!("CREATE {temporary}TABLE tree_metadata (tree_pubkey {binary} PRIMARY KEY, queue_pubkey {binary} NOT NULL, tree_type INTEGER NOT NULL, height INTEGER NOT NULL, root_history_capacity BIGINT NOT NULL DEFAULT 1000, sequence_number BIGINT NOT NULL DEFAULT 0, next_index BIGINT NOT NULL DEFAULT 0, last_synced_slot BIGINT NOT NULL DEFAULT 0)"),
        format!("CREATE {temporary}TABLE state_trees (tree {binary} NOT NULL, node_idx BIGINT NOT NULL, leaf_idx BIGINT, level BIGINT NOT NULL, hash {binary} NOT NULL, seq BIGINT, PRIMARY KEY(tree, node_idx))"),
        "CREATE INDEX proof_leaf_hash_idx ON state_trees (hash) WHERE level = 0".to_string(),
    ] {
        db.execute(Statement::from_string(backend, sql)).await?;
    }
    Ok(db)
}

async fn persist_tree(
    db: &DatabaseConnection,
    id: u64,
    root_sequence: Option<i64>,
) -> anyhow::Result<MerkleTree<Poseidon>> {
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO tree_metadata (tree_pubkey, queue_pubkey, tree_type, height) VALUES ($1, $2, 3, $3)",
        [tree_key(id).to_bytes_vec().into(), tree_key(id | (1 << 63)).to_bytes_vec().into(), (TREE_DEPTH as i32).into()],
    )).await?;
    let mut reference = MerkleTree::<Poseidon>::new(TREE_DEPTH, 0);
    let mut leaves = Vec::new();
    for index in 0..2 {
        let hash = leaf_hash(id, index);
        reference
            .append(&hash.0)
            .map_err(|error| anyhow::anyhow!("{error:?}"))?;
        leaves.push(LeafNode {
            tree: tree_key(id),
            leaf_index: index as u32,
            hash,
            seq: Some(7),
        });
    }
    let transaction = db.begin().await?;
    persist_leaf_nodes(&transaction, leaves, TREE_DEPTH as u32 + 1).await?;
    transaction
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            "UPDATE state_trees SET seq = $1 WHERE tree = $2 AND node_idx = 1",
            [root_sequence.into(), tree_key(id).to_bytes_vec().into()],
        ))
        .await?;
    transaction
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            "UPDATE state_trees SET seq = 999 WHERE tree = $1 AND level = 0",
            [tree_key(id).to_bytes_vec().into()],
        ))
        .await?;
    transaction.commit().await?;
    Ok(reference)
}

type Proof = (Vec<Hash>, Hash, u32, Hash, SerializablePubkey, u64);

fn normalized(proof: MerkleProofWithContext) -> Proof {
    (
        proof.proof,
        proof.root,
        proof.leaf_index,
        proof.hash,
        proof.merkle_tree,
        proof.root_seq,
    )
}

fn expected(
    reference: &MerkleTree<Poseidon>,
    tree: u64,
    leaf_index: u32,
    sequence: Option<i64>,
) -> Proof {
    (
        reference
            .get_proof_of_leaf(leaf_index as usize, true)
            .unwrap()
            .into_iter()
            .map(Hash::from)
            .collect(),
        Hash::from(reference.root()),
        leaf_index,
        if leaf_index < 2 {
            leaf_hash(tree, leaf_index as u64)
        } else {
            Hash::from([0; 32])
        },
        tree_key(tree),
        sequence.unwrap_or(0) as u64,
    )
}

#[rstest]
#[tokio::test]
async fn proof_root_sequence_matches_batched_root_snapshot(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
) -> anyhow::Result<()> {
    let db = database(backend).await?;
    let mut references = Vec::new();
    for (id, sequence) in [
        (1, None),
        (2, Some(0)),
        (3, Some(42)),
        (4, Some(i64::from(u32::MAX) + 42)),
    ] {
        references.push((id, sequence, persist_tree(&db, id, sequence).await?));
    }
    let mut hashes = Vec::new();
    let mut expected_proofs = Vec::new();
    for (id, sequence, reference) in references.iter().rev() {
        for leaf_index in [1, 0] {
            hashes.push(leaf_hash(*id, leaf_index));
            expected_proofs.push(expected(reference, *id, leaf_index as u32, *sequence));
        }
    }
    let transaction = db.begin().await?;
    let proofs = get_multiple_compressed_leaf_proofs(&transaction, hashes).await?;
    for proof in &proofs {
        proof.validate()?;
    }
    assert_eq!(
        proofs.into_iter().map(normalized).collect::<Vec<_>>(),
        expected_proofs
    );
    for (id, sequence, reference) in references {
        let proofs = get_multiple_compressed_leaf_proofs_by_indices(
            &transaction,
            tree_key(id),
            vec![1, 2, 0],
        )
        .await?;
        for proof in &proofs {
            proof.validate()?;
        }
        assert_eq!(
            proofs.into_iter().map(normalized).collect::<Vec<_>>(),
            [1, 2, 0]
                .into_iter()
                .map(|index| expected(&reference, id, index, sequence))
                .collect::<Vec<_>>()
        );
    }
    transaction.commit().await?;
    Ok(())
}

#[rstest]
#[tokio::test]
async fn proof_root_query_removal_preserves_missing_input_errors(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
) -> anyhow::Result<()> {
    let db = database(backend).await?;
    persist_tree(&db, 1, Some(42)).await?;
    let transaction = db.begin().await?;
    let missing_hash =
        get_multiple_compressed_leaf_proofs(&transaction, vec![leaf_hash(1, 0), leaf_hash(99, 0)])
            .await
            .unwrap_err();
    assert_eq!(
        missing_hash,
        PhotonApiError::RecordNotFound(
            "Leaf nodes not found for hashes. Got 1 hashes. Expected 2.".to_string()
        )
    );
    let missing_tree =
        get_multiple_compressed_leaf_proofs_by_indices(&transaction, tree_key(99), vec![0])
            .await
            .unwrap_err();
    assert_eq!(
        missing_tree,
        PhotonApiError::RecordNotFound(format!("Tree info not found for tree: {}", tree_key(99)))
    );
    transaction.commit().await?;
    Ok(())
}
