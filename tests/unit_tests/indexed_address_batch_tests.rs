use super::*;
use crate::api::method::get_multiple_new_address_proofs::{
    get_multiple_new_address_proofs, get_multiple_new_address_proofs_v2, AddressList,
    AddressListWithTrees, AddressWithTree,
};
use light_sdk_types::constants::ADDRESS_TREE_V1;
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn address(value: u64) -> Vec<u8> {
    let mut bytes = vec![0; 32];
    bytes
        .get_mut(24..)
        .unwrap()
        .copy_from_slice(&value.to_be_bytes());
    bytes
}

async fn database() -> anyhow::Result<DatabaseConnection> {
    let mut options = ConnectOptions::new("sqlite::memory:".to_string());
    options.max_connections(1);
    let conn = Database::connect(options).await?;
    for statement in [
        "CREATE TABLE indexed_trees(tree BLOB NOT NULL,leaf_index BIGINT NOT NULL,value BLOB NOT NULL,next_index BIGINT NOT NULL,next_value BLOB NOT NULL,seq BIGINT,PRIMARY KEY(tree,leaf_index))",
        "CREATE UNIQUE INDEX indexed_trees_tree_value_idx ON indexed_trees(tree,value)",
        "CREATE TABLE state_trees(tree BLOB NOT NULL,node_idx BIGINT NOT NULL,leaf_idx BIGINT,level BIGINT NOT NULL,hash BLOB NOT NULL,seq BIGINT,PRIMARY KEY(tree,node_idx))",
        "CREATE TABLE tree_metadata(tree_pubkey BLOB PRIMARY KEY,queue_pubkey BLOB NOT NULL,tree_type INTEGER NOT NULL,height INTEGER NOT NULL,root_history_capacity BIGINT NOT NULL,sequence_number BIGINT NOT NULL,next_index BIGINT NOT NULL,last_synced_slot BIGINT NOT NULL)",
        "CREATE TABLE address_queues(tree BLOB NOT NULL,address BLOB NOT NULL)",
        "CREATE TABLE blocks(slot BIGINT PRIMARY KEY,block_time BIGINT NOT NULL)",
        "INSERT INTO blocks VALUES(42,1000)",
    ] {
        conn.execute(Statement::from_string(DatabaseBackend::Sqlite,statement.to_string())).await?;
    }
    Ok(conn)
}

async fn insert_range(
    conn: &impl ConnectionTrait,
    model: &indexed_trees::Model,
) -> anyhow::Result<()> {
    conn.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO indexed_trees VALUES($1,$2,$3,$4,$5,$6)",
        [
            model.tree.clone().into(),
            model.leaf_index.into(),
            model.value.clone().into(),
            model.next_index.into(),
            model.next_value.clone().into(),
            model.seq.into(),
        ],
    ))
    .await?;
    Ok(())
}

async fn populated_fixture(
    nodes: usize,
    height: u32,
    tree_type: TreeType,
) -> anyhow::Result<(DatabaseConnection, Vec<indexed_trees::Model>, Hash)> {
    let conn = database().await?;
    let tree = ADDRESS_TREE_V1.to_vec();
    let top = get_top_element(tree.clone());
    let mut models = vec![
        indexed_trees::Model {
            tree: tree.clone(),
            leaf_index: 0,
            value: address(0),
            next_index: 2,
            next_value: address(100000),
            seq: Some(42),
        },
        top.clone(),
    ];
    for ordinal in 1..=nodes {
        models.push(indexed_trees::Model {
            tree: tree.clone(),
            leaf_index: ordinal as i64 + 1,
            value: address(ordinal as u64 * 100000),
            next_index: if ordinal == nodes {
                1
            } else {
                ordinal as i64 + 2
            },
            next_value: if ordinal == nodes {
                top.value.clone()
            } else {
                address((ordinal as u64 + 1) * 100000)
            },
            seq: Some(42),
        });
    }
    let leaves = 2_i64.pow(height - 1);
    assert!(nodes + 2 <= leaves as usize);
    let txn = conn.begin().await?;
    for model in &models {
        insert_range(&txn, model).await?;
    }
    txn.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO tree_metadata VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
        [
            tree.clone().into(),
            vec![8_u8; 32].into(),
            (tree_type as i32).into(),
            (height as i32 - 1).into(),
            32_i64.into(),
            42_i64.into(),
            (nodes as i64 + 2).into(),
            42_i64.into(),
        ],
    ))
    .await?;
    let mut hashes = (leaves..2 * leaves)
        .map(|index| (index, Hash::from(ZERO_BYTES[0])))
        .collect::<HashMap<_, _>>();
    for model in &models {
        hashes.insert(
            leaves + model.leaf_index,
            compute_hash_by_tree_type(model, tree_type)?,
        );
    }
    for index in (1..leaves).rev() {
        let left = hashes
            .get(&(index * 2))
            .ok_or_else(|| anyhow::anyhow!("Missing left child"))?;
        let right = hashes
            .get(&(index * 2 + 1))
            .ok_or_else(|| anyhow::anyhow!("Missing right child"))?;
        let parent = compute_parent_hash(left.to_vec(), right.to_vec())?;
        hashes.insert(index, Hash::try_from(parent)?);
    }
    let root = hashes.get(&1).unwrap().clone();
    for (index, hash) in hashes {
        let depth = 63 - (index as u64).leading_zeros();
        let level = (height - 1 - depth) as i64;
        let leaf = if index >= leaves {
            Some(index - leaves)
        } else {
            None
        };
        txn.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO state_trees VALUES($1,$2,$3,$4,$5,$6)",
            [
                tree.clone().into(),
                index.into(),
                leaf.into(),
                level.into(),
                hash.to_vec().into(),
                42_i64.into(),
            ],
        ))
        .await?;
    }
    txn.commit().await?;
    Ok((conn, models, root))
}

async fn old_lookup(
    conn: &impl ConnectionTrait,
    values: &[Vec<u8>],
    tree: &[u8],
) -> anyhow::Result<HashMap<Vec<u8>, indexed_trees::Model>> {
    let mut ranges_by_address = HashMap::with_capacity(values.len());
    let tree_literal = format_bytes(tree.to_vec(), DatabaseBackend::Sqlite);
    for value in values {
        let value_literal = format_bytes(value.clone(), DatabaseBackend::Sqlite);
        let statement = format!(
            "SELECT CAST({value_literal} AS BLOB) as input_address, tree, leaf_index, value, next_index, next_value, seq FROM indexed_trees WHERE tree = {tree_literal} AND value < {value_literal} ORDER BY value DESC LIMIT 1"
        );
        for row in conn
            .query_all(Statement::from_string(DatabaseBackend::Sqlite, statement))
            .await?
        {
            ranges_by_address.insert(
                row.try_get("", "input_address")?,
                indexed_trees::Model {
                    tree: row.try_get("", "tree")?,
                    leaf_index: row.try_get("", "leaf_index")?,
                    value: row.try_get("", "value")?,
                    next_index: row.try_get("", "next_index")?,
                    next_value: row.try_get("", "next_value")?,
                    seq: row.try_get("", "seq")?,
                },
            );
        }
    }
    Ok(ranges_by_address)
}

#[tokio::test]
async fn sqlite_batched_predecessors_preserve_strict_bounds_scope_duplicates_and_missing(
) -> anyhow::Result<()> {
    let (mut conn, models, _) = populated_fixture(3, 5, TreeType::AddressV2).await?;
    insert_range(
        &conn,
        &indexed_trees::Model {
            tree: vec![8; 32],
            leaf_index: 0,
            value: address(149999),
            next_index: 1,
            next_value: address(200000),
            seq: Some(99),
        },
    )
    .await?;
    let count = Arc::new(AtomicUsize::new(0));
    let callback_count = count.clone();
    conn.set_metric_callback(move |query_info| {
        if query_info.statement.sql.starts_with("WITH requested")
            || query_info.statement.sql.starts_with("SELECT CAST(")
        {
            callback_count.fetch_add(1, Ordering::SeqCst);
        }
    });
    let tree = ADDRESS_TREE_V1.to_vec();
    let values = [0, 1, 100000, 100001, 150000, 250000, 399999, 100001]
        .into_iter()
        .map(address)
        .collect::<Vec<_>>();
    let expected = old_lookup(&conn, &values, &tree).await?;
    count.store(0, Ordering::SeqCst);
    let actual =
        query_next_smallest_elements_by_address(&conn, values.clone(), tree.clone()).await?;
    assert_eq!(
        (actual, count.load(Ordering::SeqCst)),
        (expected, 1),
        "Strict predecessor semantics must match one-at-a-time queries"
    );
    count.store(0, Ordering::SeqCst);
    let many = (1..=1001).map(address).collect::<Vec<_>>();
    let expected = many
        .iter()
        .map(|value| (value.clone(), models.first().unwrap().clone()))
        .collect::<HashMap<_, _>>();
    let actual = query_next_smallest_elements_by_address(&conn, many, tree.clone()).await?;
    assert_eq!(
        (actual, count.load(Ordering::SeqCst)),
        (expected, 2),
        "Large batches must keep input-to-range associations across chunks"
    );
    let expected = old_lookup(&conn, &values, &tree)
        .await?
        .into_values()
        .map(|model| (model.value.clone(), model))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        query_next_smallest_elements(&conn, values, tree.clone()).await?,
        expected,
        "The ingester range-set helper must retain deduplication by low value"
    );
    count.store(0, Ordering::SeqCst);
    assert_eq!(
        (
            query_next_smallest_elements_by_address(&conn, vec![], tree).await?,
            count.load(Ordering::SeqCst)
        ),
        (HashMap::new(), 0),
        "Empty batches must perform no query"
    );
    Ok(())
}

#[tokio::test]
async fn batched_new_address_proofs_keep_roots_order_duplicates_and_existing_address_rejection(
) -> anyhow::Result<()> {
    for tree_type in [TreeType::AddressV1, TreeType::AddressV2] {
        let (conn, models, root) = populated_fixture(3, 5, tree_type).await?;
        let tree = ADDRESS_TREE_V1.to_vec();
        let values = [300001, 1, 100001, 100001, 200001]
            .into_iter()
            .map(address)
            .collect::<Vec<_>>();
        let txn = conn.begin().await?;
        let proofs = get_multiple_exclusion_ranges_with_proofs_v2(
            &txn,
            tree.clone(),
            5,
            values.clone(),
            tree_type,
        )
        .await?;
        for (value, (model, proof)) in &proofs {
            let expected = models
                .iter()
                .filter(|model| model.value < *value)
                .max_by(|left, right| left.value.cmp(&right.value))
                .unwrap();
            assert_eq!(
                (model, proof.root.clone(), proof.root_seq, proof.leaf_index),
                (expected, root.clone(), 42, model.leaf_index as u32),
                "Every range must use the current fixture root and matching leaf"
            );
            proof.validate()?;
        }
        txn.commit().await?;
        let pubkeys = values
            .iter()
            .cloned()
            .map(SerializablePubkey::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let v1 = get_multiple_new_address_proofs(&conn, AddressList(pubkeys.clone())).await?;
        let v2 = get_multiple_new_address_proofs_v2(
            &conn,
            AddressListWithTrees(
                pubkeys
                    .iter()
                    .map(|address| AddressWithTree {
                        address: *address,
                        tree: ADDRESS_TREE_V1.into(),
                    })
                    .collect(),
            ),
        )
        .await?;
        assert_eq!(
            (
                v1.value
                    .iter()
                    .map(|proof| proof.address)
                    .collect::<Vec<_>>(),
                v1.value.clone()
            ),
            (pubkeys.clone(), v2.value),
            "Public endpoints must preserve original input order and duplicates"
        );
        let existing = SerializablePubkey::try_from(address(100000))?;
        let error = get_multiple_new_address_proofs(&conn, AddressList(vec![existing]))
            .await
            .unwrap_err();
        assert_eq!(
            error,
            PhotonApiError::InvalidParams(format!("Address {} already exists", existing)),
            "The predecessor comparison must remain strict so existing addresses are rejected"
        );
    }
    Ok(())
}

async fn old_proofs(
    txn: &DatabaseTransaction,
    values: &[Vec<u8>],
    height: u32,
) -> anyhow::Result<HashMap<Vec<u8>, (indexed_trees::Model, MerkleProofWithContext)>> {
    let tree = ADDRESS_TREE_V1.to_vec();
    let ranges = old_lookup(txn, values, &tree).await?;
    let mut leaves = Vec::new();
    for value in values {
        let model = ranges
            .get(value)
            .ok_or_else(|| anyhow::anyhow!("Benchmark address has no predecessor"))?;
        let leaf = LeafNode {
            tree: SerializablePubkey::try_from(tree.clone())?,
            leaf_index: model.leaf_index as u32,
            hash: compute_hash_by_tree_type(model, TreeType::AddressV2)?,
            seq: model.seq.map(|seq| seq as u32),
        };
        let node = leaf.node_index(height);
        leaves.push((leaf, node));
    }
    let proofs =
        get_multiple_compressed_leaf_proofs_from_full_leaf_info(txn, leaves, height).await?;
    let mut proofs_by_address = HashMap::new();
    for (value, model) in ranges {
        let proof = proofs
            .iter()
            .find(|proof| proof.leaf_index == model.leaf_index as u32)
            .ok_or_else(|| anyhow::anyhow!("Benchmark proof missing"))?;
        proofs_by_address.insert(value, (model, proof.clone()));
    }
    Ok(proofs_by_address)
}

fn canonical_proofs(
    proofs: &HashMap<Vec<u8>, (indexed_trees::Model, MerkleProofWithContext)>,
) -> BTreeMap<
    Vec<u8>,
    (
        indexed_trees::Model,
        Vec<Hash>,
        Hash,
        u32,
        Hash,
        SerializablePubkey,
        u64,
    ),
> {
    proofs
        .iter()
        .map(|(value, (model, proof))| {
            (
                value.clone(),
                (
                    model.clone(),
                    proof.proof.clone(),
                    proof.root.clone(),
                    proof.leaf_index,
                    proof.hash.clone(),
                    proof.merkle_tree,
                    proof.root_seq,
                ),
            )
        })
        .collect()
}

#[tokio::test]
#[ignore = "Manual exclusive timing window; baseline and candidate run alternately"]
async fn benchmark_sqlite_address_batches() -> anyhow::Result<()> {
    use std::time::Instant;
    let (conn, _, root) = populated_fixture(1000, 12, TreeType::AddressV2).await?;
    let version = conn
        .query_one(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT sqlite_version() AS version".to_string(),
        ))
        .await?
        .unwrap()
        .try_get::<String>("", "version")?;
    println!("SQLITE_ADDRESS_VERSION {version}");
    let tree = ADDRESS_TREE_V1.to_vec();
    for shape in ["shared", "dense"] {
        for count in [1_u64, 100, 1000] {
            let values = (0..count)
                .map(|ordinal| {
                    address(if shape == "shared" {
                        ordinal + 1
                    } else {
                        ordinal * 100000 + 1
                    })
                })
                .collect::<Vec<_>>();
            let old = old_lookup(&conn, &values, &tree).await?;
            let new = query_next_smallest_elements_by_address(&conn, values.clone(), tree.clone())
                .await?;
            assert_eq!(
                new, old,
                "Lookup benchmark must do equivalent successful work"
            );
            let mut old_times = Vec::new();
            let mut new_times = Vec::new();
            for repetition in 0..4 {
                for candidate in if repetition % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    let inputs = values.clone();
                    let start = Instant::now();
                    let predecessor_rows = if candidate {
                        query_next_smallest_elements_by_address(&conn, inputs, tree.clone()).await?
                    } else {
                        old_lookup(&conn, &inputs, &tree).await?
                    };
                    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                    assert_eq!(predecessor_rows.len(), count as usize);
                    std::hint::black_box(predecessor_rows);
                    if repetition > 0 {
                        if candidate {
                            new_times.push(elapsed);
                        } else {
                            old_times.push(elapsed);
                        }
                    }
                }
            }
            println!(
                "SQLITE_ADDRESS_BENCH {}",
                serde_json::json!({"kind":"lookup","shape":shape,"n":count,"old_ms":old_times,"new_ms":new_times,"old_queries":count,"new_queries":1})
            );

            let txn = conn.begin().await?;
            let old = old_proofs(&txn, &values, 12).await?;
            let new = get_multiple_exclusion_ranges_with_proofs_v2(
                &txn,
                tree.clone(),
                12,
                values.clone(),
                TreeType::AddressV2,
            )
            .await?;
            assert_eq!(
                canonical_proofs(&new),
                canonical_proofs(&old),
                "Complete benchmark Merkle outputs must match"
            );
            let mut validated = std::collections::HashSet::new();
            for (_, proof) in new.values() {
                assert_eq!(proof.root, root);
                if validated.insert(proof.leaf_index) {
                    proof.validate()?;
                }
            }
            txn.commit().await?;
            let mut old_times = Vec::new();
            let mut new_times = Vec::new();
            for repetition in 0..4 {
                for candidate in if repetition % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    let txn = conn.begin().await?;
                    let inputs = values.clone();
                    let start = Instant::now();
                    let address_proofs = if candidate {
                        get_multiple_exclusion_ranges_with_proofs_v2(
                            &txn,
                            tree.clone(),
                            12,
                            inputs,
                            TreeType::AddressV2,
                        )
                        .await?
                    } else {
                        old_proofs(&txn, &inputs, 12).await?
                    };
                    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                    assert_eq!(address_proofs.len(), count as usize);
                    std::hint::black_box(address_proofs);
                    txn.commit().await?;
                    if repetition > 0 {
                        if candidate {
                            new_times.push(elapsed);
                        } else {
                            old_times.push(elapsed);
                        }
                    }
                }
            }
            println!(
                "SQLITE_ADDRESS_BENCH {}",
                serde_json::json!({"kind":"full_proof_helper","shape":shape,"n":count,"old_ms":old_times,"new_ms":new_times})
            );
        }
    }
    Ok(())
}
