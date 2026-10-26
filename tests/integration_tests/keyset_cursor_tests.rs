use photon_indexer::{
    api::method::{
        get_compressed_mint_token_holders::{
            get_compressed_mint_token_holders, GetCompressedMintTokenHoldersRequest,
        },
        utils::{
            fetch_token_accounts, fetch_token_accounts_v2, search_for_signatures, Authority,
            GetCompressedTokenAccountsByAuthorityOptions, SignatureFilter, SignatureInfoWithError,
            SignatureSearchType,
        },
    },
    common::typedefs::{
        bs58_string::Base58String, hash::Hash, limit::Limit,
        serializable_signature::SerializableSignature, unix_timestamp::UnixTimestamp,
        unsigned_integer::UnsignedInteger,
    },
};
use rstest::rstest;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, Statement,
};
use solana_signature::Signature;

async fn fixture(backend: DatabaseBackend) -> anyhow::Result<DatabaseConnection> {
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
    let decimal = if backend == DatabaseBackend::Postgres {
        "NUMERIC(23,0)"
    } else {
        "REAL"
    };
    for sql in [
        "CREATE TEMP TABLE blocks (slot BIGINT PRIMARY KEY, block_time BIGINT NOT NULL)".to_string(),
        format!("CREATE TEMP TABLE accounts (
            hash {binary} PRIMARY KEY, data {binary}, data_hash {binary}, address {binary},
            owner {binary} NOT NULL, tree {binary} NOT NULL, leaf_index BIGINT NOT NULL,
            seq BIGINT, slot_created BIGINT NOT NULL, spent BOOLEAN NOT NULL, prev_spent BOOLEAN,
            lamports {decimal} NOT NULL, tree_type INTEGER, nullified_in_tree BOOLEAN NOT NULL DEFAULT false,
            nullifier_queue_index BIGINT, in_output_queue BOOLEAN NOT NULL DEFAULT false,
            queue {binary}, nullifier {binary}, tx_hash {binary}, onchain_pubkey {binary},
            discriminator_v2 {binary}, discriminator {decimal}
        )"),
        format!("CREATE TEMP TABLE token_accounts (
            hash {binary} PRIMARY KEY, owner {binary} NOT NULL, mint {binary} NOT NULL,
            delegate {binary}, state INTEGER NOT NULL, spent BOOLEAN NOT NULL,
            prev_spent BOOLEAN, amount {decimal} NOT NULL, tlv {binary}, ata_owner {binary}
        )"),
        format!("CREATE TEMP TABLE token_owner_balances (
            owner {binary} NOT NULL, mint {binary} NOT NULL, amount {decimal} NOT NULL,
            PRIMARY KEY(owner,mint)
        )"),
        format!("CREATE TEMP TABLE transactions (
            signature {binary} PRIMARY KEY, slot BIGINT NOT NULL,
            uses_compression BOOLEAN NOT NULL, error TEXT
        )"),
        format!("CREATE TEMP TABLE account_transactions (
            hash {binary} NOT NULL, signature {binary} NOT NULL, PRIMARY KEY(hash,signature)
        )"),
        "CREATE INDEX keyset_token_owner_idx ON token_accounts (spent,owner,mint,hash)".to_string(),
        "CREATE INDEX keyset_token_delegate_idx ON token_accounts (spent,delegate,mint,hash)".to_string(),
        "CREATE INDEX keyset_holders_idx ON token_owner_balances (mint,amount,owner)".to_string(),
    ] {
        conn.execute(Statement::from_string(backend, sql)).await?;
    }
    conn.execute(Statement::from_string(
        backend,
        "INSERT INTO blocks VALUES (9,99),(10,100),(11,101)".to_string(),
    ))
    .await?;
    Ok(conn)
}

async fn add_account(
    conn: &DatabaseConnection,
    hash: u8,
    owner: u8,
    spent: bool,
) -> anyhow::Result<()> {
    conn.execute(Statement::from_sql_and_values(
        conn.get_database_backend(),
        "INSERT INTO accounts (hash,address,owner,tree,leaf_index,seq,slot_created,spent,lamports) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        [vec![hash;32].into(),vec![9;32].into(),vec![owner;32].into(),vec![8;32].into(),(hash as i64).into(),1_i64.into(),10_i64.into(),spent.into(),1_i64.into()],
    )).await?;
    Ok(())
}

async fn token_page(
    conn: &DatabaseConnection,
    v2: bool,
    delegate: bool,
    mint: Option<u8>,
    cursor: Option<Base58String>,
) -> anyhow::Result<(Vec<Hash>, Option<Base58String>)> {
    let authority = if delegate {
        Authority::Delegate([3; 32].into())
    } else {
        Authority::Owner([1; 32].into())
    };
    let options = GetCompressedTokenAccountsByAuthorityOptions {
        mint: mint.map(|value| [value; 32].into()),
        cursor,
        limit: Some(Limit::new(1).unwrap()),
    };
    if v2 {
        let response = fetch_token_accounts_v2(conn, authority, options).await?;
        Ok((
            response
                .value
                .items
                .into_iter()
                .map(|item| item.account.hash)
                .collect(),
            response.value.cursor,
        ))
    } else {
        let response = fetch_token_accounts(conn, authority, options).await?;
        Ok((
            response
                .value
                .items
                .into_iter()
                .map(|item| item.account.hash)
                .collect(),
            response.value.cursor,
        ))
    }
}

#[rstest]
#[tokio::test]
async fn token_keysets_preserve_ties_wire_cursors_and_mint_specific_behavior(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
) -> anyhow::Result<()> {
    let conn = fixture(backend).await?;
    let records: [(u8, u8, u8, u8, bool); 8] = [
        (1, 1, 3, 5, false),
        (2, 1, 3, 5, false),
        (3, 1, 3, 6, false),
        (4, 1, 3, 6, true),
        (5, 2, 3, 5, false),
        (6, 1, 4, 5, false),
        (7, 2, 4, 6, false),
        (8, 1, 3, 7, false),
    ];
    for (hash, owner, delegate, mint, spent) in records {
        add_account(&conn, hash, 9, spent).await?;
        conn.execute(Statement::from_sql_and_values(backend,
            "INSERT INTO token_accounts (hash,owner,mint,delegate,state,spent,amount) VALUES ($1,$2,$3,$4,$5,$6,$7)",
            [vec![hash;32].into(),vec![owner;32].into(),vec![mint;32].into(),vec![delegate;32].into(),1_i32.into(),spent.into(),1_i64.into()],
        )).await?;
    }
    for v2 in [false, true] {
        for delegate in [false, true] {
            let hashes = if delegate {
                vec![1, 2, 5, 3, 8]
            } else {
                vec![1, 2, 6, 3, 8]
            };
            let mut cursor = None;
            let mut items = Vec::new();
            let mut cursors = Vec::new();
            for _ in 0..=hashes.len() {
                let (page, next) = token_page(&conn, v2, delegate, None, cursor).await?;
                items.extend(page);
                if let Some(next) = &next {
                    cursors.push(next.0.clone());
                }
                cursor = next;
                if cursor.is_none() {
                    break;
                }
            }
            let expected_cursors = hashes
                .iter()
                .map(|hash| {
                    let mint = records.iter().find(|record| record.0 == *hash).unwrap().3;
                    [vec![mint; 32], vec![*hash; 32]].concat()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                (items, cursors, cursor),
                (
                    hashes
                        .iter()
                        .map(|hash| Hash::from([*hash; 32]))
                        .collect::<Vec<_>>(),
                    expected_cursors,
                    None
                ),
                "Token keysets must preserve complete ordering and existing 64-byte cursors"
            );

            // The mint portion is ignored when a mint is supplied, even for a mismatched cursor mint.
            let mismatched = Base58String([vec![255; 32], vec![1; 32]].concat());
            let (items, cursor) =
                token_page(&conn, v2, delegate, Some(5), Some(mismatched)).await?;
            assert_eq!(
                (items, cursor),
                (
                    vec![Hash::from([2; 32])],
                    Some(Base58String([vec![5; 32], vec![2; 32]].concat()))
                ),
                "Mint-filtered pages must continue to compare only the account hash"
            );
        }
    }
    Ok(())
}

#[rstest]
#[tokio::test]
async fn holder_keysets_preserve_descending_ties_and_wire_cursors(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
) -> anyhow::Result<()> {
    let conn = fixture(backend).await?;
    for (owner, amount, mint) in [
        (1_u8, 80_u64, 5_u8),
        (2, 90, 5),
        (3, 100, 5),
        (4, 100, 5),
        (5, 1000, 6),
    ] {
        conn.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO token_owner_balances (owner,mint,amount) VALUES ($1,$2,$3)",
            [
                vec![owner; 32].into(),
                vec![mint; 32].into(),
                (amount as i64).into(),
            ],
        ))
        .await?;
    }
    let expected = vec![
        (vec![4; 32], 100_u64),
        (vec![3; 32], 100),
        (vec![2; 32], 90),
        (vec![1; 32], 80),
    ];
    let mut cursor = None;
    let mut items = Vec::new();
    let mut cursors = Vec::new();
    for _ in 0..=expected.len() {
        let response = get_compressed_mint_token_holders(
            &conn,
            GetCompressedMintTokenHoldersRequest {
                mint: [5; 32].into(),
                cursor,
                limit: Some(Limit::new(1).unwrap()),
            },
        )
        .await?;
        items.extend(
            response
                .value
                .items
                .into_iter()
                .map(|item| (item.owner.to_bytes_vec(), item.balance.0)),
        );
        cursor = response.value.cursor;
        if let Some(cursor) = &cursor {
            cursors.push(cursor.0.clone());
        }
        if cursor.is_none() {
            break;
        }
    }
    let expected_cursors = expected
        .iter()
        .map(|(owner, amount)| [amount.to_le_bytes().to_vec(), owner.clone()].concat())
        .collect::<Vec<_>>();
    assert_eq!(
        (items, cursors, cursor),
        (expected, expected_cursors, None),
        "Holder pages must keep descending amount/owner ordering and existing 40-byte cursors"
    );
    Ok(())
}

fn signature_info(signature: u8, slot: u64) -> SignatureInfoWithError {
    SignatureInfoWithError {
        signature: SerializableSignature(Signature::from([signature; 64])),
        slot: UnsignedInteger(slot),
        block_time: UnixTimestamp(90 + slot),
        error: Some(format!("error-{signature}")),
    }
}

fn signature_cursor(item: &SignatureInfoWithError) -> String {
    bs58::encode(
        [
            item.slot.0.to_le_bytes().to_vec(),
            item.signature.0.as_ref().to_vec(),
        ]
        .concat(),
    )
    .into_string()
}

#[rstest]
#[tokio::test]
async fn signature_keysets_preserve_filter_bindings_same_slot_ties_and_wire_cursors(
    #[values(DatabaseBackend::Sqlite, DatabaseBackend::Postgres)] backend: DatabaseBackend,
) -> anyhow::Result<()> {
    let conn = fixture(backend).await?;
    for hash in [1, 2] {
        add_account(&conn, hash, 9, hash == 2).await?;
        conn.execute(Statement::from_sql_and_values(backend,
            "INSERT INTO token_accounts (hash,owner,mint,state,spent,amount) VALUES ($1,$2,$3,$4,$5,$6)",
            [vec![hash;32].into(),vec![3;32].into(),vec![5;32].into(),1_i32.into(),false.into(),1_i64.into()],
        )).await?;
    }
    for (signature, slot, compression) in [
        (10, 11, false),
        (9, 10, true),
        (8, 10, true),
        (7, 9, true),
        (6, 9, true),
    ] {
        conn.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO transactions VALUES ($1,$2,$3,$4)",
            [
                vec![signature; 64].into(),
                (slot as i64).into(),
                compression.into(),
                format!("error-{signature}").into(),
            ],
        ))
        .await?;
    }
    for (hash, signature) in [(1, 9), (1, 8), (1, 7), (2, 8), (2, 6)] {
        conn.execute(Statement::from_sql_and_values(
            backend,
            "INSERT INTO account_transactions VALUES ($1,$2)",
            [vec![hash; 32].into(), vec![signature; 64].into()],
        ))
        .await?;
    }
    let shared = vec![
        signature_info(9, 10),
        signature_info(8, 10),
        signature_info(7, 9),
        signature_info(6, 9),
    ];
    for (search_type, filter, compressed, expected) in [
        (
            SignatureSearchType::Standard,
            Some(SignatureFilter::Account([1; 32].into())),
            true,
            shared.iter().take(3).cloned().collect::<Vec<_>>(),
        ),
        (
            SignatureSearchType::Standard,
            Some(SignatureFilter::Address([9; 32].into())),
            true,
            shared.clone(),
        ),
        (
            SignatureSearchType::Standard,
            Some(SignatureFilter::Owner([9; 32].into())),
            true,
            shared.clone(),
        ),
        (
            SignatureSearchType::Token,
            Some(SignatureFilter::Owner([3; 32].into())),
            true,
            shared.clone(),
        ),
        (SignatureSearchType::Standard, None, true, shared.clone()),
        (
            SignatureSearchType::Standard,
            None,
            false,
            [vec![signature_info(10, 11)], shared.clone()].concat(),
        ),
    ] {
        let mut cursor = None;
        let mut items = Vec::new();
        let mut cursors = Vec::new();
        for _ in 0..=expected.len() {
            let next_filter = match &filter {
                Some(SignatureFilter::Account(hash)) => {
                    Some(SignatureFilter::Account(hash.clone()))
                }
                Some(SignatureFilter::Address(address)) => Some(SignatureFilter::Address(*address)),
                Some(SignatureFilter::Owner(owner)) => Some(SignatureFilter::Owner(*owner)),
                None => None,
            };
            let response = search_for_signatures(
                &conn,
                search_type.clone(),
                next_filter,
                compressed,
                cursor,
                Some(Limit::new(2).unwrap()),
            )
            .await?;
            items.extend(response.items);
            cursor = response.cursor;
            if let Some(cursor) = &cursor {
                cursors.push(cursor.clone());
            }
            if cursor.is_none() {
                break;
            }
        }
        let expected_cursors = expected
            .chunks(2)
            .filter(|chunk| chunk.len() == 2)
            .map(|chunk| signature_cursor(chunk.last().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!((items,cursors,cursor),(expected,expected_cursors,None),
            "Signature pages must preserve deduplication, strict tie bounds and existing 72-byte cursor encoding");
    }
    Ok(())
}
