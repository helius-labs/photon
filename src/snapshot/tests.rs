use super::*;
use crate::{common::typedefs::hash::Hash, ingester::typedefs::block_info::BlockMetadata};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct SnapshotDirectory(PathBuf);

impl SnapshotDirectory {
    fn new() -> Self {
        let path = temp_dir().join(format!("photon-snapshot-test-{}", rand::random::<u64>()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn adapter(&self) -> Arc<DirectoryAdapter> {
        Arc::new(DirectoryAdapter::from_local_directory(
            self.0.to_str().unwrap().to_owned(),
        ))
    }

    fn write(&self, start: u64, end: u64) {
        let mut contents = Vec::new();
        for slot in start..=end {
            let block = BlockInfo {
                metadata: BlockMetadata {
                    slot,
                    parent_slot: slot.saturating_sub(1),
                    block_time: 0,
                    blockhash: Hash::default(),
                    parent_blockhash: Hash::default(),
                    block_height: slot,
                },
                transactions: vec![],
            };
            contents.extend(bincode::serialize(&block).unwrap());
        }
        fs::write(self.0.join(format!("snapshot-{start}-{end}")), contents).unwrap();
    }
}

impl Drop for SnapshotDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

async fn loaded_slots(adapter: Arc<DirectoryAdapter>) -> Vec<u64> {
    load_block_stream_from_directory_adapter(adapter)
        .await
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .map(|block| block.metadata.slot)
        .collect()
}

#[tokio::test]
async fn completed_merge_is_not_read_twice_or_deleted_on_restart() {
    let directory = SnapshotDirectory::new();
    directory.write(0, 1);
    directory.write(2, 3);
    directory.write(0, 3);
    let adapter = directory.adapter();
    let files = get_snapshot_files_with_metadata(&adapter).await.unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].file, "snapshot-0-1");
    assert_eq!(files[1].file, "snapshot-2-3");
    let before = fs::read(directory.0.join("snapshot-0-3")).unwrap();

    try_merge_snapshots(adapter.clone()).await.unwrap();

    assert_eq!(fs::read(directory.0.join("snapshot-0-3")).unwrap(), before);
    assert_eq!(loaded_slots(adapter).await, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn one_snapshot_is_preserved_during_merge() {
    let directory = SnapshotDirectory::new();
    directory.write(0, 2);
    let adapter = directory.adapter();
    try_merge_snapshots(adapter.clone()).await.unwrap();
    assert_eq!(loaded_slots(adapter).await, vec![0, 1, 2]);
}

#[tokio::test]
async fn merged_snapshot_is_used_when_original_inputs_are_incomplete() {
    let directory = SnapshotDirectory::new();
    directory.write(0, 1);
    directory.write(3, 4);
    directory.write(0, 4);
    let adapter = directory.adapter();
    let files = get_snapshot_files_with_metadata(&adapter).await.unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].file, "snapshot-0-4");
    assert_eq!(loaded_slots(adapter).await, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn contiguous_snapshots_merge_and_remain_readable() {
    let directory = SnapshotDirectory::new();
    directory.write(0, 1);
    directory.write(2, 3);
    let adapter = directory.adapter();

    try_merge_snapshots(adapter.clone()).await.unwrap();

    assert!(!directory.0.join("snapshot-0-1").exists());
    assert!(!directory.0.join("snapshot-2-3").exists());
    assert!(directory.0.join("snapshot-0-3").exists());
    assert_eq!(loaded_slots(adapter).await, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn gaps_and_partial_overlaps_are_rejected_without_deleting_inputs() {
    for ranges in [[(0, 1), (3, 4)], [(0, 2), (2, 4)]] {
        let directory = SnapshotDirectory::new();
        for (start, end) in ranges {
            directory.write(start, end);
        }
        let error = try_merge_snapshots(directory.adapter()).await.unwrap_err();
        assert!(format!("{error:#}").contains("gap or partial overlap"));
        for (start, end) in ranges {
            assert!(directory.0.join(format!("snapshot-{start}-{end}")).exists());
        }
    }
}

#[tokio::test]
async fn denied_r2_listing_is_an_error_instead_of_empty_history() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        let mut request = [0; 8192];
        connection.read(&mut request).await.unwrap();
        let body = "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>";
        let response = format!(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        connection.write_all(response.as_bytes()).await.unwrap();
    });
    let bucket = Bucket::new(
        "test-snapshots",
        Region::Custom {
            region: "us-east-1".to_owned(),
            endpoint: format!("http://{address}"),
        },
        Credentials::new(Some("test-access"), Some("test-secret"), None, None, None).unwrap(),
    )
    .unwrap()
    .with_path_style();
    let adapter = DirectoryAdapter::new(
        None,
        Some(R2DirectoryAdapter {
            r2_bucket: bucket,
            r2_prefix: String::new(),
        }),
        None,
    );

    let error = get_snapshot_files_with_metadata(&adapter)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("Failed to list R2 snapshot files"));
    server.await.unwrap();
}
