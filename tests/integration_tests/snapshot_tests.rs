use futures::stream;

use photon_indexer::common::typedefs::hash::Hash;

use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use photon_indexer::snapshot::{
    create_snapshot_from_byte_stream, get_r2_bucket, get_snapshot_files_with_metadata,
    load_block_stream_from_directory_adapter, load_byte_stream_from_directory_adapter,
    update_snapshot_helper, R2BucketArgs, R2DirectoryAdapter,
};
use s3::creds::Credentials;
use s3::Region;

use std::sync::Arc;
use std::vec;

#[tokio::test]
#[ignore]
async fn test_basic_snapshotting() {
    use futures::StreamExt;
    use std::env::temp_dir;

    let temp_dir = temp_dir();
    let snapshot_dirs = ["snapshots1", "snapshots2"];
    let file_system_directory_adapters = snapshot_dirs
        .iter()
        .map(|snapshot_dir| {
            let snapshot_dir = temp_dir.join(snapshot_dir);
            let filesystem_adapter = photon_indexer::snapshot::FileSystemDirectoryApapter {
                snapshot_dir: snapshot_dir.clone().to_str().unwrap().to_string(),
            };

            Arc::new(photon_indexer::snapshot::DirectoryAdapter::new(
                Some(filesystem_adapter),
                None,
                None,
            ))
        })
        .collect::<Vec<Arc<photon_indexer::snapshot::DirectoryAdapter>>>();

    let mut r2_directory_adapters = vec![];

    for snapshot_dir in snapshot_dirs.iter() {
        let r2_credentials =
            Credentials::new(Some("minioadmin"), Some("minioadmin"), None, None, None).unwrap();
        let r2_region = Region::Custom {
            region: "us-east-1".to_string(),
            endpoint: "http://localhost:9000".to_string(),
        };
        let r2_bucket = get_r2_bucket(R2BucketArgs {
            r2_credentials,
            r2_region,
            r2_bucket: snapshot_dir.to_string(),
            create_bucket: true,
        })
        .await;

        let directory_adapter = Arc::new(photon_indexer::snapshot::DirectoryAdapter::new(
            None,
            Some(R2DirectoryAdapter {
                r2_bucket,
                r2_prefix: "some_prefix".to_string(),
            }),
            None,
        ));
        r2_directory_adapters.push(directory_adapter);
    }

    let directory_adapters = vec![r2_directory_adapters, file_system_directory_adapters];
    for adapter_pair in directory_adapters {
        let directory_adapter = adapter_pair[0].clone();
        let directory_adapter_v2 = adapter_pair[1].clone();

        let blocks: Vec<BlockInfo> = (0..30)
            .map(|i| BlockInfo {
                metadata: BlockMetadata {
                    slot: i,
                    parent_slot: if i == 0 { 0 } else { i - 1 },
                    block_time: 0,
                    blockhash: Hash::default(),
                    parent_blockhash: Hash::default(),
                    block_height: i,
                },
                transactions: vec![],
            })
            .collect();
        let blocks_stream = stream::iter(vec![blocks.clone()]);

        let snapshot_files = get_snapshot_files_with_metadata(directory_adapter.as_ref())
            .await
            .unwrap();
        for file in snapshot_files {
            directory_adapter.delete_file(file.file).await.unwrap();
        }

        update_snapshot_helper(directory_adapter.clone(), blocks_stream, 0, 2, 4).await;
        let snapshot_blocks =
            load_block_stream_from_directory_adapter(directory_adapter.clone()).await;
        let snapshot_blocks: Vec<Vec<BlockInfo>> = snapshot_blocks.collect().await;
        let snapshot_blocks: Vec<BlockInfo> = snapshot_blocks.into_iter().flatten().collect();
        assert_eq!(snapshot_blocks, blocks);

        let byte_stream = load_byte_stream_from_directory_adapter(directory_adapter.clone()).await;
        let snapshot_files = get_snapshot_files_with_metadata(directory_adapter_v2.as_ref())
            .await
            .unwrap();
        for file in snapshot_files {
            directory_adapter_v2.delete_file(file.file).await.unwrap();
        }
        create_snapshot_from_byte_stream(byte_stream, directory_adapter_v2.as_ref())
            .await
            .unwrap();
        let snapshot_blocks_v2 =
            load_block_stream_from_directory_adapter(directory_adapter_v2.clone()).await;
        let snapshot_blocks_v2: Vec<Vec<BlockInfo>> = snapshot_blocks_v2.collect().await;
        let snapshot_blocks_v2: Vec<BlockInfo> = snapshot_blocks_v2.into_iter().flatten().collect();
        assert_eq!(snapshot_blocks_v2, blocks);
    }
}
