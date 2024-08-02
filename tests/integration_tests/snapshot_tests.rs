use futures::stream;


use photon_indexer::common::typedefs::{hash::Hash};

use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};



use std::vec;

#[tokio::test]
async fn test_basic_snapshotting() {
    use std::{env::temp_dir, fs};

    use futures::StreamExt;
    use photon_indexer::ingester::snapshotter::{
        load_block_stream_from_snapshot_directory, update_snapshot_helper,
    };

    let blocks: Vec<BlockInfo> = (0..10)
        .map(|i| {
            let block = BlockInfo {
                metadata: BlockMetadata {
                    slot: i,
                    parent_slot: if i == 0 { 0 } else { i - 1 },
                    block_time: 0,
                    blockhash: Hash::default(),
                    parent_blockhash: Hash::default(),
                    block_height: i,
                },
                transactions: vec![],
            };
            block
        })
        .collect();
    let blocks_stream = stream::iter(blocks.clone().into_iter());
    let temp_dir = temp_dir();
    let snapshot_dir = temp_dir.as_path().join("test-snapshots");
    if snapshot_dir.exists() {
        fs::remove_dir_all(&snapshot_dir).unwrap();
    } else {
        fs::create_dir(&snapshot_dir).unwrap();
    }
    update_snapshot_helper(blocks_stream, 0, 2, 4, &snapshot_dir).await;
    let snapshot_blocks = load_block_stream_from_snapshot_directory(&snapshot_dir);
    let snapshot_blocks: Vec<BlockInfo> = snapshot_blocks.collect().await;
    assert_eq!(snapshot_blocks, blocks);
}
