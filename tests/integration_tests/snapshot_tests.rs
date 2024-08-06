use futures::stream;

use photon_indexer::common::typedefs::hash::Hash;

use photon_indexer::ingester::typedefs::block_info::{BlockInfo, BlockMetadata};
use photon_indexer::snapshot::{
    create_snapshot_from_byte_stream, load_block_stream_from_snapshot_directory,
    load_byte_stream_from_snapshot_directory, update_snapshot_helper,
};

use std::vec;

#[tokio::test]
async fn test_basic_snapshotting() {
    use std::{env::temp_dir, fs};

    use futures::StreamExt;

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

    let byte_stream = load_byte_stream_from_snapshot_directory(&snapshot_dir);
    let snapshot_dir_v2 = temp_dir.as_path().join("test-snapshots-v2");
    if snapshot_dir_v2.exists() {
        fs::remove_dir_all(&snapshot_dir_v2).unwrap();
    } else {
        fs::create_dir(&snapshot_dir_v2).unwrap();
    }
    create_snapshot_from_byte_stream(byte_stream, &snapshot_dir_v2)
        .await
        .unwrap();
    let snapshot_blocks_v2 = load_block_stream_from_snapshot_directory(&snapshot_dir_v2);
    let snapshot_blocks_v2: Vec<BlockInfo> = snapshot_blocks_v2.collect().await;
    assert_eq!(snapshot_blocks_v2, blocks);
}
