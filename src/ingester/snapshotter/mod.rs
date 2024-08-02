use std::{
    env::temp_dir,
    fs::{self, File},
    io::{self, BufReader, Seek, Write},
    path::{Path, PathBuf},
};

use async_std::stream::StreamExt;
use async_stream::stream;
use futures::{pin_mut, Stream};

use super::{
    fetchers::BlockStreamConfig,
    parser::ACCOUNT_COMPRESSION_PROGRAM_ID,
    typedefs::block_info::{BlockInfo, Instruction, TransactionInfo},
};

const SNAPSHOT_VERSION: u64 = 1;

fn is_compression_instruction(instruction: &Instruction) -> bool {
    instruction.program_id == ACCOUNT_COMPRESSION_PROGRAM_ID
        || instruction
            .accounts
            .contains(&ACCOUNT_COMPRESSION_PROGRAM_ID)
}

fn is_compression_transaction(tx: &TransactionInfo) -> bool {
    for instruction_group in &tx.instruction_groups {
        if is_compression_instruction(&instruction_group.outer_instruction) {
            return true;
        }
        for instruction in &instruction_group.inner_instructions {
            if is_compression_instruction(instruction) {
                return true;
            }
        }
    }
    false
}

fn serialize_block_to_file(block: &BlockInfo, file: &mut File) {
    let trimmed_block = BlockInfo {
        metadata: block.metadata.clone(),
        transactions: block
            .transactions
            .iter()
            .filter(|tx| is_compression_transaction(tx))
            .cloned()
            .collect(),
    };
    let block_bytes = bincode::serialize(&trimmed_block).unwrap();
    file.write_all(block_bytes.as_ref()).unwrap();
}

pub struct SnapshotFileWithSlots {
    file: PathBuf,
    start_slot: u64,
    end_slot: u64,
}

pub fn get_snapshot_files_with_slots(snapshot_dir: &Path) -> Vec<SnapshotFileWithSlots> {
    let snapshot_files = fs::read_dir(snapshot_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    let mut snapshot_files_with_slots = Vec::new();

    for file in snapshot_files {
        let file_name = file.file_name().unwrap().to_str().unwrap();
        let parts: Vec<&str> = file_name.split('-').collect();
        if parts.len() == 3 {
            let start_slot = parts[1].parse::<u64>().unwrap();
            let end_slot = parts[2].parse::<u64>().unwrap();
            snapshot_files_with_slots.push(SnapshotFileWithSlots {
                file,
                start_slot,
                end_slot,
            });
        }
    }
    snapshot_files_with_slots.sort_by_key(|file| file.start_slot);
    snapshot_files_with_slots
}

fn create_temp_snapshot_file(dir: &str) -> (File, PathBuf) {
    let temp_dir = temp_dir();
    // Create a subdirectory for the snapshot files
    let temp_dir = temp_dir.join(dir);
    if !temp_dir.exists() {
        fs::create_dir(&temp_dir).unwrap();
    }
    let temp_file_path = temp_dir.join("temp-snapshot");
    if temp_file_path.exists() {
        fs::remove_file(&temp_file_path).unwrap();
    }
    let mut temp_file = File::create(&temp_file_path).unwrap();
    temp_file
        .write_all(&SNAPSHOT_VERSION.to_le_bytes())
        .unwrap();
    (temp_file, temp_file_path)
}

async fn merge_snapshots(snapshot_dir: &Path) {
    let (mut temp_file, temp_file_path) = create_temp_snapshot_file("fullsnaphot/");

    let block_stream = load_block_stream_from_snapshot_directory(snapshot_dir);
    pin_mut!(block_stream);
    let mut start_slot = None;
    let mut end_slot = None;

    while let Some(block) = block_stream.next().await {
        if start_slot.is_none() {
            start_slot = Some(block.metadata.slot);
        }
        end_slot = Some(block.metadata.slot);
        serialize_block_to_file(&block, &mut temp_file);
    }
    let temp_file_directory = temp_file_path.parent().unwrap();
    let snapshot_file_path = temp_file_directory.join(format!(
        "snapshot-{}-{}",
        start_slot.unwrap(),
        end_slot.unwrap()
    ));
    fs::rename(&temp_file_path, &snapshot_file_path).unwrap();
    let backup_dir = temp_dir().join("backup");
    fs::rename(snapshot_dir, &backup_dir).unwrap();
    fs::rename(temp_file_directory, snapshot_dir).unwrap();
    fs::remove_dir_all(backup_dir).unwrap();
}

pub async fn update_snapshot(
    block_stream_config: BlockStreamConfig,
    incremental_snapshot_interval_slots: u64,
    full_snapshot_interval_slots: u64,
    snapshot_dir: &Path,
) {
    // Convert stream to iterator
    let block_stream = block_stream_config.load_block_stream();
    update_snapshot_helper(
        block_stream,
        block_stream_config.last_indexed_slot,
        incremental_snapshot_interval_slots,
        full_snapshot_interval_slots,
        snapshot_dir,
    )
    .await;
}

pub async fn update_snapshot_helper(
    blocks: impl Stream<Item = BlockInfo>,
    last_indexed_slot: u64,
    incremental_snapshot_interval_slots: u64,
    full_snapshot_interval_slots: u64,
    snapshot_dir: &Path,
) {
    if !snapshot_dir.exists() {
        fs::create_dir(snapshot_dir).unwrap();
    }
    let snapshot_files = get_snapshot_files_with_slots(snapshot_dir);

    let mut last_full_snapshot_slot = snapshot_files
        .first()
        .map(|file| file.end_slot)
        .unwrap_or(last_indexed_slot);
    let mut last_snapshot_slot = snapshot_files
        .last()
        .map(|file| file.end_slot)
        .unwrap_or(last_indexed_slot);

    let (mut temp_file, temp_file_path) = create_temp_snapshot_file("incremental_snapshot/");

    pin_mut!(blocks);
    while let Some(block) = blocks.next().await {
        let slot = block.metadata.slot;
        serialize_block_to_file(&block, &mut temp_file);

        let write_full_snapshot = slot - last_full_snapshot_slot + (last_indexed_slot == 0) as u64
            >= full_snapshot_interval_slots;
        let write_incremental_snapshot = slot - last_snapshot_slot
            + (last_snapshot_slot == 0) as u64
            >= incremental_snapshot_interval_slots;

        if write_full_snapshot || write_incremental_snapshot {
            let snapshot_file_path =
                snapshot_dir.join(format!("snapshot-{}-{}", last_snapshot_slot + 1, slot));
            fs::rename(&temp_file_path, &snapshot_file_path).unwrap();
            temp_file = create_temp_snapshot_file("incremental_snapshot/").0;
            last_snapshot_slot = slot;

            if write_full_snapshot {
                merge_snapshots(snapshot_dir).await;
                last_full_snapshot_slot = slot;
            }
        }
    }
}

pub fn load_block_stream_from_snapshot_directory(
    snapshot_dir: &Path,
) -> impl Stream<Item = BlockInfo> {
    let snapshot_files = get_snapshot_files_with_slots(snapshot_dir);
    stream! {
        for snapshot_file in snapshot_files {
            let file = File::open(&snapshot_file.file).unwrap();
            let mut reader = BufReader::new(file);
            reader.seek(io::SeekFrom::Start(8)).unwrap();
            loop {
                let block = bincode::deserialize_from(&mut reader);
                match block {
                    Ok(block) => { yield block;}
                    Err(e) => {
                        println!("Error deserializing block: {:?}", e);
                        break
                    }
                }
            }
        }
    }
}
