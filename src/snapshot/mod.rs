use std::{
    env::temp_dir,
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use async_std::stream::StreamExt;
use async_stream::stream;
use futures::{pin_mut, Stream};
use log::{error, info};

pub use crate::common::{
    fetch_block_parent_slot, get_network_start_slot, setup_logging, setup_metrics, LoggingFormat,
};
use crate::ingester::{
    fetchers::BlockStreamConfig,
    parser::ACCOUNT_COMPRESSION_PROGRAM_ID,
    typedefs::block_info::{BlockInfo, Instruction, TransactionInfo},
};

const SNAPSHOT_VERSION: u8 = 1;

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
    pub file: PathBuf,
    pub start_slot: u64,
    pub end_slot: u64,
}

pub fn get_snapshot_files_with_slots(
    snapshot_dir: &Path,
) -> anyhow::Result<Vec<SnapshotFileWithSlots>> {
    let snapshot_files = fs::read_dir(snapshot_dir)?
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    let mut snapshot_files_with_slots = Vec::new();

    for file in snapshot_files {
        // Make this return an error if file name is not in the expected format
        let file_name = file
            .file_name()
            .ok_or(anyhow!("Missing file name".to_string()))?
            .to_str()
            .ok_or(anyhow!("Invalid file name"))?;
        let parts: Vec<&str> = file_name.split('-').collect();
        if parts.len() == 3 {
            let start_slot = parts[1].parse::<u64>()?;
            let end_slot = parts[2].parse::<u64>()?;
            snapshot_files_with_slots.push(SnapshotFileWithSlots {
                file,
                start_slot,
                end_slot,
            });
        }
    }
    snapshot_files_with_slots.sort_by_key(|file| file.start_slot);
    Ok(snapshot_files_with_slots)
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
    let temp_file = File::create(&temp_file_path).unwrap();
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
    full_snapshot_interval_slots: u64,
    incremental_snapshot_interval_slots: u64,
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
    let snapshot_files = get_snapshot_files_with_slots(snapshot_dir).unwrap();

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
            info!(
                "Creating incremental snapshot file {:?} for slot {}",
                snapshot_file_path.to_str(),
                slot
            );
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

/// Loads a stream of bytes from snapshot files in the given directory.
pub fn load_byte_stream_from_snapshot_directory(
    snapshot_dir: &PathBuf,
) -> impl Stream<Item = Result<u8>> {
    // Create an asynchronous stream of bytes from the snapshot files
    let snapshot_dir = snapshot_dir.clone();
    stream! {
        let snapshot_files =
        get_snapshot_files_with_slots(&snapshot_dir).context("Failed to retrieve snapshot files")?;

        if snapshot_files.is_empty() {
            yield Err(anyhow!("No snapshot files found"));
        }

        // Yield the snapshot version byte
        yield Ok(SNAPSHOT_VERSION);

        let start_slot = snapshot_files.first().map(|file| file.start_slot).unwrap();
        let end_slot = snapshot_files.last().map(|file| file.end_slot).unwrap();

        for byte in start_slot.to_le_bytes().iter() {
            yield Ok(*byte);
        }
        for byte in end_slot.to_le_bytes().iter() {
            yield Ok(*byte);
        }

        // Iterate over each snapshot file
        for snapshot_file in snapshot_files {
            // Use anyhow context to add more error information
            let file = File::open(&snapshot_file.file)
                .with_context(|| format!("Failed to open snapshot file: {:?}", snapshot_file.file))?;
            let mut reader = BufReader::new(file);
            let mut buffer = [0; 1024];

            // Read bytes from the file in chunks
            loop {
                let n = reader.read(&mut buffer)
                    .context("Failed to read from snapshot file")?;

                if n == 0 {
                    break; // EOF reached
                }

                // Yield each byte from the buffer
                for &byte in &buffer[..n] {
                    yield Ok(byte);
                }
            }
        }
    }
}

pub fn load_block_stream_from_snapshot_directory(
    snapshot_dir: &Path,
) -> impl Stream<Item = BlockInfo> {
    let snapshot_files = get_snapshot_files_with_slots(snapshot_dir).unwrap();
    stream! {
        for snapshot_file in snapshot_files {
            let file = File::open(&snapshot_file.file).unwrap();
            let mut reader = BufReader::new(file);
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

pub async fn create_snapshot_from_byte_stream(
    byte_stream: impl Stream<Item = Result<u8>>,
    snapshot_dir: &PathBuf,
) -> Result<()> {
    pin_mut!(byte_stream);

    // Skip snapshot version byte
    byte_stream.next().await.unwrap().unwrap();

    // The start slot is the first 8 bytes of the snapshot
    let mut start_slot_bytes = [0u8; 8];
    for i in 0..8 {
        start_slot_bytes[i] = byte_stream
            .next()
            .await
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("Failed to read start slot byte {}", i))?;
    }
    let start_slot = u64::from_le_bytes(start_slot_bytes);

    // The end slot is the next 8 bytes of the snapshot
    let mut end_slot_bytes = [0u8; 8];
    for i in 0..8 {
        end_slot_bytes[i] = byte_stream
            .next()
            .await
            .transpose()?
            .ok_or_else(|| anyhow::anyhow!("Failed to read end slot byte {}", i))?;
    }
    let end_slot = u64::from_le_bytes(end_slot_bytes);
    let snapshot_name = format!("snapshot-{}-{}", start_slot, end_slot);
    let snapshot_file_path = Path::new(&snapshot_dir).join(snapshot_name);
    println!("Creating snapshot file: {:?}", snapshot_file_path);

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&snapshot_file_path)
        .context("Failed to open snapshot file")
        .unwrap();

    // Buffer to hold bytes
    let mut buffer = Vec::with_capacity(8192); // 8 KB buffer

    // Process the byte stream
    while let Some(byte_result) = byte_stream.next().await {
        match byte_result {
            Ok(byte) => {
                buffer.push(byte);

                // Write to file if buffer is full
                if buffer.len() >= buffer.capacity() {
                    file.write_all(&buffer)
                        .context("Failed to write buffer to file")?;
                    buffer.clear();
                }
            }
            Err(e) => {
                error!("Error receiving byte: {:?}", e);
                return Err(anyhow::anyhow!("Failed to receive byte"));
            }
        }
    }

    // Write any remaining bytes in the buffer
    if !buffer.is_empty() {
        file.write_all(&buffer)
            .context("Failed to write remaining buffer to file")?;
    }

    // Ensure the file is properly flushed
    file.flush().context("Failed to flush file")?;

    info!(
        "Snapshot downloaded successfully to {:?}",
        snapshot_file_path
    );
    Ok(())
}
