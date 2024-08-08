use std::{
    env::temp_dir,
    fs::{self, File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
    pin::{pin, Pin},
    sync::Arc,
};

pub use crate::common::{
    fetch_block_parent_slot, get_network_start_slot, setup_logging, setup_metrics, LoggingFormat,
};
use crate::ingester::{
    fetchers::BlockStreamConfig,
    parser::ACCOUNT_COMPRESSION_PROGRAM_ID,
    typedefs::block_info::{BlockInfo, Instruction, TransactionInfo},
};
use anyhow::{anyhow, Context, Result};
use async_std::stream::StreamExt;
use async_stream::stream;
use async_trait::async_trait;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::{config::Builder, primitives::ByteStream};
use aws_sdk_s3::{config::Credentials, primitives::SdkBody};
use aws_sdk_s3::{Client, Config};
use futures::{pin_mut, Stream};
use hyper::{body::Bytes, Body};
use log::{error, info};
use std::str::FromStr;
use tokio::io::AsyncReadExt;

pub struct R2DirectoryAdapter {
    client: Client,
    r2_bucket: String,
    r2_prefix: String,
}

impl R2DirectoryAdapter {
    pub fn new(r2_bucket: String, r2_prefix: String) -> Self {
        let region = Region::new("auto"); // Use "auto" for R2

        let credentials = Credentials::new(
            "your-access-key-id",
            "your-secret-access-key",
            None,
            None,
            "static",
        );

        let config = Config::builder()
            .region(region)
            .credentials_provider(credentials)
            .endpoint_url("https://www.example.com")
            .build();

        let client = Client::from_conf(config);
        Self {
            client,
            r2_bucket,
            r2_prefix,
        }
    }

    async fn read_file(
        arc_self: Arc<Self>,
        path: String,
    ) -> impl Stream<Item = Result<u8>> + std::marker::Send + 'static {
        stream! {
            let r2_directory_adapter = arc_self.clone();
            let path = format!("{}/{}", r2_directory_adapter.r2_prefix, path);
            let request = r2_directory_adapter
            .client
            .get_object()
            .bucket(r2_directory_adapter.r2_bucket.clone())
            .key(path.clone());

            let response = request.send().await.with_context(|| format!("Failed to read file: {:?}", path))?;
            let body = response.body;
            let mut stream = body.into_async_read();

            let mut buf = [0; 1024];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                for i in 0..n {
                    yield Ok(buf[i]);
                }
            }
        }
    }

    async fn list_files(&self) -> Result<Vec<String>> {
        let request = self
            .client
            .list_objects_v2()
            .bucket(self.r2_bucket.clone())
            .prefix(self.r2_prefix.clone());

        let response = request
            .send()
            .await
            .with_context(|| "Failed to list files")?;
        let objects = response.contents.unwrap_or_default();
        let mut files = Vec::new();
        for object in objects {
            if let Some(key) = object.key {
                files.push(key);
            }
        }
        Ok(files)
    }

    async fn delete_file(&self, path: String) -> Result<()> {
        let path = format!("{}/{}", self.r2_prefix, path);
        let request = self
            .client
            .delete_object()
            .bucket(self.r2_bucket.clone())
            .key(path.clone());

        request
            .send()
            .await
            .with_context(|| format!("Failed to delete file: {:?}", path))?;
        Ok(())
    }

    async fn write_file(
        &self,
        path: String,
        bytes: impl Stream<Item = Result<u8>> + std::marker::Send + 'static,
    ) -> Result<()> {
        let path = format!("{}/{}", self.r2_prefix, path);
        let request = self
            .client
            .put_object()
            .bucket(self.r2_bucket.clone())
            .key(path.clone());

        let byte_stream = bytes.map(|result| {
            result.map(|byte| {
                let mut vec = Vec::new();
                vec.push(byte);
                Bytes::from(vec)
            })
        });

        let body = Body::wrap_stream(byte_stream);
        let sdk_body = SdkBody::from(body);

        let byte_stream = ByteStream::new(sdk_body);
        request
            .body(byte_stream)
            .send()
            .await
            .with_context(|| format!("Failed to write file: {:?}", path))?;

        Ok(())
    }
}

pub struct FileSystemDirectoryApapter {
    pub snapshot_dir: String,
}

impl FileSystemDirectoryApapter {
    async fn read_file(&self, path: String) -> impl Stream<Item = Result<u8>> {
        let path = format!("{}/{}", self.snapshot_dir, path);
        let file = OpenOptions::new().read(true).open(path).unwrap();
        let reader = BufReader::new(file);
        stream! {
            for byte in reader.bytes() {
                yield byte.with_context(|| "Failed to read byte from file");
            }
        }
    }

    async fn list_files(&self) -> Result<Vec<String>> {
        let files = fs::read_dir(&self.snapshot_dir)
            .with_context(|| format!("Failed to read directory: {:?}", self.snapshot_dir))?;
        let mut file_names = Vec::new();
        for file in files {
            let file = file?;
            let file_name = file.file_name().into_string().unwrap();
            file_names.push(file_name);
        }
        Ok(file_names)
    }

    async fn delete_file(&self, path: String) -> Result<()> {
        let path = format!("{}/{}", self.snapshot_dir, path);
        fs::remove_file(path.clone())
            .with_context(|| format!("Failed to delete file: {:?}", path))?;
        Ok(())
    }

    async fn write_file(&self, path: String, bytes: impl Stream<Item = Result<u8>>) -> Result<()> {
        let (mut temp_file, temp_path) = create_temp_snapshot_file(&self.snapshot_dir);
        pin_mut!(bytes);
        while let Some(byte) = bytes.next().await {
            let byte = byte?;
            temp_file
                .write_all(&[byte])
                .with_context(|| "Failed to write byte to file")?;
        }
        let path = format!("{}/{}", self.snapshot_dir, path);
        fs::rename(temp_path.clone(), path.clone())
            .with_context(|| format!("Failed to rename file: {:?} -> {:?}", temp_path, path))?;
        Ok(())
    }
}

/// Trait representing a file system adapter
pub struct DirectoryAdapter {
    filesystem_directory_adapter: Option<Arc<FileSystemDirectoryApapter>>,
    r2_directory_adapter: Option<Arc<R2DirectoryAdapter>>,
}

impl DirectoryAdapter {
    pub fn new(
        filesystem_directory_adapter: Option<FileSystemDirectoryApapter>,
        r2_directory_adapter: Option<R2DirectoryAdapter>,
    ) -> Self {
        match (&filesystem_directory_adapter, &r2_directory_adapter) {
            (Some(_snapshot_dir), None) => {}
            (None, Some(_r2_bucket)) => {}
            _ => panic!("Either snapshot_dir or r2_bucket must be provided"),
        };

        Self {
            filesystem_directory_adapter: filesystem_directory_adapter.map(Arc::new),
            r2_directory_adapter: r2_directory_adapter.map(Arc::new),
        }
    }

    /// Reads the contents of a file at the given path
    async fn read_file(&self, path: String) -> impl Stream<Item = Result<u8>> + 'static {
        let file_system_directory_adapter = self.filesystem_directory_adapter.clone();
        let r2_directory_adapter = self.r2_directory_adapter.clone();
        stream! {
            if let Some(filesystem_directory_adapter) = file_system_directory_adapter {
                let stream = filesystem_directory_adapter.read_file(path).await;
                pin_mut!(stream);
                while let Some(byte) = stream.next().await {
                    yield byte;
                }
            } else if let Some(r2_directory_adapter) = r2_directory_adapter {
                let stream = R2DirectoryAdapter::read_file(r2_directory_adapter, path).await;
                pin_mut!(stream);
                while let Some(byte) = stream.next().await {
                    yield byte;
                }
            } else {
                panic!("No directory adapter provided");
            }
        }
    }

    /// Writes data to a file at the given path
    async fn list_files(&self) -> Result<Vec<String>> {
        if let Some(filesystem_directory_adapter) = &self.filesystem_directory_adapter {
            filesystem_directory_adapter.list_files().await
        } else if let Some(r2_directory_adapter) = &self.r2_directory_adapter {
            r2_directory_adapter.list_files().await
        } else {
            panic!("No directory adapter provided");
        }
    }

    /// Deletes the file at the given path
    async fn delete_file(&self, path: String) -> Result<()> {
        if let Some(filesystem_directory_adapter) = &self.filesystem_directory_adapter {
            filesystem_directory_adapter.delete_file(path).await
        } else if let Some(r2_directory_adapter) = &self.r2_directory_adapter {
            r2_directory_adapter.delete_file(path).await
        } else {
            panic!("No directory adapter provided");
        }
    }

    /// Write file to the given path
    async fn write_file(
        &self,
        path: String,
        bytes: impl Stream<Item = Result<u8>> + std::marker::Send + 'static,
    ) -> Result<()> {
        if let Some(filesystem_directory_adapter) = &self.filesystem_directory_adapter {
            filesystem_directory_adapter.write_file(path, bytes).await
        } else if let Some(r2_directory_adapter) = &self.r2_directory_adapter {
            r2_directory_adapter.write_file(path, bytes).await
        } else {
            panic!("No directory adapter provided");
        }
    }
}

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

pub struct SnapshotFileWithSlots {
    pub file: String,
    pub start_slot: u64,
    pub end_slot: u64,
}

pub async fn get_snapshot_files_with_slots(
    directory_adapter: &DirectoryAdapter,
) -> anyhow::Result<Vec<SnapshotFileWithSlots>> {
    let snapshot_files = directory_adapter.list_files().await?;
    let mut snapshot_files_with_slots = Vec::new();

    for file in snapshot_files {
        // Make this return an error if file name is not in the expected format
        let parts: Vec<&str> = file.split('-').collect();
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
    let random_number = rand::random::<u64>();
    let temp_file_path = temp_dir.join(format!("temp-snapshot-{}", random_number));
    if temp_file_path.exists() {
        fs::remove_file(&temp_file_path).unwrap();
    }
    let temp_file = File::create(&temp_file_path).unwrap();
    (temp_file, temp_file_path)
}

async fn merge_snapshots(directory_adapter: Arc<DirectoryAdapter>) {
    let (mut temp_file, temp_file_path) = create_temp_snapshot_file("fullsnaphot/");

    let byte_stream = load_byte_stream_from_snapshot_directory(directory_adapter.clone()).await;
    create_snapshot_from_byte_stream(byte_stream, directory_adapter.as_ref())
        .await
        .unwrap();
}

pub async fn update_snapshot(
    directory_adapter: Arc<DirectoryAdapter>,
    block_stream_config: BlockStreamConfig,
    full_snapshot_interval_slots: u64,
    incremental_snapshot_interval_slots: u64,
) {
    // Convert stream to iterator
    let block_stream = block_stream_config.load_block_stream();
    update_snapshot_helper(
        directory_adapter,
        block_stream,
        block_stream_config.last_indexed_slot,
        incremental_snapshot_interval_slots,
        full_snapshot_interval_slots,
    )
    .await;
}

pub async fn update_snapshot_helper(
    directory_adapter: Arc<DirectoryAdapter>,
    blocks: impl Stream<Item = BlockInfo>,
    last_indexed_slot: u64,
    incremental_snapshot_interval_slots: u64,
    full_snapshot_interval_slots: u64,
) {
    let snapshot_files = get_snapshot_files_with_slots(directory_adapter.as_ref())
        .await
        .unwrap();

    let mut last_full_snapshot_slot = snapshot_files
        .first()
        .map(|file| file.end_slot)
        .unwrap_or(last_indexed_slot);
    let mut last_snapshot_slot = snapshot_files
        .last()
        .map(|file| file.end_slot)
        .unwrap_or(last_indexed_slot);

    let mut byte_buffer = Vec::new();

    pin_mut!(blocks);
    while let Some(block) = blocks.next().await {
        let slot = block.metadata.slot;

        let write_full_snapshot = slot - last_full_snapshot_slot + (last_indexed_slot == 0) as u64
            >= full_snapshot_interval_slots;
        let write_incremental_snapshot = slot - last_snapshot_slot
            + (last_snapshot_slot == 0) as u64
            >= incremental_snapshot_interval_slots;

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
        byte_buffer.push(block_bytes);

        if write_incremental_snapshot {
            let snapshot_file_path = format!("snapshot-{}-{}", last_snapshot_slot + 1, slot);
            info!(
                "Creating incremental snapshot file {:?} for slot {}",
                snapshot_file_path, slot
            );
            let byte_buffer_clone = byte_buffer.clone();
            let byte_stream = stream! {
                for block in byte_buffer_clone {
                    for byte in block {
                        yield Ok(byte);
                    }
                }
            };
            directory_adapter
                .as_ref()
                .write_file(snapshot_file_path, byte_stream)
                .await
                .unwrap();
            byte_buffer.clear();
            last_snapshot_slot = slot;
        }
        if write_full_snapshot {
            merge_snapshots(directory_adapter.clone()).await;
            last_full_snapshot_slot = slot;
        }
    }
}

pub async fn load_byte_stream_from_snapshot_directory(
    directory_adapter: Arc<DirectoryAdapter>,
) -> impl Stream<Item = Result<u8>> + 'static {
    // Create an asynchronous stream of bytes from the snapshot files
    stream! {
        let snapshot_files =
            get_snapshot_files_with_slots(directory_adapter.as_ref()).await.context("Failed to retrieve snapshot files")?;

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
            let byte_stream = directory_adapter.read_file(snapshot_file.file.clone()).await;
            pin_mut!(byte_stream);
            while let Some(byte) = byte_stream.next().await {
                yield byte;
            }
        }
    }
}

pub async fn load_block_stream_from_directory_adapter(
    directory_adapter: Arc<DirectoryAdapter>,
) -> impl Stream<Item = BlockInfo> {
    stream! {
        let byte_stream = load_byte_stream_from_snapshot_directory(directory_adapter.clone()).await;
        pin_mut!(byte_stream);
        // Skip the snapshot version byte
        byte_stream.next().await.unwrap().unwrap();
        // Skip the start slot and end slot
        for _ in 0..16 {
            byte_stream.next().await.unwrap().unwrap();
        }
        let mut reader = Vec::new();
        while let Some(byte) = byte_stream.next().await {
            let byte = byte.unwrap();
            reader.push(byte);
            if reader.len() > 2_usize.pow(20) {
                let block = bincode::deserialize(&reader).unwrap();
                // 
                yield block;
                reader.clear();
            }
        }
    }
}

pub async fn create_snapshot_from_byte_stream(
    byte_stream: impl Stream<Item = Result<u8, anyhow::Error>> + std::marker::Send + 'static,
    directory_adapter: &DirectoryAdapter,
) -> Result<()> {
    // Skip snapshot version byte
    let mut byte_stream: Pin<Box<dyn Stream<Item = Result<u8, anyhow::Error>> + Send>> =
        Box::pin(byte_stream);

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
    println!("Creating snapshot file: {:?}", snapshot_name);
    directory_adapter
        .write_file(snapshot_name.clone(), byte_stream)
        .await?;

    info!("Snapshot downloaded successfully to {:?}", snapshot_name);
    Ok(())
}
