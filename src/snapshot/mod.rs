use std::{
    env::temp_dir,
    fs::{self, File, OpenOptions},
    io::{BufReader, Error, ErrorKind, Read, Write},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::Poll,
};

pub use crate::common::{
    fetch_block_parent_slot, get_network_start_slot, setup_logging, setup_metrics, LoggingFormat,
};
use crate::ingester::parser::get_compression_program_id;
use crate::ingester::{
    fetchers::BlockStreamConfig,
    typedefs::block_info::{BlockInfo, Instruction, TransactionInfo},
};
use anyhow::{anyhow, Context as AnyhowContext, Result};
use async_stream::stream;
use bytes::{BufMut, Bytes};
use futures::stream::StreamExt;
use futures::{pin_mut, stream, Stream};
use log::info;
use s3::creds::Credentials;
use s3::region::Region;
use s3::{bucket::Bucket, BucketConfiguration};
use s3_utils::multipart_upload::put_object_stream_custom;
use tokio::io::{AsyncRead, ReadBuf};

pub mod s3_utils;

pub const MEGABYTE: usize = 1024 * 1024;
pub const CHUNK_SIZE: usize = 100 * 1024 * 1024;
// Up to 50 MB
pub const TRANSACTIONS_TO_ACCUMULATE: usize = 5000;

const SNAPSHOT_VERSION: u8 = 1;

pub struct R2DirectoryAdapter {
    pub r2_bucket: Bucket,
    pub r2_prefix: String,
}

pub struct R2BucketArgs {
    pub r2_credentials: Credentials,
    pub r2_region: Region,
    pub r2_bucket: String,
    pub create_bucket: bool,
}

pub async fn get_r2_bucket(args: R2BucketArgs) -> Bucket {
    let bucket = Bucket::new(
        args.r2_bucket.as_str(),
        args.r2_region.clone(),
        args.r2_credentials.clone(),
    )
    .unwrap()
    .with_path_style();
    if args.create_bucket {
        // Check if the bucket already exists
        let bucket_exists = bucket.exists().await.unwrap();
        if !bucket_exists {
            Bucket::create_with_path_style(
                args.r2_bucket.as_str(),
                args.r2_region.clone(),
                args.r2_credentials.clone(),
                BucketConfiguration::default(),
            )
            .await
            .unwrap();
        }
    }
    bucket
}

struct StreamReader<S> {
    stream: S,
    byte_buffer: Vec<u8>,
}

impl<S> AsyncRead for StreamReader<S>
where
    S: stream::Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if !self.byte_buffer.is_empty() {
            let len = std::cmp::min(self.byte_buffer.len(), buf.remaining_mut());
            buf.put_slice(&self.byte_buffer[..len]);
            self.byte_buffer.drain(..len);
            return Poll::Ready(Ok(()));
        }
        match futures::ready!(self.stream.poll_next_unpin(cx)) {
            Some(Ok(chunk)) => {
                self.byte_buffer.extend_from_slice(chunk.as_ref());
                let len = std::cmp::min(self.byte_buffer.len(), buf.remaining_mut());
                buf.put_slice(&self.byte_buffer[..len]);
                self.byte_buffer.drain(..len);
                Poll::Ready(Ok(()))
            }
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => Poll::Ready(Ok(())), // EOF
        }
    }
}

impl R2DirectoryAdapter {
    async fn read_file(
        arc_self: Arc<Self>,
        path: String,
    ) -> impl Stream<Item = Result<Bytes>> + std::marker::Send + 'static {
        stream! {
            let r2_directory_adapter = arc_self.clone();
            let mut result = r2_directory_adapter.r2_bucket.get_object_stream(path.clone()).await.with_context(|| format!("Failed to read file: {:?}", path))?;
            let stream = result.bytes();

            while let Some(byte) = stream.next().await {
                let byte = byte.with_context(|| "Failed to read byte from file").unwrap();
                yield Ok(byte);
            }
        }
    }

    async fn list_files(&self) -> Result<Vec<String>> {
        let results = self
            .r2_bucket
            .list(self.r2_prefix.clone(), None)
            .await
            .unwrap_or_default();

        let mut files = Vec::new();
        for result in results {
            for object in result.contents {
                files.push(object.key);
            }
        }
        Ok(files)
    }

    async fn delete_file(&self, path: String) -> Result<()> {
        self.r2_bucket.delete_object(path).await?;
        Ok(())
    }

    async fn write_file(
        &self,
        path: String,
        byte_stream: impl Stream<Item = Result<Bytes>> + std::marker::Send + 'static,
    ) -> Result<()> {
        let path = format!("{}/{}", self.r2_prefix, path);

        pin_mut!(byte_stream);
        // Create a stream that converts `Result<u8, S3Error>` to `Result<Vec<u8>, S3Error>`
        let byte_stream =
            byte_stream.map(|bytes| bytes.map_err(|e| Error::new(ErrorKind::Other, e)));

        let mut stream_reader = StreamReader {
            stream: byte_stream,
            byte_buffer: Vec::new(),
        };
        // Stream the bytes directly to S3 without collecting them in memory
        put_object_stream_custom(&self.r2_bucket, &mut stream_reader, &path).await?;
        Ok(())
    }
}

pub struct FileSystemDirectoryApapter {
    pub snapshot_dir: String,
}

impl FileSystemDirectoryApapter {
    async fn read_file(&self, path: String) -> impl Stream<Item = Result<Bytes>> + Send {
        let path = format!("{}/{}", self.snapshot_dir, path);
        let file = OpenOptions::new().read(true).open(path).unwrap();
        let bytes = BufReader::new(file).bytes();
        stream! {
            let mut byte_chunk = vec![];
            for byte in bytes.into_iter() {
                byte_chunk.push(byte.with_context(|| "Failed to read byte from file")?);
                if byte_chunk.len() == CHUNK_SIZE {
                    yield Ok(Bytes::from(byte_chunk.clone()));
                    byte_chunk.clear();
                }
            }
            if !byte_chunk.is_empty() {
                yield Ok(Bytes::from(byte_chunk));
            }
        }
    }

    async fn list_files(&self) -> Result<Vec<String>> {
        if !PathBuf::new().join(&self.snapshot_dir).exists() {
            return Ok(Vec::new());
        }
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

    async fn write_file(
        &self,
        path: String,
        bytes: impl Stream<Item = Result<Bytes>>,
    ) -> Result<()> {
        let (mut temp_file, temp_path) = create_temp_snapshot_file(&self.snapshot_dir);
        pin_mut!(bytes);
        while let Some(byte) = bytes.next().await {
            let byte = byte?;
            temp_file.write_all(&byte).unwrap();
        }

        // Create snapshot directory if it doesn't exist
        if !PathBuf::new().join(&self.snapshot_dir).exists() {
            fs::create_dir_all(&self.snapshot_dir).unwrap();
        }
        let path = format!("{}/{}", self.snapshot_dir, path);
        fs::rename(temp_path.clone(), path.clone())
            .with_context(|| format!("Failed to rename file: {:?} -> {:?}", temp_path, path))?;
        Ok(())
    }
}

/// Struct representing a directory adapter that can read and write files
/// HACK: This should definitely be a trait, but we used a struct to get around some cryptic
///       compiler errors
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

    pub fn from_local_directory(snapshot_dir: String) -> Self {
        Self::new(Some(FileSystemDirectoryApapter { snapshot_dir }), None)
    }

    pub async fn from_r2_bucket_and_prefix_and_env(r2_bucket: String, r2_prefix: String) -> Self {
        // Get endpoint url, access key, region, and secret key from environment variables
        let r2_credentials = Credentials::new(
            Some(&std::env::var("R2_ACCESS_KEY").unwrap()),
            Some(&std::env::var("R2_SECRET_KEY").unwrap()),
            None,
            None,
            None,
        )
        .unwrap();
        let r2_region = Region::R2 {
            account_id: std::env::var("R2_ACCOUNT_ID").unwrap(),
        };
        let r2_bucket_args = R2BucketArgs {
            r2_credentials,
            r2_region,
            r2_bucket,
            create_bucket: false,
        };
        let r2_bucket = get_r2_bucket(r2_bucket_args).await;
        Self::new(
            None,
            Some(R2DirectoryAdapter {
                r2_bucket,
                r2_prefix,
            }),
        )
    }

    /// Reads the contents of a file at the given path
    async fn read_file(&self, path: String) -> impl Stream<Item = Result<Bytes>> + 'static {
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
    pub async fn delete_file(&self, path: String) -> Result<()> {
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
        bytes: impl Stream<Item = Result<Bytes>> + std::marker::Send + 'static,
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

fn is_compression_instruction(instruction: &Instruction) -> bool {
    instruction.program_id == get_compression_program_id()
        || instruction.accounts.contains(&get_compression_program_id())
}

pub fn is_compression_transaction(tx: &TransactionInfo) -> bool {
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

#[derive(Debug)]
pub struct SnapshotFileWithSlots {
    pub file: String,
    pub start_slot: u64,
    pub end_slot: u64,
}

pub async fn get_snapshot_files_with_metadata(
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
        fs::create_dir_all(&temp_dir).unwrap();
    }
    let random_number = rand::random::<u64>();
    let temp_file_path = temp_dir.join(format!("temp-snapshot-{}", random_number));
    if temp_file_path.exists() {
        fs::remove_file(&temp_file_path).unwrap();
    }
    println!("Creating temp file: {:?}", temp_file_path);
    let temp_file = File::create(&temp_file_path).unwrap();
    (temp_file, temp_file_path)
}

async fn merge_snapshots(directory_adapter: Arc<DirectoryAdapter>) {
    let snapshot_files = get_snapshot_files_with_metadata(directory_adapter.as_ref())
        .await
        .unwrap();
    let start_slot = snapshot_files.first().map(|file| file.start_slot).unwrap();
    let end_slot = snapshot_files.last().map(|file| file.end_slot).unwrap();
    info!(
        "Merging snapshots from slot {} to slot {}",
        start_slot, end_slot
    );
    let byte_stream = load_byte_stream_from_directory_adapter(directory_adapter.clone()).await;
    create_snapshot_from_byte_stream(byte_stream, directory_adapter.as_ref())
        .await
        .unwrap();
    for snapshot_file in snapshot_files {
        directory_adapter
            .delete_file(snapshot_file.file)
            .await
            .unwrap();
    }
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
    blocks_stream: impl Stream<Item = Vec<BlockInfo>>,
    last_indexed_slot: u64,
    incremental_snapshot_interval_slots: u64,
    full_snapshot_interval_slots: u64,
) {
    let snapshot_files = get_snapshot_files_with_metadata(directory_adapter.as_ref())
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

    pin_mut!(blocks_stream);
    while let Some(blocks) = blocks_stream.next().await {
        for block in blocks {
            let slot = block.metadata.slot;
            let write_full_snapshot = slot - last_full_snapshot_slot
                + (last_indexed_slot == 0) as u64
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
            byte_buffer.extend(block_bytes);

            if write_incremental_snapshot {
                let snapshot_file_path = format!("snapshot-{}-{}", last_snapshot_slot + 1, slot);
                info!("Writing snapshot file: {}", snapshot_file_path);
                let byte_buffer_clone = byte_buffer.clone();
                let byte_stream = stream! {
                    yield Ok(Bytes::from(byte_buffer_clone));
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
}

pub async fn load_byte_stream_from_directory_adapter(
    directory_adapter: Arc<DirectoryAdapter>,
) -> impl Stream<Item = Result<Bytes>> + 'static {
    // Create an asynchronous stream of bytes from the snapshot files
    stream! {
        let snapshot_files =
            get_snapshot_files_with_metadata(directory_adapter.as_ref()).await.context("Failed to retrieve snapshot files")?;
        if snapshot_files.is_empty() {
            yield Err(anyhow!("No snapshot files found"));
        }

        // Yield the snapshot version byte
        yield Ok(Bytes::from(vec![SNAPSHOT_VERSION]));

        let start_slot = snapshot_files.first().map(|file| file.start_slot).unwrap();
        let end_slot = snapshot_files.last().map(|file| file.end_slot).unwrap();

        let start_slot = start_slot.to_le_bytes();
        let end_slot = end_slot.to_le_bytes();
        yield Ok(Bytes::from(start_slot.to_vec()));
        yield Ok(Bytes::from(end_slot.to_vec()));

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
) -> impl Stream<Item = Vec<BlockInfo>> {
    stream! {
        let byte_stream = load_byte_stream_from_directory_adapter(directory_adapter.clone()).await;
        pin_mut!(byte_stream);
        // Skip the snapshot version byte
        let snapshot_version = byte_stream.next().await.unwrap().unwrap();
        let snapshot_version = snapshot_version[0];


        if snapshot_version != SNAPSHOT_VERSION {
            panic!("Unsupported snapshot version: {}. Please upgrade Photon package", snapshot_version);
        }
        // Skip the start slot and end slot
        for _ in 0..2 {
            byte_stream.next().await.unwrap().unwrap();
        }

        let mut reader = Vec::new();
        let mut index = 0;
        let mut accumulated_blocks = Vec::new();
        let mut accumulated_transactions = 0;

        while let Some(bytes) = byte_stream.next().await {
            let bytes = bytes.unwrap();
            reader.extend(&bytes);
            while reader.len() - index > CHUNK_SIZE {
                let block: BlockInfo = bincode::deserialize(&reader[index..]).unwrap();
                let size = bincode::serialized_size(&block).unwrap() as usize;
                index += size;
                accumulated_transactions += block.transactions.len();
                accumulated_blocks.push(block);
                if accumulated_transactions >= TRANSACTIONS_TO_ACCUMULATE {
                    yield accumulated_blocks;
                    accumulated_blocks = Vec::new();
                    accumulated_transactions = 0;
                }
            }
            if index > 0 {
                reader.drain(..index);
                index = 0;
            }
        }

        while index < reader.len() {
            let block: BlockInfo = bincode::deserialize(&reader[index..]).unwrap();
            let size = bincode::serialized_size(&block).unwrap() as usize;
            index += size;
            accumulated_transactions += block.transactions.len();
            accumulated_blocks.push(block);
            if accumulated_transactions >= TRANSACTIONS_TO_ACCUMULATE {
                yield accumulated_blocks;
                accumulated_blocks = Vec::new();
                accumulated_transactions = 0;
            }
        }

        if !accumulated_blocks.is_empty() {
            yield accumulated_blocks;
        }
    }
}

pub async fn create_snapshot_from_byte_stream(
    byte_stream: impl Stream<Item = Result<Bytes, anyhow::Error>> + std::marker::Send + 'static,
    directory_adapter: &DirectoryAdapter,
) -> Result<()> {
    // Skip snapshot version byte
    let mut byte_stream: Pin<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send>> =
        Box::pin(byte_stream);

    let mut byte_buffer = Vec::new();
    while let Some(byte) = byte_stream.next().await {
        let byte = byte?;
        byte_buffer.extend(byte.iter().copied());
        // 1 byte for version, 8 bytes for start slot, 8 bytes for end slot
        if byte_buffer.len() > 17 {
            break;
        }
    }
    // Snapshot version is the first byte
    let snapshot_version = byte_buffer.remove(0);

    if snapshot_version != SNAPSHOT_VERSION {
        panic!(
            "Unsupported snapshot version: {}. Please upgrade Photon package",
            snapshot_version
        );
    }
    let start_slot_bytes: [u8; 8] = byte_buffer
        .drain(..8)
        .collect::<Vec<u8>>()
        .try_into()
        .unwrap();
    let start_slot = u64::from_le_bytes(start_slot_bytes);
    let end_slot_bytes: [u8; 8] = byte_buffer
        .drain(..8)
        .collect::<Vec<u8>>()
        .try_into()
        .unwrap();
    let end_slot = u64::from_le_bytes(end_slot_bytes);
    let snapshot_name = format!("snapshot-{}-{}", start_slot, end_slot);
    info!("Creating snapshot: {}", snapshot_name);
    let byte_stream = stream! {
        yield Ok(Bytes::from(byte_buffer));
        while let Some(byte) = byte_stream.next().await {
            yield byte;
        }
    };
    directory_adapter
        .write_file(snapshot_name.clone(), byte_stream)
        .await?;

    info!("Snapshot downloaded successfully to {:?}", snapshot_name);
    Ok(())
}
