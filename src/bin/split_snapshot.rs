use clap::Parser;
use photon_indexer::ingester::typedefs::block_info::BlockInfo;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input snapshot file path
    #[arg(short, long)]
    input: String,

    /// Number of blocks per chunk
    #[arg(short, long, default_value = "1000000")]
    blocks_per_chunk: usize,

    /// Output directory (defaults to current directory)
    #[arg(short, long, default_value = ".")]
    output_dir: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Create output directory if it doesn't exist
    std::fs::create_dir_all(&args.output_dir)?;

    // Open input file
    let mut input_file = File::open(&args.input)?;
    let file_size = input_file.metadata()?.len();

    println!(
        "Reading snapshot file: {} ({} bytes)",
        args.input, file_size
    );

    // Read entire file into memory
    let mut buffer = Vec::new();
    input_file.read_to_end(&mut buffer)?;

    println!("Loaded {} bytes into memory", buffer.len());
    println!(
        "Processing and writing chunks of {} blocks each",
        args.blocks_per_chunk
    );

    // Parse and write blocks in chunks
    let mut index = 0;
    let mut current_chunk = Vec::new();
    let mut total_blocks = 0;
    let mut chunk_count = 0;
    let mut current_output_file: Option<File> = None;
    let mut chunk_first_slot = 0u64;

    while index < buffer.len() {
        match bincode::deserialize::<BlockInfo>(&buffer[index..]) {
            Ok(block) => {
                let size = bincode::serialized_size(&block)? as usize;
                let slot = block.metadata.slot;

                // If starting a new chunk, record the first slot
                if current_chunk.is_empty() {
                    chunk_first_slot = slot;
                }

                current_chunk.push(block);
                index += size;
                total_blocks += 1;

                // Write chunk when it reaches the target size
                if current_chunk.len() >= args.blocks_per_chunk {
                    let last_slot = current_chunk.last().unwrap().metadata.slot;
                    let output_path = format!(
                        "{}/snapshot-{}-{}",
                        args.output_dir, chunk_first_slot, last_slot
                    );

                    chunk_count += 1;
                    println!(
                        "Writing chunk {} ({} blocks, slots {}-{}) to {}",
                        chunk_count,
                        current_chunk.len(),
                        chunk_first_slot,
                        last_slot,
                        output_path
                    );

                    let mut output_file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&output_path)?;

                    for block in current_chunk.iter() {
                        let serialized = bincode::serialize(block)?;
                        output_file.write_all(&serialized)?;
                    }

                    output_file.flush()?;
                    current_chunk.clear();
                }

                if total_blocks % 100_000 == 0 {
                    println!("  Processed {} blocks total", total_blocks);
                }
            }
            Err(e) => {
                println!("Error deserializing at offset {}: {}", index, e);
                println!("Successfully parsed {} blocks before error", total_blocks);
                break;
            }
        }
    }

    // Write any remaining blocks
    if !current_chunk.is_empty() {
        let last_slot = current_chunk.last().unwrap().metadata.slot;
        let output_path = format!(
            "{}/snapshot-{}-{}",
            args.output_dir, chunk_first_slot, last_slot
        );

        chunk_count += 1;
        println!(
            "Writing final chunk {} ({} blocks, slots {}-{}) to {}",
            chunk_count,
            current_chunk.len(),
            chunk_first_slot,
            last_slot,
            output_path
        );

        let mut output_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&output_path)?;

        for block in current_chunk.iter() {
            let serialized = bincode::serialize(block)?;
            output_file.write_all(&serialized)?;
        }

        output_file.flush()?;
    }

    println!("\nDone! Created {} snapshot files", chunk_count);
    println!("Total blocks processed: {}", total_blocks);

    Ok(())
}
