use clap::Parser;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::str::FromStr;

/// Find the next slot where the Light System Program was active
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// The slot to start searching from
    start_slot: u64,

    /// RPC URL
    #[arg(short, long, default_value = "https://api.devnet.solana.com")]
    rpc_url: String,

    /// Compression program ID
    #[arg(long, default_value = "compr6CUsB5m2jS4Y3831ztGSTnDpnKJTKS95d64XVq")]
    program_id: String,

    /// Maximum slots to scan forward
    #[arg(long, default_value = "10000")]
    max_scan: u64,
}

fn main() {
    let args = Args::parse();

    let client =
        RpcClient::new_with_commitment(args.rpc_url.clone(), CommitmentConfig::confirmed());

    let program_id = Pubkey::from_str(&args.program_id).expect("Invalid program ID");

    println!(
        "Searching for next active slot after {}...",
        args.start_slot
    );
    println!("Compression program: {}", program_id);
    println!("RPC URL: {}", args.rpc_url);

    // Search forward from start slot
    println!("\nSearching forward from slot {}...", args.start_slot);

    let current_slot = match client.get_slot() {
        Ok(slot) => slot,
        Err(e) => {
            eprintln!("Error getting current slot: {}", e);
            return;
        }
    };

    println!("Current slot: {}", current_slot);
    let search_range = std::cmp::min(args.max_scan, current_slot - args.start_slot);
    println!("Searching up to {} slots forward...", search_range);

    // Search in batches to be more efficient
    const BATCH_SIZE: u64 = 100;
    let mut found_slot = None;

    for batch_start in
        (args.start_slot..args.start_slot + search_range).step_by(BATCH_SIZE as usize)
    {
        let batch_end = std::cmp::min(batch_start + BATCH_SIZE, args.start_slot + search_range);
        print!("\rChecking slots {} - {}...", batch_start, batch_end);

        for slot in batch_start..batch_end {
            match client.get_block(slot) {
                Ok(block) => {
                    // Check if block contains compression program
                    let has_compression = block.transactions.iter().any(|tx| {
                        if let Some(_meta) = &tx.meta {
                            if let Some(tx) = &tx.transaction.decode() {
                                match &tx.message {
                                    solana_sdk::message::VersionedMessage::Legacy(msg) => {
                                        msg.account_keys.contains(&program_id)
                                    }
                                    solana_sdk::message::VersionedMessage::V0(msg) => {
                                        msg.account_keys.contains(&program_id)
                                    }
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    });

                    if has_compression {
                        found_slot = Some(slot);
                        println!("\n\nFound compression activity at slot: {}", slot);
                        println!("Gap size: {} slots", slot - args.start_slot);
                        break;
                    }
                }
                Err(_) => {
                    // Slot might be skipped, continue
                }
            }
        }

        if found_slot.is_some() {
            break;
        }
    }

    if let Some(slot) = found_slot {
        println!("\n✅ Next active slot: {}", slot);
        println!(
            "You can restart the indexer with: --start-slot {}",
            slot - 1
        );
    } else {
        println!(
            "\n\n❌ No compression activity found in the next {} slots",
            search_range
        );
        println!("You may need to:");
        println!("1. Increase --max-scan (current: {})", args.max_scan);
        println!("2. Use --start-slot latest to skip to current");
        println!(
            "3. Check if the compression program ID is correct: {}",
            program_id
        );
    }
}
