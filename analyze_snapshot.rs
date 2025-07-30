use futures::StreamExt;
use photon_indexer::ingester::parser::parse_transaction;
use photon_indexer::snapshot::{load_block_stream_from_directory_adapter, DirectoryAdapter};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let snapshot_dir = std::env::args()
        .nth(1)
        .expect("Please provide snapshot directory");
    let target_tree = std::env::args().nth(2);

    println!("Analyzing snapshot in: {}", snapshot_dir);
    if let Some(ref tree) = target_tree {
        println!("Target tree filter: {}", tree);
    }

    let directory_adapter = Arc::new(DirectoryAdapter::from_local_directory(snapshot_dir));
    let block_stream = load_block_stream_from_directory_adapter(directory_adapter).await;

    let mut total_blocks = 0;
    let mut total_transactions = 0;
    let mut compression_transactions = 0;
    let mut tree_transactions = HashMap::new();
    let mut blocks_with_target_tree = 0;
    let mut target_tree_txs = 0;

    let target_tree_pubkey = target_tree
        .as_ref()
        .map(|s| s.parse::<solana_pubkey::Pubkey>().unwrap());

    let blocks: Vec<_> = block_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect();

    for block in &blocks {
        total_blocks += 1;
        let mut block_has_target = false;

        for tx in &block.transactions {
            total_transactions += 1;

            match parse_transaction(tx, block.metadata.slot, None) {
                Ok(state_update) => {
                    let has_compression = !state_update.indexed_merkle_tree_updates.is_empty()
                        || !state_update.batch_merkle_tree_events.is_empty()
                        || !state_update.out_accounts.is_empty()
                        || !state_update.leaf_nullifications.is_empty()
                        || !state_update.batch_new_addresses.is_empty();

                    if has_compression {
                        compression_transactions += 1;

                        // Collect tree statistics
                        for ((tree, _), _) in &state_update.indexed_merkle_tree_updates {
                            *tree_transactions.entry(tree.to_string()).or_insert(0) += 1;
                            if target_tree_pubkey.as_ref() == Some(tree) {
                                block_has_target = true;
                                target_tree_txs += 1;
                            }
                        }

                        for (tree_bytes, _) in &state_update.batch_merkle_tree_events {
                            let tree = solana_pubkey::Pubkey::from(*tree_bytes);
                            *tree_transactions.entry(tree.to_string()).or_insert(0) += 1;
                            if target_tree_pubkey.as_ref() == Some(&tree) {
                                block_has_target = true;
                                target_tree_txs += 1;
                            }
                        }

                        for account in &state_update.out_accounts {
                            let tree = &account.account.tree.0;
                            *tree_transactions.entry(tree.to_string()).or_insert(0) += 1;
                            if target_tree_pubkey.as_ref() == Some(tree) {
                                block_has_target = true;
                                target_tree_txs += 1;
                            }
                        }

                        for nullification in &state_update.leaf_nullifications {
                            *tree_transactions
                                .entry(nullification.tree.to_string())
                                .or_insert(0) += 1;
                            if target_tree_pubkey.as_ref() == Some(&nullification.tree) {
                                block_has_target = true;
                                target_tree_txs += 1;
                            }
                        }

                        for address in &state_update.batch_new_addresses {
                            *tree_transactions
                                .entry(address.tree.0.to_string())
                                .or_insert(0) += 1;
                            if target_tree_pubkey.as_ref() == Some(&address.tree.0) {
                                block_has_target = true;
                                target_tree_txs += 1;
                            }
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        if block_has_target {
            blocks_with_target_tree += 1;
        }
    }

    println!("\n=== Snapshot Analysis ===");
    println!("Total blocks: {}", total_blocks);
    println!("Total transactions: {}", total_transactions);
    println!(
        "Compression transactions: {} ({:.2}%)",
        compression_transactions,
        (compression_transactions as f64 / total_transactions as f64) * 100.0
    );

    println!("\n=== Tree Distribution ===");
    let mut tree_vec: Vec<_> = tree_transactions.into_iter().collect();
    tree_vec.sort_by(|a, b| b.1.cmp(&a.1));

    for (i, (tree, count)) in tree_vec.iter().enumerate() {
        if i < 10 || target_tree.as_ref().map(|t| t == tree).unwrap_or(false) {
            println!("{}: {} transactions", tree, count);
        }
    }

    if let Some(tree) = target_tree {
        println!("\n=== Target Tree Analysis ===");
        println!("Target tree: {}", tree);
        println!(
            "Blocks containing target tree: {} ({:.2}%)",
            blocks_with_target_tree,
            (blocks_with_target_tree as f64 / total_blocks as f64) * 100.0
        );
        println!("Transactions for target tree: {}", target_tree_txs);
        println!(
            "\nPotential optimization: Skip {:.2}% of blocks",
            ((total_blocks - blocks_with_target_tree) as f64 / total_blocks as f64) * 100.0
        );
    }

    Ok(())
}
