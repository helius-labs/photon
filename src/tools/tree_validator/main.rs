use clap::Parser;
use log::info;
use photon_indexer::{
    common::{
        setup_logging, setup_pg_connection, typedefs::serializable_pubkey::SerializablePubkey,
        LoggingFormat,
    },
    ingester::persist::persisted_indexed_merkle_tree::validate_tree,
};
use solana_pubkey::Pubkey;
use std::str::FromStr;
use tokio;

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    db_url: String,
    #[arg(short, long)]
    tree_address: String,
}

#[tokio::main]
async fn main() {
    setup_logging(LoggingFormat::Standard);

    let args = Args::parse();
    let max_connections = 1;
    let db = setup_pg_connection(&args.db_url, max_connections).await;
    let tree_address = SerializablePubkey::from(Pubkey::from_str(&args.tree_address).unwrap());
    info!("Validating tree {:?}", tree_address);

    validate_tree(&db, tree_address).await;
}
