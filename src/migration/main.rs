use photon_indexer::migration::{MigractorWithCustomMigrations, Migrator};
use sea_orm_migration::prelude::*;

#[async_std::main]
async fn main() {
    let custom_indexes_enabled = std::env::var("ENABLE_CUSTOM_INDEXES")
        .unwrap_or("false".to_string())
        .to_lowercase()
        == "true";
    if custom_indexes_enabled {
        cli::run_cli(MigractorWithCustomMigrations).await;
    } else {
        cli::run_cli(Migrator).await;
    }
}
