pub use sea_orm_migration::prelude::*;

mod m20220101_000001_init;
mod m20240623_000002_init;
mod m20240624_000003_init;
mod m20240807_000004_init;
mod m20240914_000005_init;
mod model;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20220101_000001_init::Migration),
            Box::new(m20240623_000002_init::Migration),
            Box::new(m20240624_000003_init::Migration),
            Box::new(m20240807_000004_init::Migration),
            Box::new(m20240914_000005_init::Migration),
        ]
    }
}
