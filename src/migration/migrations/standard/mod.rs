use sea_orm_migration::MigrationTrait;

pub mod m20220101_000001_init;
pub mod m20240623_000002_init;
pub mod m20240624_000003_init;
pub mod m20240807_000004_init;
pub mod m20240914_000005_init;
pub mod m20241008_000006_init;
pub mod m20250206_000007_init;
pub mod m20250314_000008_init;

pub fn get_standard_migrations() -> Vec<Box<dyn MigrationTrait>> {
    vec![
        Box::new(m20220101_000001_init::Migration),
        Box::new(m20240623_000002_init::Migration),
        Box::new(m20240624_000003_init::Migration),
        Box::new(m20240807_000004_init::Migration),
        Box::new(m20240914_000005_init::Migration),
        Box::new(m20241008_000006_init::Migration),
        Box::new(m20250206_000007_init::Migration),
        Box::new(m20250314_000008_init::Migration),
    ]
}
