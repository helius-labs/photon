use sea_orm_migration::MigrationTrait;

pub mod custom20250211_000002_solayer2;
pub mod custom20252201_000001_init;

pub fn get_custom_migrations() -> Vec<Box<dyn MigrationTrait>> {
    vec![
        Box::new(custom20252201_000001_init::Migration),
        Box::new(custom20250211_000002_solayer2::Migration),
    ]
}
