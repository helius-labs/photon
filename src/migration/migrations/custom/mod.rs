use sea_orm_migration::MigrationTrait;

pub mod custom20252201_000001_init;

pub fn get_custom_migrations() -> Vec<Box<dyn MigrationTrait>> {
    vec![Box::new(custom20252201_000001_init::Migration)]
}
