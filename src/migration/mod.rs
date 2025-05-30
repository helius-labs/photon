use migrations::{custom::get_custom_migrations, standard::get_standard_migrations};

pub use sea_orm_migration::prelude::*;

mod migrations;
mod model;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        get_standard_migrations()
    }
}

pub struct MigractorWithCustomMigrations;

#[async_trait::async_trait]
impl MigratorTrait for MigractorWithCustomMigrations {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        get_standard_migrations()
            .into_iter()
            .chain(get_custom_migrations())
            .collect()
    }
}
