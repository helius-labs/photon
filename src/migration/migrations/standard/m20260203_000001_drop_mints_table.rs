use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

/// Table identifier for the mints table (matches the old definition)
#[derive(Copy, Clone, Iden)]
enum Mints {
    Table,
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the mints table if it exists
        manager
            .drop_table(Table::drop().table(Mints::Table).if_exists().to_owned())
            .await?;

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // We don't recreate the table on down - it's permanently removed
        Ok(())
    }
}
