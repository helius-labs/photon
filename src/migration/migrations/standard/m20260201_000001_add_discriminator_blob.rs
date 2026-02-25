use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

/// Originally intended to change discriminator from REAL/Decimal to BLOB/BYTEA.
///
/// Reverted: Postgres DECIMAL(23,0) already handles u64 without precision loss.
/// SQLite REAL has precision loss for u64 > 2^53, but this is an inherent
/// sqlx/sea_orm limitation (Decimal maps to f64 on SQLite) that cannot be fixed
/// with a column type change alone. This is acceptable for dev/test use.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Ok(())
    }
}
