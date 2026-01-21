//! `SeaORM` Entity for compressed mints

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "mints")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub hash: Vec<u8>,
    pub address: Vec<u8>,
    pub mint_pda: Vec<u8>,
    pub mint_signer: Vec<u8>,
    pub mint_authority: Option<Vec<u8>>,
    pub freeze_authority: Option<Vec<u8>>,
    pub supply: i64,
    pub decimals: i16,
    pub version: i16,
    pub mint_decompressed: bool,
    pub extensions: Option<Vec<u8>>,
    pub spent: bool,
    pub prev_spent: Option<bool>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::accounts::Entity",
        from = "Column::Hash",
        to = "super::accounts::Column::Hash",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Accounts,
}

impl Related<super::accounts::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Accounts.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
