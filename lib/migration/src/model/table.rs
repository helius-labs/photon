use sea_orm_migration::prelude::*;

#[derive(Copy, Clone, DeriveIden)]
pub enum StateTrees {
    Table,
    Id,
    Tree,
    NodeIdx,
    LeafIdx,
    Hash,
    Seq,
    SlotUpdated,
    CreatedAt,
}

#[derive(Copy, Clone, DeriveIden)]
pub enum UTXOs {
    #[sea_orm(iden = "utxos")]
    Table,
    Id,
    Hash,
    Data,
    Account,
    Tree,
    LeafIdx,
    Spent,
    SlotUpdated,
    CreatedAt,
}
