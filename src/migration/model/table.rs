use sea_orm_migration::prelude::*;

#[derive(Copy, Clone, Iden)]
pub enum StateTrees {
    Table,
    Id,
    Tree,
    NodeIdx,
    LeafIdx,
    Level,
    Hash,
    Seq,
    SlotUpdated,
}

#[derive(Copy, Clone, Iden)]
pub enum UTXOs {
    #[iden = "utxos"]
    Table,
    Id,
    Hash,
    Data,
    Account,
    Owner,
    Lamports,
    Tree,
    Spent,
    Seq,
    SlotUpdated,
    CreatedAt,
}

#[derive(Copy, Clone, Iden)]
pub enum TokenOwners {
    Table,
    Id,
    Hash,
    Owner,
    Mint,
    Amount,
    Delegate,
    Frozen,
    IsNative,
    DelegatedAmount,
    CloseAuthority,
}
