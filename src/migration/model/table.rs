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
    Account,
    Owner,
    Mint,
    Amount,
    Delegate,
    Frozen,
    IsNative,
    DelegatedAmount,
    CloseAuthority,
    // Duplicate to Spent in UTXOs. Added to simplify the handling of concurrent insertions and deletions
    // of token utxos.
    Spent,
    SlotUpdated,
    CreatedAt,
}

#[derive(Copy, Clone, Iden)]
pub enum Blocks {
    Table,
    Id,
    Slot,
    ParentSlot,
    Blockhash,
    ParentBlockhash,
    BlockHeight,
    BlockTime,
}
