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
pub enum Accounts {
    Table,
    Id,
    Hash,
    Address,
    Data,
    Discriminator,
    Owner,
    Lamports,
    Tree,
    Spent,
    Seq,
    SlotUpdated,
    CreatedAt,
}

#[derive(Copy, Clone, Iden)]
pub enum TokenAccounts {
    Table,
    Id,
    Hash,
    Address,
    Owner,
    Mint,
    Amount,
    Delegate,
    Frozen,
    IsNative,
    DelegatedAmount,
    CloseAuthority,
    // Duplicate to Spent in accounts. Added to simplify the handling of concurrent insertions and deletions
    // of token accounts.
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
