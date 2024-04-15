use sea_orm_migration::prelude::*;

#[derive(Copy, Clone, Iden)]
pub enum StateTrees {
    Table,
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
    Hash,
    Address,
    Data,
    DataHash,
    Discriminator,
    Owner,
    Tree,
    LeafIndex,
    Spent,
    Seq,
    SlotUpdated,
}

#[derive(Copy, Clone, Iden)]
pub enum TokenAccounts {
    Table,
    Hash,
    Owner,
    Mint,
    Delegate,
    Frozen,
    // Duplicate to Spent in accounts. Added to simplify the handling of concurrent insertions and deletions
    // of token accounts.
    Spent,
    SlotUpdated,
}

#[derive(Copy, Clone, Iden)]
pub enum Blocks {
    Table,
    Slot,
    ParentSlot,
    Blockhash,
    ParentBlockhash,
    BlockHeight,
    BlockTime,
}

#[derive(Copy, Clone, Iden)]
pub enum Transactions {
    Table,
    Signature,
    Slot,
}

#[derive(Copy, Clone, Iden)]
pub enum AccountTransactions {
    Table,
    Hash,
    Signature,
    Closure,
}
