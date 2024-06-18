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
}

#[derive(Copy, Clone, Iden)]
pub enum Accounts {
    Table,
    Hash,
    Address,
    Data,
    DataHash,
    Owner,
    Tree,
    LeafIndex,
    Spent,
    PrevSpent,
    Seq,
    SlotCreated,
}

#[derive(Copy, Clone, Iden)]
pub enum TokenAccounts {
    Table,
    Hash,
    Owner,
    Mint,
    Delegate,
    State,
    Spent,
    PrevSpent,
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
    UsesCompression,
}

#[derive(Copy, Clone, Iden)]
pub enum AccountTransactions {
    Table,
    Hash,
    Signature,
}

#[derive(Copy, Clone, Iden)]
pub enum OwnerBalances {
    Table,
    Owner,
}

#[derive(Copy, Clone, Iden)]
pub enum TokenOwnerBalances {
    Table,
    Owner,
    Mint,
}

#[derive(Copy, Clone, Iden)]
pub enum IndexedTrees {
    Table,
    Tree,
    LeafIndex,
    Value,
    NextIndex,
    NextValue,
    Seq,
}
