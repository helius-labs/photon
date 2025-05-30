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
    InOutputQueue,
    NullifierQueueIndex,
    NullifiedInTree,
    TreeType,
    Queue,
    Nullifier,
    TxHash,
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
    Tlv,
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
    Error,
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

#[derive(Copy, Clone, Iden)]
pub enum StateTreeHistories {
    Table,
    Tree,
    Seq,
    TransactionSignature,
    LeafIdx,
}

#[derive(Copy, Clone, Iden)]
pub enum AddressQueues {
    Table,
    Tree,
    Address,
    QueueIndex,
}
