use super::*;
use yellowstone_grpc_proto::solana::storage::confirmed_block::{
    CompiledInstruction, Message, Transaction,
};

#[test]
fn missing_block_metadata_returns_an_error() {
    assert!(matches!(
        parse_block(SubscribeUpdateBlock::default()),
        Err(IngesterError::ParserError(_))
    ));
}

#[test]
fn incomplete_blocks_are_rejected_before_advancing_the_cursor() {
    let block = SubscribeUpdateBlock {
        slot: 11,
        executed_transaction_count: 1,
        ..Default::default()
    };
    assert!(
        matches!(parse_block(block), Err(IngesterError::ParserError(message)) if message.contains("Incomplete gRPC block 11"))
    );
}

#[test]
fn invalid_account_indexes_do_not_panic() {
    let transaction = SubscribeUpdateTransactionInfo {
        signature: vec![0; 64],
        meta: Some(Default::default()),
        transaction: Some(Transaction {
            message: Some(Message {
                account_keys: vec![vec![0; 32]],
                instructions: vec![CompiledInstruction {
                    program_id_index: 7,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert!(
        matches!(parse_transaction(transaction), Err(IngesterError::ParserError(message)) if message.contains("out of bounds"))
    );
}

#[test]
fn malformed_transaction_metadata_returns_an_error() {
    assert!(matches!(
        parse_transaction(SubscribeUpdateTransactionInfo::default()),
        Err(IngesterError::ParserError(_))
    ));
}
