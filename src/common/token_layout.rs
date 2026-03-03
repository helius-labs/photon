/// Shared constants for Light Token account byte layouts.
///
/// SPL token account base length.
pub const SPL_TOKEN_ACCOUNT_BASE_LEN: usize = 165;
/// Account type marker byte offset in token/mint account data.
pub const TOKEN_ACCOUNT_TYPE_OFFSET: usize = SPL_TOKEN_ACCOUNT_BASE_LEN;
/// 1-indexed equivalent for SQL substring functions.
pub const TOKEN_ACCOUNT_TYPE_OFFSET_SQL: usize = TOKEN_ACCOUNT_TYPE_OFFSET + 1;

/// AccountType::Mint discriminator value.
pub const ACCOUNT_TYPE_MINT: u8 = 1;

/// Compressed mint PDA offset range `[84..116]` (32 bytes).
pub const COMPRESSED_MINT_PDA_OFFSET: usize = 84;
pub const COMPRESSED_MINT_PDA_LEN: usize = 32;
pub const COMPRESSED_MINT_PDA_END: usize = COMPRESSED_MINT_PDA_OFFSET + COMPRESSED_MINT_PDA_LEN;
/// 1-indexed equivalent for SQL substring functions.
pub const COMPRESSED_MINT_PDA_OFFSET_SQL: usize = COMPRESSED_MINT_PDA_OFFSET + 1;
