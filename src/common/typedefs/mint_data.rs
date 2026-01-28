use light_token_interface::state::{Mint, BASE_MINT_ACCOUNT_SIZE};
use light_zero_copy::traits::ZeroCopyAt;
use serde::Serialize;
use utoipa::ToSchema;

use super::{
    bs64_string::Base64String, serializable_pubkey::SerializablePubkey,
    unsigned_integer::UnsignedInteger,
};

/// Compressed mint account data
#[derive(Debug, PartialEq, Eq, Clone, ToSchema, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MintData {
    /// The mint PDA (Solana account address when decompressed)
    pub mint_pda: SerializablePubkey,
    /// The mint signer used to derive the mint PDA
    pub mint_signer: SerializablePubkey,
    /// Optional authority used to mint new tokens
    pub mint_authority: Option<SerializablePubkey>,
    /// Optional authority to freeze token accounts
    pub freeze_authority: Option<SerializablePubkey>,
    /// Total supply of tokens
    pub supply: UnsignedInteger,
    /// Number of base 10 digits to the right of the decimal place
    pub decimals: u8,
    /// Version for upgradability
    pub version: u8,
    /// Whether the compressed mint has been decompressed to a Mint Solana account
    pub mint_decompressed: bool,
    /// Serialized extensions data (TokenMetadata, etc.)
    pub extensions: Option<Base64String>,
}

impl MintData {
    /// Parse mint data from compressed account data using light-token-interface zero-copy parser.
    pub fn parse(data: &[u8]) -> Result<Self, std::io::Error> {
        let (zmint, _) = Mint::zero_copy_at(data).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse mint data: {:?}", e),
            )
        })?;

        let extensions = if zmint.extensions.is_some() {
            Some(Base64String(
                data[BASE_MINT_ACCOUNT_SIZE as usize..].to_vec(),
            ))
        } else {
            None
        };

        Ok(MintData {
            mint_pda: SerializablePubkey::from(zmint.metadata.mint.to_bytes()),
            mint_signer: SerializablePubkey::from(zmint.metadata.mint_signer),
            mint_authority: zmint
                .mint_authority()
                .map(|p| SerializablePubkey::from(p.to_bytes())),
            freeze_authority: zmint
                .freeze_authority()
                .map(|p| SerializablePubkey::from(p.to_bytes())),
            supply: UnsignedInteger(u64::from(zmint.supply)),
            decimals: zmint.decimals,
            version: zmint.metadata.version,
            mint_decompressed: zmint.metadata.mint_decompressed != 0,
            extensions,
        })
    }
}
