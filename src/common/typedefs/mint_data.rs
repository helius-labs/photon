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

/// Discriminator for compressed mints (1 in little endian, but stored as big endian [0,0,0,0,0,0,0,1])
pub const COMPRESSED_MINT_DISCRIMINATOR: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 1];

impl MintData {
    /// Parse mint data from compressed account data.
    ///
    /// Layout:
    /// - BaseMint (82 bytes):
    ///   - mint_authority COption (4 + 32 = 36 bytes)
    ///   - supply (8 bytes)
    ///   - decimals (1 byte)
    ///   - is_initialized (1 byte)
    ///   - freeze_authority COption (4 + 32 = 36 bytes)
    /// - MintMetadata (67 bytes):
    ///   - version (1 byte)
    ///   - mint_decompressed (1 byte)
    ///   - mint (32 bytes)
    ///   - mint_signer (32 bytes)
    ///   - bump (1 byte)
    /// - Reserved (16 bytes)
    /// - account_type (1 byte)
    /// - CompressionInfo (variable)
    /// - Extensions (optional, variable)
    pub fn parse(data: &[u8]) -> Result<Self, std::io::Error> {
        // Minimum size: BaseMint (82) + MintMetadata (67) + Reserved (16) + account_type (1) = 166
        const MIN_SIZE: usize = 166;
        if data.len() < MIN_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Mint data too short: expected at least {} bytes, got {}",
                    MIN_SIZE,
                    data.len()
                ),
            ));
        }

        let mut offset = 0;

        // Parse BaseMint (82 bytes)
        // mint_authority COption
        let mint_authority_discriminator =
            u32::from_le_bytes(data[offset..offset + 4].try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid mint authority discriminator",
                )
            })?);
        offset += 4;
        let mint_authority = if mint_authority_discriminator == 1 {
            let pubkey_bytes: [u8; 32] = data[offset..offset + 32].try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid mint authority pubkey",
                )
            })?;
            Some(SerializablePubkey::from(pubkey_bytes))
        } else {
            None
        };
        offset += 32;

        // supply (8 bytes)
        let supply =
            u64::from_le_bytes(data[offset..offset + 8].try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid supply")
            })?);
        offset += 8;

        // decimals (1 byte)
        let decimals = data[offset];
        offset += 1;

        // is_initialized (1 byte) - skip
        offset += 1;

        // freeze_authority COption
        let freeze_authority_discriminator =
            u32::from_le_bytes(data[offset..offset + 4].try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid freeze authority discriminator",
                )
            })?);
        offset += 4;
        let freeze_authority = if freeze_authority_discriminator == 1 {
            let pubkey_bytes: [u8; 32] = data[offset..offset + 32].try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid freeze authority pubkey",
                )
            })?;
            Some(SerializablePubkey::from(pubkey_bytes))
        } else {
            None
        };
        offset += 32;

        // Parse MintMetadata (67 bytes)
        // version (1 byte)
        let version = data[offset];
        offset += 1;

        // mint_decompressed (1 byte)
        let mint_decompressed = data[offset] != 0;
        offset += 1;

        // mint (32 bytes) - this is the mint PDA
        let mint_pda_bytes: [u8; 32] = data[offset..offset + 32].try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid mint PDA")
        })?;
        let mint_pda = SerializablePubkey::from(mint_pda_bytes);
        offset += 32;

        // mint_signer (32 bytes)
        let mint_signer_bytes: [u8; 32] = data[offset..offset + 32].try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid mint signer")
        })?;
        let mint_signer = SerializablePubkey::from(mint_signer_bytes);
        offset += 32;

        // bump (1 byte) - skip
        offset += 1;

        // Reserved (16 bytes) - skip
        offset += 16;

        // account_type (1 byte) - skip
        offset += 1;

        // CompressionInfo - skip (we don't need it for the response)
        // The remaining bytes after CompressionInfo would be extensions
        // For now, we'll include any remaining data as extensions

        // Skip CompressionInfo (variable size, but we can detect extensions by parsing)
        // CompressionInfo is: rent_epoch (8) + rent_recipient (Option<Pubkey>) + lamports (8) = ~49 bytes min
        // For simplicity, we'll store the remaining bytes as extensions if there are any significant bytes
        let extensions = if offset < data.len() {
            // Store remaining data as extensions (includes CompressionInfo + actual extensions)
            Some(Base64String(data[offset..].to_vec()))
        } else {
            None
        };

        Ok(MintData {
            mint_pda,
            mint_signer,
            mint_authority,
            freeze_authority,
            supply: UnsignedInteger(supply),
            decimals,
            version,
            mint_decompressed,
            extensions,
        })
    }
}
