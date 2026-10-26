use photon_indexer::common::typedefs::{
    hash::Hash, serializable_pubkey::SerializablePubkey,
    serializable_signature::SerializableSignature,
};
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde_json::json;
use solana_signature::Signature;

fn assert_legacy_wire_bytes(hash_bytes: [u8; 32], signature_bytes: [u8; 64]) {
    let hash = Hash::from(hash_bytes);
    let pubkey = SerializablePubkey::from(hash_bytes);
    let signature = SerializableSignature(Signature::from(signature_bytes));
    let hash_base58 = bs58::encode(hash_bytes).into_string();
    let signature_base58 = bs58::encode(signature_bytes).into_string();
    assert_eq!(
        (
            serde_json::to_value(&hash).unwrap(),
            hash.to_base58(),
            hash.to_string(),
            serde_json::to_value(&pubkey).unwrap(),
            serde_json::to_value(&signature).unwrap(),
            serde_json::to_string(&hash).unwrap(),
            serde_json::to_string(&pubkey).unwrap(),
            serde_json::to_string(&signature).unwrap(),
        ),
        (
            json!(hash_base58),
            hash_base58.clone(),
            hash_base58.clone(),
            json!(hash_base58),
            json!(signature_base58),
            serde_json::to_string(&hash_base58).unwrap(),
            serde_json::to_string(&hash_base58).unwrap(),
            serde_json::to_string(&signature_base58).unwrap(),
        ),
        "Fixed-size encoders must retain exact base58 and JSON representations"
    );
}

#[test]
fn base58_serialization_matches_legacy_random_and_boundary_values() {
    assert_legacy_wire_bytes([0; 32], [0; 64]);
    assert_legacy_wire_bytes([u8::MAX; 32], [u8::MAX; 64]);
    let mut random = StdRng::seed_from_u64(0x5345524445423538);
    for _ in 0..4096 {
        let mut hash_bytes = [0; 32];
        let mut signature_bytes = [0; 64];
        random.fill_bytes(&mut hash_bytes);
        random.fill_bytes(&mut signature_bytes);
        assert_legacy_wire_bytes(hash_bytes, signature_bytes);
    }
}

#[test]
fn base58_serialization_retains_every_leading_zero_length() {
    for zero_bytes in 0..=64 {
        let mut hash_bytes = [1; 32];
        let mut signature_bytes = [1; 64];
        hash_bytes.get_mut(..zero_bytes.min(32)).unwrap().fill(0);
        signature_bytes.get_mut(..zero_bytes).unwrap().fill(0);
        assert_legacy_wire_bytes(hash_bytes, signature_bytes);
    }
}
