// Temporary script to generate correct EMPTY_SUBTREES for AddressV2 trees
use light_hasher::Poseidon;
use light_indexed_array::{HIGHEST_ADDRESS_PLUS_ONE, array::IndexedArray};
use num_bigint::BigUint;
use num_traits::{Num, Zero};

fn main() {
    let init_next_value = BigUint::from_str_radix(HIGHEST_ADDRESS_PLUS_ONE, 10).unwrap();
    let indexed_array = IndexedArray::<Poseidon, usize>::new(BigUint::zero(), init_next_value.clone());

    let element_0 = indexed_array.get(0).unwrap();
    let leaf_hash = element_0.hash::<Poseidon>(&init_next_value).unwrap();

    println!("pub const EMPTY_SUBTREES: [[u8; 32]; 40] = [");
    println!("    // Level 0: Leaf hash");
    print!("    {:?},\n", leaf_hash);

    // Compute each level by hashing with zero sibling
    let mut current = leaf_hash;
    for level in 0..39 {
        let zero = Poseidon::zero_bytes()[level];
        current = Poseidon::hashv(&[&current, &zero]).unwrap();
        println!("    // Level {}: hash(level_{}, ZERO_BYTES[{}])", level + 1, level, level);
        print!("    {:?},\n", current);
    }
    println!("];");

    // Verify the last one is the correct root
    const EXPECTED_ROOT: [u8; 32] = [
        28, 65, 107, 255, 208, 234, 51, 3, 131, 95, 62, 130, 202, 177, 176, 26, 216, 81, 64, 184, 200,
        25, 95, 124, 248, 129, 44, 109, 229, 146, 106, 76,
    ];

    println!("\n// Verification:");
    println!("// Expected root: {:?}", &EXPECTED_ROOT[..8]);
    println!("// Computed root: {:?}", &current[..8]);
    println!("// Match: {}", current == EXPECTED_ROOT);
}
