#[cfg(test)]
mod tests {
    use crate::dao::generated::indexed_trees;
    use crate::ingester::persist::compute_parent_hash;
    use crate::ingester::persist::indexed_merkle_tree::{
        get_zeroeth_exclusion_range, ADDRESS_TREE_INIT_ROOT_40, HIGHEST_ADDRESS_PLUS_ONE,
    };
    use crate::ingester::persist::persisted_state_tree::ZERO_BYTES;
    use ark_bn254::Fr;
    use light_poseidon::{Poseidon, PoseidonBytesHasher};

    /// Test computing the initial root for an AddressV2 tree (height 40)
    /// using 2-field hash: H(value, next_value)
    #[test]
    fn test_address_tree_init_root_2_field_hash() {
        // Element 0: value=0, next_index=1, next_value=HIGHEST_ADDRESS
        let zeroeth_element = get_zeroeth_exclusion_range(vec![0; 32]);

        println!("Zeroeth element:");
        println!("  value: {:?}", &zeroeth_element.value);
        println!("  next_index: {}", zeroeth_element.next_index);
        println!("  next_value: {:?}", &zeroeth_element.next_value[..8]);

        // Compute hash using 2 fields: H(value, next_value)
        let mut poseidon = Poseidon::<Fr>::new_circom(2).unwrap();
        let leaf_hash = poseidon
            .hash_bytes_be(&[&zeroeth_element.value, &zeroeth_element.next_value])
            .unwrap();

        println!("\n2-field hash H(value, next_value):");
        println!("  Leaf hash: {:?}", &leaf_hash[..8]);

        // Compute root by hashing up the tree (height 40 = 40 hash operations!)
        let mut current_hash = leaf_hash.to_vec();
        for i in 0..40 {
            let zero_hash = ZERO_BYTES[i];
            current_hash = compute_parent_hash(current_hash, zero_hash.to_vec()).unwrap();
        }

        println!("\nComputed root (2-field):");
        println!("  {:?}", &current_hash[..8]);
        println!("\nExpected root (ADDRESS_TREE_INIT_ROOT_40):");
        println!("  {:?}", &ADDRESS_TREE_INIT_ROOT_40[..8]);

        // Check if it matches the hardcoded constant
        if current_hash.as_slice() == ADDRESS_TREE_INIT_ROOT_40 {
            println!("\n✅ 2-field hash produces CORRECT root!");
        } else {
            println!("\n❌ 2-field hash produces WRONG root!");
        }

        println!("\nFull computed root: {:?}", current_hash);
        println!("Full expected root: {:?}", ADDRESS_TREE_INIT_ROOT_40);
    }

    /// Test computing the initial root for an AddressV2 tree (height 40)
    /// using 3-field hash: H(value, next_index, next_value)
    #[test]
    fn test_address_tree_init_root_3_field_hash() {
        // Element 0: value=0, next_index=1, next_value=HIGHEST_ADDRESS
        let zeroeth_element = get_zeroeth_exclusion_range(vec![0; 32]);

        println!("Zeroeth element:");
        println!("  value: {:?}", &zeroeth_element.value);
        println!("  next_index: {}", zeroeth_element.next_index);
        println!("  next_value: {:?}", &zeroeth_element.next_value[..8]);

        // Compute hash using 3 fields: H(value, next_index, next_value)
        let mut poseidon = Poseidon::<Fr>::new_circom(3).unwrap();
        let mut next_index_bytes = vec![0u8; 32];
        let index_be = zeroeth_element.next_index.to_be_bytes();
        next_index_bytes[24..32].copy_from_slice(&index_be);

        let leaf_hash = poseidon
            .hash_bytes_be(&[
                &zeroeth_element.value,
                &next_index_bytes,
                &zeroeth_element.next_value,
            ])
            .unwrap();

        println!("\n3-field hash H(value, next_index, next_value):");
        println!("  next_index_bytes: {:?}", &next_index_bytes[24..32]);
        println!("  Leaf hash: {:?}", &leaf_hash[..8]);

        // Compute root by hashing up the tree (height 40 = 40 hash operations!)
        let mut current_hash = leaf_hash.to_vec();
        for i in 0..40 {
            let zero_hash = ZERO_BYTES[i];
            current_hash = compute_parent_hash(current_hash, zero_hash.to_vec()).unwrap();
        }

        println!("\nComputed root (3-field):");
        println!("  {:?}", &current_hash[..8]);
        println!("\nExpected root (ADDRESS_TREE_INIT_ROOT_40):");
        println!("  {:?}", &ADDRESS_TREE_INIT_ROOT_40[..8]);

        // Check if it matches the hardcoded constant
        if current_hash.as_slice() == ADDRESS_TREE_INIT_ROOT_40 {
            println!("\n✅ 3-field hash produces CORRECT root!");
        } else {
            println!("\n❌ 3-field hash produces WRONG root!");
        }

        println!("\nFull computed root: {:?}", current_hash);
        println!("Full expected root: {:?}", ADDRESS_TREE_INIT_ROOT_40);
    }

    /// Test with next_index=0 vs next_index=1 for 2-field hash
    #[test]
    fn test_address_tree_init_next_index_variants() {
        println!("Testing different next_index values with 2-field hash:\n");

        for next_idx in [0, 1] {
            let element = indexed_trees::Model {
                tree: vec![0; 32],
                leaf_index: 0,
                value: vec![0; 32],
                next_index: next_idx,
                next_value: vec![0]
                    .into_iter()
                    .chain(HIGHEST_ADDRESS_PLUS_ONE.to_bytes_be())
                    .collect(),
                seq: Some(0),
            };

            // 2-field hash (doesn't use next_index in hash, but affects the model)
            let mut poseidon = Poseidon::<Fr>::new_circom(2).unwrap();
            let leaf_hash = poseidon
                .hash_bytes_be(&[&element.value, &element.next_value])
                .unwrap();

            // Compute root (height 40 = 40 hash operations!)
            let mut current_hash = leaf_hash.to_vec();
            for i in 0..40 {
                current_hash = compute_parent_hash(current_hash, ZERO_BYTES[i].to_vec()).unwrap();
            }

            println!("next_index={}: root={:?}", next_idx, &current_hash[..8]);

            if current_hash.as_slice() == ADDRESS_TREE_INIT_ROOT_40 {
                println!("  ✅ MATCHES expected root!\n");
            } else {
                println!("  ❌ Does NOT match expected root\n");
            }
        }

        println!("Expected root: {:?}", &ADDRESS_TREE_INIT_ROOT_40[..8]);
    }

    /// CRITICAL TEST: User's theory - AddressV2 uses next_index=1 + 2-field hash
    #[test]
    fn test_address_v2_theory_next_index_1_with_2_field_hash() {
        println!("=== Testing: next_index=1 + 2-field hash H(value, next_value) ===\n");

        let zeroeth_element = get_zeroeth_exclusion_range(vec![0; 32]);

        println!("Element configuration:");
        println!("  value: {:?}", &zeroeth_element.value[..8]);
        println!(
            "  next_index: {} (stored but NOT used in hash)",
            zeroeth_element.next_index
        );
        println!("  next_value: {:?}\n", &zeroeth_element.next_value[..8]);

        // Use 2-field hash: H(value, next_value)
        // next_index is NOT included in the hash
        let mut poseidon = Poseidon::<Fr>::new_circom(2).unwrap();
        let leaf_hash = poseidon
            .hash_bytes_be(&[&zeroeth_element.value, &zeroeth_element.next_value])
            .unwrap();

        println!("2-field hash H(value, next_value):");
        println!("  Leaf hash: {:?}", &leaf_hash[..8]);

        // Compute root by hashing up the tree (height 40 = 40 hash operations!)
        let mut current_hash = leaf_hash.to_vec();
        for i in 0..40 {
            current_hash = compute_parent_hash(current_hash, ZERO_BYTES[i].to_vec()).unwrap();
        }

        println!("\nComputed root:");
        println!("  {:?}", &current_hash[..8]);
        println!("\nExpected root (ADDRESS_TREE_INIT_ROOT_40):");
        println!("  {:?}", &ADDRESS_TREE_INIT_ROOT_40[..8]);

        if current_hash.as_slice() == ADDRESS_TREE_INIT_ROOT_40 {
            println!("\n🎉 ✅ PERFECT MATCH! User theory is CORRECT!");
            println!(
                "AddressV2 uses: next_index=1 (for proofs) + 2-field hash H(value, next_value)"
            );
        } else {
            println!("\n❌ Does NOT match - theory incorrect");
        }

        println!("\nFull computed root: {:?}", current_hash);
        println!("Full expected root: {:?}", ADDRESS_TREE_INIT_ROOT_40);

        // Assert to make test pass/fail clearly
        assert_eq!(
            current_hash.as_slice(),
            ADDRESS_TREE_INIT_ROOT_40,
            "AddressV2 root should match with next_index=1 + 2-field hash"
        );
    }

    /// REMOVED TEST: This test was based on an incorrect hypothesis
    /// The tree does NOT initialize with both elements - it initializes with ONLY element 0
    /// See test_address_tree_init_correct_formula for the correct approach

    /// FINAL TEST: Verify the CORRECT formula - ONE element with 2-field hash
    #[test]
    fn test_address_tree_init_correct_formula() {
        println!("=== CORRECT: Single element with 2-field hash ===\n");

        // AddressV2 tree initializes with just ONE element (not two!)
        let element_0 = get_zeroeth_exclusion_range(vec![0; 32]);

        println!("Element 0:");
        println!("  value: {:?}", &element_0.value[..8]);
        println!(
            "  next_index: {} (stored, but NOT in hash)",
            element_0.next_index
        );
        println!("  next_value: {:?}", &element_0.next_value[..8]);

        // Hash element 0 using 2-field hash: H(value, next_value)
        let mut poseidon = Poseidon::<Fr>::new_circom(2).unwrap();
        let leaf_hash_0 = poseidon
            .hash_bytes_be(&[&element_0.value, &element_0.next_value])
            .unwrap();

        println!("\nLeaf 0 hash (2-field): {:?}", &leaf_hash_0[..8]);

        // Hash up the tree (just one leaf, rest are zeros)
        // Height 40 = 40 hash operations!
        let mut current_hash = leaf_hash_0.to_vec();
        for i in 0..40 {
            let zero_hash = ZERO_BYTES[i];
            current_hash = compute_parent_hash(current_hash, zero_hash.to_vec()).unwrap();
        }

        println!("\nComputed root (single element, 2-field hash):");
        println!("  {:?}", &current_hash[..8]);
        println!("\nExpected root (ADDRESS_TREE_INIT_ROOT_40):");
        println!("  {:?}", &ADDRESS_TREE_INIT_ROOT_40[..8]);

        if current_hash.as_slice() == ADDRESS_TREE_INIT_ROOT_40 {
            println!("\n✅ ✅ ✅ SUCCESS! This is the CORRECT formula!");
            println!("\nAddressV2 Tree Initialization:");
            println!("  1. Tree starts with ONE element (index 0)");
            println!("  2. Element 0: value=0, next_index=1, next_value=HIGHEST_ADDRESS");
            println!("  3. Hash uses 2-field: H(value, next_value)");
            println!("  4. next_index is stored but NOT included in hash");
            println!("  5. Root = hash single leaf up tree with zero siblings");
            println!("\nThis change was introduced in commit e208fa1eb");
            println!("'perf: indexed array remove next_index'");
        } else {
            println!("\n❌ Does NOT match");
        }

        println!("\nFull computed root: {:?}", current_hash);
        println!("Full expected root: {:?}", ADDRESS_TREE_INIT_ROOT_40);

        assert_eq!(
            current_hash.as_slice(),
            ADDRESS_TREE_INIT_ROOT_40,
            "AddressV2 root MUST match using single element with 2-field hash"
        );
    }
}
