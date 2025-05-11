use crate::api::method::get_validity_proof::prover::structs::{CompressedProof, ProofABC};
use lazy_static::lazy_static;
use num_bigint::BigUint;
use std::str::FromStr;

lazy_static! {
    static ref FIELD_SIZE: BigUint = BigUint::from_str(
        "21888242871839275222246405745257275088548364400416034343698204186575808495616"
    )
    .unwrap();
}

fn y_element_is_positive_g1(y_element: &BigUint) -> bool {
    y_element <= &(FIELD_SIZE.clone() - y_element)
}

fn y_element_is_positive_g2(y_element1: &BigUint, y_element2: &BigUint) -> bool {
    let field_midpoint = FIELD_SIZE.clone() / 2u32;

    if y_element1 < &field_midpoint {
        true
    } else if y_element1 > &field_midpoint {
        false
    } else {
        y_element2 < &field_midpoint
    }
}

fn add_bitmask_to_byte(mut byte: u8, y_is_positive: bool) -> u8 {
    if !y_is_positive {
        byte |= 1 << 7;
    }
    byte
}

pub fn negate_and_compress_proof(proof: ProofABC) -> CompressedProof {
    let proof_a = &proof.a;
    let proof_b = &proof.b;
    let proof_c = &proof.c;

    let a_x_element = &mut proof_a[0..32].to_vec();
    let a_y_element = BigUint::from_bytes_be(&proof_a[32..64]);

    let proof_a_is_positive = !y_element_is_positive_g1(&a_y_element);
    a_x_element[0] = add_bitmask_to_byte(a_x_element[0], proof_a_is_positive);

    let b_x_element = &mut proof_b[0..64].to_vec();
    let b_y_element = &proof_b[64..128];
    let b_y1_element = BigUint::from_bytes_be(&b_y_element[0..32]);
    let b_y2_element = BigUint::from_bytes_be(&b_y_element[32..64]);

    let proof_b_is_positive = y_element_is_positive_g2(&b_y1_element, &b_y2_element);
    b_x_element[0] = add_bitmask_to_byte(b_x_element[0], proof_b_is_positive);

    let c_x_element = &mut proof_c[0..32].to_vec();
    let c_y_element = BigUint::from_bytes_be(&proof_c[32..64]);

    let proof_c_is_positive = y_element_is_positive_g1(&c_y_element);
    c_x_element[0] = add_bitmask_to_byte(c_x_element[0], proof_c_is_positive);

    CompressedProof {
        a: a_x_element.clone(),
        b: b_x_element.clone(),
        c: c_x_element.clone(),
    }
}
