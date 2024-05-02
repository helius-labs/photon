use std::collections::HashMap;

use crate::{
    common::typedefs::serializable_pubkey::SerializablePubkey, dao::generated::state_trees,
};
use itertools::Itertools;
use sea_orm::{
    sea_query::Expr, ColumnTrait, Condition, DatabaseConnection, EntityTrait, QueryFilter,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::{
    super::error::PhotonApiError,
    utils::{Context, PAGE_LIMIT},
};
use crate::common::typedefs::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MerkleProofWithContext {
    pub proof: Vec<Hash>,
    pub leaf_index: u32,
    pub hash: Hash,
    pub merkle_tree: SerializablePubkey,
    pub root_seq: u64,
}

// We do not use generics to simplify documentation generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GetMultipleCompressedAccountProofsResponse {
    pub context: Context,
    pub value: Vec<MerkleProofWithContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HashList(pub Vec<Hash>);

pub async fn get_multiple_compressed_account_proofs(
    conn: &DatabaseConnection,
    request: HashList,
) -> Result<GetMultipleCompressedAccountProofsResponse, PhotonApiError> {
    let request = request.0;
    if request.len() > PAGE_LIMIT as usize {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many hashes requested {}. Maximum allowed: {}",
            request.len(),
            PAGE_LIMIT
        )));
    }
    let context = Context::extract(conn).await?;
    let proofs = get_multiple_compressed_account_proofs_helper(conn, request).await?;
    Ok(GetMultipleCompressedAccountProofsResponse {
        value: proofs,
        context,
    })
}

pub async fn get_multiple_compressed_account_proofs_helper(
    conn: &DatabaseConnection,
    hashes: Vec<Hash>,
) -> Result<Vec<MerkleProofWithContext>, PhotonApiError> {
    let leaf_nodes = state_trees::Entity::find()
        .filter(
            state_trees::Column::Hash
                .is_in(hashes.iter().map(|x| x.to_vec()).collect::<Vec<Vec<u8>>>())
                .and(state_trees::Column::Level.eq(0)),
        )
        .all(conn)
        .await?;

    if leaf_nodes.len() != hashes.len() {
        return Err(PhotonApiError::RecordNotFound(
            "Leaf nodes not found for some hashes".to_string(),
        ));
    }
    let leaf_hashes_to_model = leaf_nodes
        .iter()
        .map(|leaf_node| (leaf_node.hash.clone(), leaf_node.clone()))
        .collect::<HashMap<Vec<u8>, state_trees::Model>>();

    let leaf_hashes_to_required_nodes = leaf_nodes
        .iter()
        .map(|leaf_node| {
            let required_node_indices = get_proof_path(leaf_node.node_idx);
            (
                leaf_node.hash.clone(),
                (leaf_node.tree.clone(), required_node_indices),
            )
        })
        .collect::<HashMap<Vec<u8>, (Vec<u8>, Vec<i64>)>>();

    let all_required_node_indices = leaf_hashes_to_required_nodes
        .values()
        .flat_map(|(tree, indices)| indices.iter().map(move |&idx| (tree.clone(), idx)))
        .dedup()
        .collect::<Vec<(Vec<u8>, i64)>>();

    let mut condition = Condition::any();
    for (tree, node) in all_required_node_indices.clone() {
        let node_condition = Condition::all()
            .add(Expr::col(state_trees::Column::Tree).eq(tree))
            .add(Expr::col(state_trees::Column::NodeIdx).eq(node));

        // Add this condition to the overall condition with an OR
        condition = condition.add(node_condition);
    }

    let proof_nodes = state_trees::Entity::find()
        .filter(condition)
        .all(conn)
        .await?;

    let node_to_model = proof_nodes
        .iter()
        .map(|node| ((node.tree.clone(), node.node_idx), node.clone()))
        .collect::<HashMap<(Vec<u8>, i64), state_trees::Model>>();

    hashes
        .iter()
        .map(|hash| {
            let (tree, required_node_indices) = leaf_hashes_to_required_nodes
                .get(&hash.to_vec())
                .ok_or(PhotonApiError::RecordNotFound(format!(
                "Leaf node not found for hash {}",
                hash
            )))?;

            let proofs = required_node_indices
                .iter()
                .enumerate()
                .map(|(level, idx)| {
                    node_to_model
                        .get(&(tree.clone(), *idx))
                        .map(|node| {
                            Hash::try_from(node.hash.clone()).map_err(|_| {
                                PhotonApiError::UnexpectedError(
                                    "Failed to convert hash to bytes".to_string(),
                                )
                            })
                        })
                        .unwrap_or(Ok(Hash::from(ZERO_BYTES[level])))
                })
                .collect::<Result<Vec<Hash>, PhotonApiError>>()?;

            let root_seq = 0;
            // let root_seq = node_to_model
            //     .get(&(tree.clone(), 1))
            //     .ok_or({
            //         let tree_rep: SerializablePubkey = tree.clone().try_into()?;
            //         PhotonApiError::UnexpectedError(format!(
            //             "Missing root index for tree {}",
            //             tree_rep
            //         ))
            //     })?
            //     .seq as u64;

            let leaf_model =
                leaf_hashes_to_model
                    .get(&hash.to_vec())
                    .ok_or(PhotonApiError::RecordNotFound(format!(
                        "Leaf node not found for hash {}",
                        hash
                    )))?;

            Ok(MerkleProofWithContext {
                proof: proofs,
                leaf_index: leaf_model.leaf_idx.ok_or(PhotonApiError::RecordNotFound(
                    "Leaf index not found".to_string(),
                ))? as u32,
                hash: hash.clone(),
                merkle_tree: leaf_model.tree.clone().try_into()?,
                root_seq,
            })
        })
        .collect()
}

pub fn get_proof_path(index: i64) -> Vec<i64> {
    let mut indexes = vec![];
    let mut idx = index;
    while idx > 1 {
        if idx % 2 == 0 {
            indexes.push(idx + 1)
        } else {
            indexes.push(idx - 1)
        }
        idx >>= 1
    }
    indexes.push(1);
    indexes
}

// Autogenerated by the Light team. Do not edit.

pub const MAX_HEIGHT: usize = 32;
type ZeroBytes = [[u8; 32]; MAX_HEIGHT + 1];

pub const ZERO_BYTES: ZeroBytes = [
    [
        0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
        0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8,
    ],
    [
        32u8, 152u8, 245u8, 251u8, 158u8, 35u8, 158u8, 171u8, 60u8, 234u8, 195u8, 242u8, 123u8,
        129u8, 228u8, 129u8, 220u8, 49u8, 36u8, 213u8, 95u8, 254u8, 213u8, 35u8, 168u8, 57u8,
        238u8, 132u8, 70u8, 182u8, 72u8, 100u8,
    ],
    [
        16u8, 105u8, 103u8, 61u8, 205u8, 177u8, 34u8, 99u8, 223u8, 48u8, 26u8, 111u8, 245u8, 132u8,
        167u8, 236u8, 38u8, 26u8, 68u8, 203u8, 157u8, 198u8, 141u8, 240u8, 103u8, 164u8, 119u8,
        68u8, 96u8, 177u8, 241u8, 225u8,
    ],
    [
        24u8, 244u8, 51u8, 49u8, 83u8, 126u8, 226u8, 175u8, 46u8, 61u8, 117u8, 141u8, 80u8, 247u8,
        33u8, 6u8, 70u8, 124u8, 110u8, 234u8, 80u8, 55u8, 29u8, 213u8, 40u8, 213u8, 126u8, 178u8,
        184u8, 86u8, 210u8, 56u8,
    ],
    [
        7u8, 249u8, 216u8, 55u8, 203u8, 23u8, 176u8, 211u8, 99u8, 32u8, 255u8, 233u8, 59u8, 165u8,
        35u8, 69u8, 241u8, 183u8, 40u8, 87u8, 26u8, 86u8, 130u8, 101u8, 202u8, 172u8, 151u8, 85u8,
        157u8, 188u8, 149u8, 42u8,
    ],
    [
        43u8, 148u8, 207u8, 94u8, 135u8, 70u8, 179u8, 245u8, 201u8, 99u8, 31u8, 76u8, 93u8, 243u8,
        41u8, 7u8, 166u8, 153u8, 197u8, 140u8, 148u8, 178u8, 173u8, 77u8, 123u8, 92u8, 236u8, 22u8,
        57u8, 24u8, 63u8, 85u8,
    ],
    [
        45u8, 238u8, 147u8, 197u8, 166u8, 102u8, 69u8, 150u8, 70u8, 234u8, 125u8, 34u8, 204u8,
        169u8, 225u8, 188u8, 254u8, 215u8, 30u8, 105u8, 81u8, 185u8, 83u8, 97u8, 29u8, 17u8, 221u8,
        163u8, 46u8, 160u8, 157u8, 120u8,
    ],
    [
        7u8, 130u8, 149u8, 229u8, 162u8, 43u8, 132u8, 233u8, 130u8, 207u8, 96u8, 30u8, 182u8, 57u8,
        89u8, 123u8, 139u8, 5u8, 21u8, 168u8, 140u8, 181u8, 172u8, 127u8, 168u8, 164u8, 170u8,
        190u8, 60u8, 135u8, 52u8, 157u8,
    ],
    [
        47u8, 165u8, 229u8, 241u8, 143u8, 96u8, 39u8, 166u8, 80u8, 27u8, 236u8, 134u8, 69u8, 100u8,
        71u8, 42u8, 97u8, 107u8, 46u8, 39u8, 74u8, 65u8, 33u8, 26u8, 68u8, 76u8, 190u8, 58u8,
        153u8, 243u8, 204u8, 97u8,
    ],
    [
        14u8, 136u8, 67u8, 118u8, 208u8, 216u8, 253u8, 33u8, 236u8, 183u8, 128u8, 56u8, 158u8,
        148u8, 31u8, 102u8, 228u8, 94u8, 122u8, 204u8, 227u8, 226u8, 40u8, 171u8, 62u8, 33u8, 86u8,
        166u8, 20u8, 252u8, 215u8, 71u8,
    ],
    [
        27u8, 114u8, 1u8, 218u8, 114u8, 73u8, 79u8, 30u8, 40u8, 113u8, 122u8, 209u8, 165u8, 46u8,
        180u8, 105u8, 249u8, 88u8, 146u8, 249u8, 87u8, 113u8, 53u8, 51u8, 222u8, 97u8, 117u8,
        229u8, 218u8, 25u8, 10u8, 242u8,
    ],
    [
        31u8, 141u8, 136u8, 34u8, 114u8, 94u8, 54u8, 56u8, 82u8, 0u8, 192u8, 178u8, 1u8, 36u8,
        152u8, 25u8, 166u8, 230u8, 225u8, 228u8, 101u8, 8u8, 8u8, 181u8, 190u8, 188u8, 107u8,
        250u8, 206u8, 125u8, 118u8, 54u8,
    ],
    [
        44u8, 93u8, 130u8, 246u8, 108u8, 145u8, 75u8, 175u8, 185u8, 112u8, 21u8, 137u8, 186u8,
        140u8, 252u8, 251u8, 97u8, 98u8, 176u8, 161u8, 42u8, 207u8, 136u8, 168u8, 208u8, 135u8,
        154u8, 4u8, 113u8, 181u8, 248u8, 90u8,
    ],
    [
        20u8, 197u8, 65u8, 72u8, 160u8, 148u8, 11u8, 184u8, 32u8, 149u8, 127u8, 90u8, 223u8, 63u8,
        161u8, 19u8, 78u8, 245u8, 196u8, 170u8, 161u8, 19u8, 244u8, 100u8, 100u8, 88u8, 242u8,
        112u8, 224u8, 191u8, 191u8, 208u8,
    ],
    [
        25u8, 13u8, 51u8, 177u8, 47u8, 152u8, 111u8, 150u8, 30u8, 16u8, 192u8, 238u8, 68u8, 216u8,
        185u8, 175u8, 17u8, 190u8, 37u8, 88u8, 140u8, 173u8, 137u8, 212u8, 22u8, 17u8, 142u8, 75u8,
        244u8, 235u8, 232u8, 12u8,
    ],
    [
        34u8, 249u8, 138u8, 169u8, 206u8, 112u8, 65u8, 82u8, 172u8, 23u8, 53u8, 73u8, 20u8, 173u8,
        115u8, 237u8, 17u8, 103u8, 174u8, 101u8, 150u8, 175u8, 81u8, 10u8, 165u8, 179u8, 100u8,
        147u8, 37u8, 224u8, 108u8, 146u8,
    ],
    [
        42u8, 124u8, 124u8, 155u8, 108u8, 229u8, 136u8, 11u8, 159u8, 111u8, 34u8, 141u8, 114u8,
        191u8, 106u8, 87u8, 90u8, 82u8, 111u8, 41u8, 198u8, 110u8, 204u8, 238u8, 248u8, 183u8,
        83u8, 211u8, 139u8, 186u8, 115u8, 35u8,
    ],
    [
        46u8, 129u8, 134u8, 229u8, 88u8, 105u8, 142u8, 193u8, 198u8, 122u8, 249u8, 193u8, 77u8,
        70u8, 63u8, 252u8, 71u8, 0u8, 67u8, 201u8, 194u8, 152u8, 139u8, 149u8, 77u8, 117u8, 221u8,
        100u8, 63u8, 54u8, 185u8, 146u8,
    ],
    [
        15u8, 87u8, 197u8, 87u8, 30u8, 154u8, 78u8, 171u8, 73u8, 226u8, 200u8, 207u8, 5u8, 13u8,
        174u8, 148u8, 138u8, 239u8, 110u8, 173u8, 100u8, 115u8, 146u8, 39u8, 53u8, 70u8, 36u8,
        157u8, 28u8, 31u8, 241u8, 15u8,
    ],
    [
        24u8, 48u8, 238u8, 103u8, 181u8, 251u8, 85u8, 74u8, 213u8, 246u8, 61u8, 67u8, 136u8, 128u8,
        14u8, 28u8, 254u8, 120u8, 227u8, 16u8, 105u8, 125u8, 70u8, 228u8, 60u8, 156u8, 227u8, 97u8,
        52u8, 247u8, 44u8, 202u8,
    ],
    [
        33u8, 52u8, 231u8, 106u8, 197u8, 210u8, 26u8, 171u8, 24u8, 108u8, 43u8, 225u8, 221u8,
        143u8, 132u8, 238u8, 136u8, 10u8, 30u8, 70u8, 234u8, 247u8, 18u8, 249u8, 211u8, 113u8,
        182u8, 223u8, 34u8, 25u8, 31u8, 62u8,
    ],
    [
        25u8, 223u8, 144u8, 236u8, 132u8, 78u8, 188u8, 79u8, 254u8, 235u8, 216u8, 102u8, 243u8,
        56u8, 89u8, 176u8, 192u8, 81u8, 216u8, 201u8, 88u8, 238u8, 58u8, 168u8, 143u8, 143u8,
        141u8, 243u8, 219u8, 145u8, 165u8, 177u8,
    ],
    [
        24u8, 204u8, 162u8, 166u8, 107u8, 92u8, 7u8, 135u8, 152u8, 30u8, 105u8, 174u8, 253u8,
        132u8, 133u8, 45u8, 116u8, 175u8, 14u8, 147u8, 239u8, 73u8, 18u8, 180u8, 100u8, 140u8, 5u8,
        247u8, 34u8, 239u8, 229u8, 43u8,
    ],
    [
        35u8, 136u8, 144u8, 148u8, 21u8, 35u8, 13u8, 27u8, 77u8, 19u8, 4u8, 210u8, 213u8, 79u8,
        71u8, 58u8, 98u8, 131u8, 56u8, 242u8, 239u8, 173u8, 131u8, 250u8, 223u8, 5u8, 100u8, 69u8,
        73u8, 210u8, 83u8, 141u8,
    ],
    [
        39u8, 23u8, 31u8, 180u8, 169u8, 123u8, 108u8, 192u8, 233u8, 232u8, 245u8, 67u8, 181u8,
        41u8, 77u8, 232u8, 102u8, 162u8, 175u8, 44u8, 156u8, 141u8, 11u8, 29u8, 150u8, 230u8,
        115u8, 228u8, 82u8, 158u8, 213u8, 64u8,
    ],
    [
        47u8, 246u8, 101u8, 5u8, 64u8, 246u8, 41u8, 253u8, 87u8, 17u8, 160u8, 188u8, 116u8, 252u8,
        13u8, 40u8, 220u8, 178u8, 48u8, 185u8, 57u8, 37u8, 131u8, 229u8, 248u8, 213u8, 150u8,
        150u8, 221u8, 230u8, 174u8, 33u8,
    ],
    [
        18u8, 12u8, 88u8, 241u8, 67u8, 212u8, 145u8, 233u8, 89u8, 2u8, 247u8, 245u8, 39u8, 119u8,
        120u8, 162u8, 224u8, 173u8, 81u8, 104u8, 246u8, 173u8, 215u8, 86u8, 105u8, 147u8, 38u8,
        48u8, 206u8, 97u8, 21u8, 24u8,
    ],
    [
        31u8, 33u8, 254u8, 183u8, 13u8, 63u8, 33u8, 176u8, 123u8, 248u8, 83u8, 213u8, 229u8, 219u8,
        3u8, 7u8, 30u8, 196u8, 149u8, 160u8, 165u8, 101u8, 162u8, 29u8, 162u8, 214u8, 101u8, 210u8,
        121u8, 72u8, 55u8, 149u8,
    ],
    [
        36u8, 190u8, 144u8, 95u8, 167u8, 19u8, 53u8, 225u8, 76u8, 99u8, 140u8, 192u8, 246u8, 106u8,
        134u8, 35u8, 168u8, 38u8, 231u8, 104u8, 6u8, 138u8, 158u8, 150u8, 139u8, 177u8, 161u8,
        221u8, 225u8, 138u8, 114u8, 210u8,
    ],
    [
        15u8, 134u8, 102u8, 182u8, 46u8, 209u8, 116u8, 145u8, 197u8, 12u8, 234u8, 222u8, 173u8,
        87u8, 212u8, 205u8, 89u8, 126u8, 243u8, 130u8, 29u8, 101u8, 195u8, 40u8, 116u8, 76u8,
        116u8, 229u8, 83u8, 218u8, 194u8, 109u8,
    ],
    [
        9u8, 24u8, 212u8, 107u8, 245u8, 45u8, 152u8, 176u8, 52u8, 65u8, 63u8, 74u8, 26u8, 28u8,
        65u8, 89u8, 78u8, 122u8, 122u8, 63u8, 106u8, 224u8, 140u8, 180u8, 61u8, 26u8, 42u8, 35u8,
        14u8, 25u8, 89u8, 239u8,
    ],
    [
        27u8, 190u8, 176u8, 27u8, 76u8, 71u8, 158u8, 205u8, 231u8, 105u8, 23u8, 100u8, 94u8, 64u8,
        77u8, 250u8, 46u8, 38u8, 249u8, 13u8, 10u8, 252u8, 90u8, 101u8, 18u8, 133u8, 19u8, 173u8,
        55u8, 92u8, 95u8, 242u8,
    ],
    [
        47u8, 104u8, 161u8, 197u8, 142u8, 37u8, 126u8, 66u8, 161u8, 122u8, 108u8, 97u8, 223u8,
        245u8, 85u8, 30u8, 213u8, 96u8, 185u8, 146u8, 42u8, 177u8, 25u8, 213u8, 172u8, 142u8, 24u8,
        76u8, 151u8, 52u8, 234u8, 217u8,
    ],
];
