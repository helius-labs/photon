use std::collections::HashMap;

use itertools::Itertools;
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseTransaction, DbErr, EntityTrait, Statement,
    TransactionTrait, Value,
};
use solana_program::pubkey::Pubkey;

use crate::api::error::PhotonApiError;
use crate::dao::generated::state_trees;
use crate::ingester::parser::tree_info::TreeInfo;
use crate::ingester::persist::leaf_node::STATE_TREE_HEIGHT_V2;
use crate::ingester::persist::persisted_indexed_merkle_tree::format_bytes;

pub fn get_proof_path(index: i64, include_leaf: bool) -> Vec<i64> {
    let mut indexes = vec![];
    let mut idx = index;
    if include_leaf {
        indexes.push(index);
    }
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

pub fn get_level_by_node_index(index: i64, tree_height: u32) -> i64 {
    if index >= 2_i64.pow(tree_height - 2) {
        // If it's a leaf index (large number)
        return 0;
    }
    let mut level = 0;
    let mut idx = index;
    while idx > 1 {
        idx >>= 1;
        level += 1;
    }
    level
}

pub async fn get_proof_nodes<T>(
    txn_or_conn: &T,
    leaf_nodes_locations: Vec<(Vec<u8>, i64)>,
    include_leafs: bool,
    include_empty_leaves: bool,
    tree_height: Option<u32>,
) -> Result<HashMap<(Vec<u8>, i64), state_trees::Model>, DbErr>
where
    T: ConnectionTrait + TransactionTrait,
{
    let all_required_node_indices = leaf_nodes_locations
        .iter()
        .flat_map(|(tree, index)| {
            get_proof_path(*index, include_leafs)
                .iter()
                .map(move |&idx| (tree.clone(), idx))
                .collect::<Vec<(Vec<u8>, i64)>>()
        })
        .sorted_by(|a, b| {
            // Need to sort elements before dedup
            a.0.cmp(&b.0) // Sort by tree
                .then_with(|| a.1.cmp(&b.1)) // Then by node index
        })
        .dedup()
        .collect::<Vec<(Vec<u8>, i64)>>();

    let mut params = Vec::new();
    let mut placeholders = Vec::new();

    for (index, (tree, node_idx)) in all_required_node_indices.into_iter().enumerate() {
        let param_index = index * 2; // each pair contributes two parameters
        params.push(Value::from(tree));
        params.push(Value::from(node_idx));
        placeholders.push(format!("(${}, ${})", param_index + 1, param_index + 2));
    }

    let placeholder_str = placeholders.join(", ");
    let sql = format!(
            "WITH vals(tree, node_idx) AS (VALUES {}) SELECT st.* FROM state_trees st JOIN vals v ON st.tree = v.tree AND st.node_idx = v.node_idx",
            placeholder_str
        );

    let proof_nodes = state_trees::Entity::find()
        .from_raw_sql(Statement::from_sql_and_values(
            txn_or_conn.get_database_backend(),
            &sql,
            params,
        ))
        .all(txn_or_conn)
        .await?;

    let mut result = proof_nodes
        .iter()
        .map(|node| ((node.tree.clone(), node.node_idx), node.clone()))
        .collect::<HashMap<(Vec<u8>, i64), state_trees::Model>>();

    if include_empty_leaves {
        leaf_nodes_locations.iter().for_each(|(tree, index)| {
            result.entry((tree.clone(), *index)).or_insert_with(|| {
                let tree_pubkey = Pubkey::try_from(tree.clone()).unwrap();
                let tree_height = if let Some(height) = tree_height {
                    height
                } else {
                    let height = TreeInfo::height(&tree_pubkey.to_string());
                    height.unwrap_or(STATE_TREE_HEIGHT_V2)
                };
                let tree_height = tree_height + 1;
                let model = state_trees::Model {
                    tree: tree.clone(),
                    level: get_level_by_node_index(*index, tree_height),
                    node_idx: *index,
                    hash: ZERO_BYTES[get_level_by_node_index(*index, tree_height) as usize]
                        .to_vec(),
                    leaf_idx: None,
                    seq: None,
                };
                model
            });
        });
    }

    Ok(result)
}

pub fn validate_leaf_index(leaf_index: u32, tree_height: u32) -> bool {
    let max_leaves = 2_u64.pow(tree_height - 1);
    (leaf_index as u64) < max_leaves
}

pub fn get_merkle_proof_length(tree_height: u32) -> usize {
    (tree_height - 1) as usize
}

pub async fn get_subtrees(
    txn: &DatabaseTransaction,
    tree: Vec<u8>,
    tree_height: usize,
) -> Result<Vec<[u8; 32]>, PhotonApiError> {
    let mut subtrees = vec![[0u8; 32]; tree_height];

    let query = match txn.get_database_backend() {
        DatabaseBackend::Postgres => Statement::from_string(
            DatabaseBackend::Postgres,
            format!(
                "WITH ranked_nodes AS (
                        SELECT level, node_idx, hash,
                            ROW_NUMBER() OVER (PARTITION BY level ORDER BY node_idx DESC) as rank,
                            COUNT(*) OVER (PARTITION BY level) as count
                        FROM state_trees
                        WHERE tree = {}
                    )
                    SELECT level, hash
                    FROM ranked_nodes
                    WHERE (count % 2 = 0 AND rank = 2) OR (count % 2 = 1 AND rank = 1)",
                format_bytes(tree.clone(), DatabaseBackend::Postgres)
            ),
        ),
        DatabaseBackend::Sqlite => Statement::from_string(
            DatabaseBackend::Sqlite,
            format!(
                "SELECT t1.level, t1.hash
                    FROM state_trees t1
                    JOIN (
                        SELECT level,
                               MAX(node_idx) as max_idx,
                               COUNT(*) as count
                        FROM state_trees
                        WHERE tree = {}
                        GROUP BY level
                    ) t2 ON t1.level = t2.level
                    WHERE t1.tree = {} AND
                          ((t2.count % 2 = 0 AND t1.node_idx = t2.max_idx - 1) OR
                           (t2.count % 2 = 1 AND t1.node_idx = t2.max_idx))",
                format_bytes(tree.clone(), DatabaseBackend::Sqlite),
                format_bytes(tree.clone(), DatabaseBackend::Sqlite)
            ),
        ),
        _ => {
            return Err(PhotonApiError::UnexpectedError(
                "Unsupported database backend".to_string(),
            ))
        }
    };

    let results = txn
        .query_all(query)
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Failed to query nodes: {}", e)))?;

    if results.is_empty() {
        return Ok(EMPTY_SUBTREES.to_vec());
    }

    for row in results {
        let level: i64 = row.try_get("", "level").map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to extract level: {}", e))
        })?;

        let hash: Vec<u8> = row.try_get("", "hash").map_err(|e| {
            PhotonApiError::UnexpectedError(format!("Failed to extract hash: {}", e))
        })?;

        if level >= 0 && level < tree_height as i64 && hash.len() == 32 {
            let mut hash_array = [0u8; 32];
            hash_array.copy_from_slice(&hash);
            subtrees[level as usize] = hash_array;
        }
    }

    Ok(subtrees)
}

pub const MAX_HEIGHT: usize = 40;
type ZeroBytes = [[u8; 32]; MAX_HEIGHT + 1];

pub const ZERO_BYTES: ZeroBytes = [
    [
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ],
    [
        32, 152, 245, 251, 158, 35, 158, 171, 60, 234, 195, 242, 123, 129, 228, 129, 220, 49, 36,
        213, 95, 254, 213, 35, 168, 57, 238, 132, 70, 182, 72, 100,
    ],
    [
        16, 105, 103, 61, 205, 177, 34, 99, 223, 48, 26, 111, 245, 132, 167, 236, 38, 26, 68, 203,
        157, 198, 141, 240, 103, 164, 119, 68, 96, 177, 241, 225,
    ],
    [
        24, 244, 51, 49, 83, 126, 226, 175, 46, 61, 117, 141, 80, 247, 33, 6, 70, 124, 110, 234,
        80, 55, 29, 213, 40, 213, 126, 178, 184, 86, 210, 56,
    ],
    [
        7, 249, 216, 55, 203, 23, 176, 211, 99, 32, 255, 233, 59, 165, 35, 69, 241, 183, 40, 87,
        26, 86, 130, 101, 202, 172, 151, 85, 157, 188, 149, 42,
    ],
    [
        43, 148, 207, 94, 135, 70, 179, 245, 201, 99, 31, 76, 93, 243, 41, 7, 166, 153, 197, 140,
        148, 178, 173, 77, 123, 92, 236, 22, 57, 24, 63, 85,
    ],
    [
        45, 238, 147, 197, 166, 102, 69, 150, 70, 234, 125, 34, 204, 169, 225, 188, 254, 215, 30,
        105, 81, 185, 83, 97, 29, 17, 221, 163, 46, 160, 157, 120,
    ],
    [
        7, 130, 149, 229, 162, 43, 132, 233, 130, 207, 96, 30, 182, 57, 89, 123, 139, 5, 21, 168,
        140, 181, 172, 127, 168, 164, 170, 190, 60, 135, 52, 157,
    ],
    [
        47, 165, 229, 241, 143, 96, 39, 166, 80, 27, 236, 134, 69, 100, 71, 42, 97, 107, 46, 39,
        74, 65, 33, 26, 68, 76, 190, 58, 153, 243, 204, 97,
    ],
    [
        14, 136, 67, 118, 208, 216, 253, 33, 236, 183, 128, 56, 158, 148, 31, 102, 228, 94, 122,
        204, 227, 226, 40, 171, 62, 33, 86, 166, 20, 252, 215, 71,
    ],
    [
        27, 114, 1, 218, 114, 73, 79, 30, 40, 113, 122, 209, 165, 46, 180, 105, 249, 88, 146, 249,
        87, 113, 53, 51, 222, 97, 117, 229, 218, 25, 10, 242,
    ],
    [
        31, 141, 136, 34, 114, 94, 54, 56, 82, 0, 192, 178, 1, 36, 152, 25, 166, 230, 225, 228,
        101, 8, 8, 181, 190, 188, 107, 250, 206, 125, 118, 54,
    ],
    [
        44, 93, 130, 246, 108, 145, 75, 175, 185, 112, 21, 137, 186, 140, 252, 251, 97, 98, 176,
        161, 42, 207, 136, 168, 208, 135, 154, 4, 113, 181, 248, 90,
    ],
    [
        20, 197, 65, 72, 160, 148, 11, 184, 32, 149, 127, 90, 223, 63, 161, 19, 78, 245, 196, 170,
        161, 19, 244, 100, 100, 88, 242, 112, 224, 191, 191, 208,
    ],
    [
        25, 13, 51, 177, 47, 152, 111, 150, 30, 16, 192, 238, 68, 216, 185, 175, 17, 190, 37, 88,
        140, 173, 137, 212, 22, 17, 142, 75, 244, 235, 232, 12,
    ],
    [
        34, 249, 138, 169, 206, 112, 65, 82, 172, 23, 53, 73, 20, 173, 115, 237, 17, 103, 174, 101,
        150, 175, 81, 10, 165, 179, 100, 147, 37, 224, 108, 146,
    ],
    [
        42, 124, 124, 155, 108, 229, 136, 11, 159, 111, 34, 141, 114, 191, 106, 87, 90, 82, 111,
        41, 198, 110, 204, 238, 248, 183, 83, 211, 139, 186, 115, 35,
    ],
    [
        46, 129, 134, 229, 88, 105, 142, 193, 198, 122, 249, 193, 77, 70, 63, 252, 71, 0, 67, 201,
        194, 152, 139, 149, 77, 117, 221, 100, 63, 54, 185, 146,
    ],
    [
        15, 87, 197, 87, 30, 154, 78, 171, 73, 226, 200, 207, 5, 13, 174, 148, 138, 239, 110, 173,
        100, 115, 146, 39, 53, 70, 36, 157, 28, 31, 241, 15,
    ],
    [
        24, 48, 238, 103, 181, 251, 85, 74, 213, 246, 61, 67, 136, 128, 14, 28, 254, 120, 227, 16,
        105, 125, 70, 228, 60, 156, 227, 97, 52, 247, 44, 202,
    ],
    [
        33, 52, 231, 106, 197, 210, 26, 171, 24, 108, 43, 225, 221, 143, 132, 238, 136, 10, 30, 70,
        234, 247, 18, 249, 211, 113, 182, 223, 34, 25, 31, 62,
    ],
    [
        25, 223, 144, 236, 132, 78, 188, 79, 254, 235, 216, 102, 243, 56, 89, 176, 192, 81, 216,
        201, 88, 238, 58, 168, 143, 143, 141, 243, 219, 145, 165, 177,
    ],
    [
        24, 204, 162, 166, 107, 92, 7, 135, 152, 30, 105, 174, 253, 132, 133, 45, 116, 175, 14,
        147, 239, 73, 18, 180, 100, 140, 5, 247, 34, 239, 229, 43,
    ],
    [
        35, 136, 144, 148, 21, 35, 13, 27, 77, 19, 4, 210, 213, 79, 71, 58, 98, 131, 56, 242, 239,
        173, 131, 250, 223, 5, 100, 69, 73, 210, 83, 141,
    ],
    [
        39, 23, 31, 180, 169, 123, 108, 192, 233, 232, 245, 67, 181, 41, 77, 232, 102, 162, 175,
        44, 156, 141, 11, 29, 150, 230, 115, 228, 82, 158, 213, 64,
    ],
    [
        47, 246, 101, 5, 64, 246, 41, 253, 87, 17, 160, 188, 116, 252, 13, 40, 220, 178, 48, 185,
        57, 37, 131, 229, 248, 213, 150, 150, 221, 230, 174, 33,
    ],
    [
        18, 12, 88, 241, 67, 212, 145, 233, 89, 2, 247, 245, 39, 119, 120, 162, 224, 173, 81, 104,
        246, 173, 215, 86, 105, 147, 38, 48, 206, 97, 21, 24,
    ],
    [
        31, 33, 254, 183, 13, 63, 33, 176, 123, 248, 83, 213, 229, 219, 3, 7, 30, 196, 149, 160,
        165, 101, 162, 29, 162, 214, 101, 210, 121, 72, 55, 149,
    ],
    [
        36, 190, 144, 95, 167, 19, 53, 225, 76, 99, 140, 192, 246, 106, 134, 35, 168, 38, 231, 104,
        6, 138, 158, 150, 139, 177, 161, 221, 225, 138, 114, 210,
    ],
    [
        15, 134, 102, 182, 46, 209, 116, 145, 197, 12, 234, 222, 173, 87, 212, 205, 89, 126, 243,
        130, 29, 101, 195, 40, 116, 76, 116, 229, 83, 218, 194, 109,
    ],
    [
        9, 24, 212, 107, 245, 45, 152, 176, 52, 65, 63, 74, 26, 28, 65, 89, 78, 122, 122, 63, 106,
        224, 140, 180, 61, 26, 42, 35, 14, 25, 89, 239,
    ],
    [
        27, 190, 176, 27, 76, 71, 158, 205, 231, 105, 23, 100, 94, 64, 77, 250, 46, 38, 249, 13,
        10, 252, 90, 101, 18, 133, 19, 173, 55, 92, 95, 242,
    ],
    [
        47, 104, 161, 197, 142, 37, 126, 66, 161, 122, 108, 97, 223, 245, 85, 30, 213, 96, 185,
        146, 42, 177, 25, 213, 172, 142, 24, 76, 151, 52, 234, 217,
    ],
    [
        17, 2, 210, 248, 219, 5, 228, 175, 72, 66, 232, 173, 61, 133, 237, 69, 235, 40, 68, 126,
        183, 33, 34, 53, 162, 40, 29, 90, 181, 216, 29, 17,
    ],
    [
        42, 248, 193, 202, 245, 96, 221, 65, 249, 151, 160, 31, 248, 149, 178, 30, 13, 31, 237,
        183, 134, 231, 202, 210, 153, 1, 225, 35, 16, 99, 139, 220,
    ],
    [
        1, 29, 146, 59, 193, 75, 90, 19, 151, 42, 199, 223, 230, 66, 11, 21, 176, 66, 92, 152, 186,
        128, 237, 175, 94, 2, 145, 180, 162, 101, 224, 165,
    ],
    [
        34, 76, 204, 37, 152, 24, 34, 212, 197, 182, 252, 25, 159, 188, 116, 130, 132, 136, 116,
        28, 113, 81, 166, 21, 158, 207, 170, 183, 194, 168, 186, 201,
    ],
    [
        39, 232, 57, 246, 245, 85, 254, 174, 130, 74, 180, 51, 227, 75, 28, 14, 100, 206, 92, 117,
        150, 45, 255, 193, 60, 104, 87, 29, 107, 74, 97, 14,
    ],
    [
        42, 186, 32, 63, 189, 4, 191, 171, 200, 107, 77, 80, 214, 171, 173, 195, 194, 79, 55, 239,
        160, 14, 112, 14, 244, 209, 119, 100, 194, 204, 213, 124,
    ],
    [
        17, 239, 244, 246, 12, 44, 220, 197, 72, 193, 224, 119, 12, 61, 100, 180, 156, 1, 227, 77,
        164, 175, 41, 207, 234, 87, 90, 25, 190, 250, 102, 156,
    ],
    [
        19, 52, 250, 75, 85, 47, 0, 239, 64, 51, 97, 201, 53, 193, 171, 207, 137, 104, 81, 0, 60,
        64, 218, 169, 59, 176, 253, 10, 11, 185, 168, 129,
    ],
];

pub const EMPTY_SUBTREES: [[u8; 32]; 40] = [
    [
        20, 60, 11, 236, 225, 135, 154, 131, 147, 160, 45, 8, 88, 53, 104, 12, 211, 241, 51, 6,
        246, 74, 149, 120, 67, 52, 190, 125, 51, 177, 204, 231,
    ],
    [
        31, 158, 23, 142, 99, 175, 62, 243, 89, 151, 175, 72, 239, 185, 225, 179, 167, 48, 176, 66,
        195, 189, 200, 52, 107, 214, 155, 69, 247, 250, 226, 202,
    ],
    [
        19, 53, 131, 105, 217, 57, 70, 146, 52, 4, 3, 167, 240, 151, 58, 0, 196, 32, 99, 84, 76,
        42, 51, 75, 135, 144, 252, 132, 243, 170, 126, 218,
    ],
    [
        39, 179, 94, 18, 252, 185, 169, 255, 120, 165, 144, 16, 1, 32, 20, 232, 78, 83, 218, 104,
        223, 120, 246, 194, 144, 226, 215, 127, 21, 103, 157, 48,
    ],
    [
        21, 116, 243, 6, 243, 192, 189, 219, 189, 237, 53, 145, 180, 121, 80, 146, 31, 120, 183,
        128, 238, 190, 81, 33, 151, 25, 207, 48, 152, 127, 141, 196,
    ],
    [
        40, 105, 204, 182, 42, 188, 141, 71, 231, 97, 186, 195, 220, 120, 189, 248, 88, 37, 93,
        197, 211, 126, 116, 221, 215, 237, 30, 134, 31, 11, 213, 22,
    ],
    [
        25, 103, 213, 65, 243, 229, 97, 252, 65, 135, 200, 39, 64, 150, 246, 78, 114, 102, 37, 172,
        240, 218, 98, 33, 73, 157, 252, 203, 242, 34, 134, 27,
    ],
    [
        34, 113, 156, 10, 247, 149, 107, 116, 102, 2, 152, 241, 120, 255, 80, 226, 138, 209, 90,
        163, 184, 73, 215, 142, 246, 100, 3, 142, 254, 77, 7, 164,
    ],
    [
        31, 114, 74, 136, 58, 155, 61, 88, 232, 183, 167, 48, 111, 128, 60, 101, 135, 98, 66, 200,
        17, 196, 170, 231, 105, 247, 238, 143, 21, 25, 114, 129,
    ],
    [
        27, 154, 178, 97, 223, 248, 20, 167, 245, 141, 187, 0, 173, 79, 50, 250, 158, 98, 234, 188,
        22, 229, 82, 118, 175, 98, 3, 50, 90, 3, 187, 207,
    ],
    [
        16, 104, 254, 45, 183, 113, 222, 104, 115, 158, 200, 25, 144, 146, 226, 57, 52, 46, 13,
        124, 111, 108, 231, 126, 80, 205, 224, 99, 58, 195, 45, 237,
    ],
    [
        39, 43, 62, 71, 142, 70, 192, 22, 189, 89, 31, 84, 118, 66, 8, 113, 138, 167, 100, 95, 148,
        181, 61, 134, 137, 220, 116, 59, 136, 72, 25, 138,
    ],
    [
        36, 228, 214, 34, 17, 186, 197, 10, 241, 7, 241, 206, 118, 189, 76, 189, 196, 10, 217, 252,
        247, 150, 24, 140, 214, 248, 118, 108, 213, 1, 92, 198,
    ],
    [
        4, 189, 19, 92, 24, 21, 120, 8, 133, 122, 170, 157, 119, 3, 177, 200, 248, 222, 196, 156,
        78, 112, 49, 227, 141, 246, 238, 56, 191, 154, 99, 117,
    ],
    [
        26, 118, 35, 14, 29, 120, 25, 237, 212, 51, 71, 54, 14, 205, 9, 90, 205, 145, 140, 117,
        110, 167, 151, 210, 248, 190, 35, 218, 59, 223, 197, 1,
    ],
    [
        12, 241, 73, 63, 160, 180, 28, 33, 62, 197, 246, 157, 161, 66, 116, 24, 174, 135, 182, 73,
        76, 245, 139, 76, 248, 128, 41, 15, 235, 9, 229, 230,
    ],
    [
        17, 202, 129, 100, 100, 182, 8, 91, 177, 159, 126, 19, 108, 195, 153, 221, 163, 174, 193,
        8, 194, 165, 155, 41, 108, 56, 241, 208, 15, 66, 145, 128,
    ],
    [
        0, 83, 234, 224, 150, 177, 45, 230, 158, 255, 65, 243, 111, 114, 187, 108, 220, 0, 70, 229,
        107, 177, 41, 137, 213, 159, 42, 62, 247, 0, 216, 96,
    ],
    [
        29, 202, 53, 98, 1, 67, 189, 200, 130, 89, 23, 23, 208, 215, 54, 165, 91, 206, 69, 99, 155,
        53, 217, 145, 133, 32, 27, 31, 44, 230, 239, 73,
    ],
    [
        36, 69, 17, 148, 122, 1, 46, 80, 219, 3, 19, 243, 191, 116, 209, 28, 21, 11, 131, 79, 42,
        126, 213, 156, 155, 89, 41, 194, 4, 149, 5, 35,
    ],
    [
        42, 106, 233, 49, 211, 12, 253, 42, 193, 117, 87, 179, 82, 196, 194, 131, 227, 9, 130, 126,
        180, 60, 44, 169, 161, 193, 163, 99, 219, 242, 165, 87,
    ],
    [
        21, 207, 14, 234, 32, 22, 204, 3, 216, 58, 149, 23, 3, 106, 179, 151, 60, 246, 210, 119,
        26, 63, 89, 197, 100, 213, 222, 15, 174, 33, 53, 72,
    ],
    [
        43, 68, 149, 146, 2, 237, 193, 0, 190, 230, 192, 129, 108, 193, 83, 16, 23, 71, 2, 156, 68,
        159, 242, 252, 82, 153, 72, 136, 169, 74, 122, 130,
    ],
    [
        7, 102, 143, 18, 231, 221, 200, 47, 188, 129, 252, 174, 152, 169, 54, 90, 227, 216, 240,
        239, 38, 59, 233, 225, 172, 43, 75, 88, 217, 34, 87, 119,
    ],
    [
        3, 128, 2, 111, 8, 97, 254, 204, 199, 175, 195, 74, 254, 96, 128, 111, 72, 106, 54, 2, 26,
        247, 200, 143, 164, 50, 57, 65, 136, 219, 8, 231,
    ],
    [
        47, 14, 38, 144, 35, 169, 70, 232, 32, 157, 247, 141, 16, 125, 161, 188, 55, 73, 250, 121,
        65, 27, 149, 219, 127, 61, 63, 166, 98, 73, 236, 51,
    ],
    [
        12, 192, 184, 61, 225, 169, 250, 219, 72, 31, 85, 251, 98, 186, 97, 83, 160, 211, 181, 216,
        86, 130, 15, 129, 178, 199, 135, 240, 163, 58, 240, 219,
    ],
    [
        47, 61, 30, 232, 48, 100, 74, 6, 78, 160, 155, 36, 120, 233, 107, 9, 247, 81, 44, 217, 182,
        33, 236, 77, 211, 159, 240, 234, 4, 63, 159, 38,
    ],
    [
        17, 219, 227, 3, 22, 187, 208, 6, 2, 186, 189, 158, 169, 243, 95, 194, 16, 217, 238, 184,
        130, 7, 227, 17, 45, 2, 161, 116, 220, 34, 249, 234,
    ],
    [
        3, 19, 38, 52, 143, 142, 62, 92, 44, 102, 47, 19, 242, 24, 121, 153, 12, 240, 243, 101, 9,
        37, 202, 115, 148, 161, 16, 30, 155, 161, 144, 59,
    ],
    [
        26, 79, 66, 22, 76, 127, 70, 53, 10, 130, 68, 184, 169, 180, 247, 196, 185, 112, 105, 124,
        242, 46, 162, 120, 17, 173, 217, 34, 66, 139, 8, 51,
    ],
    [
        23, 171, 234, 98, 23, 43, 129, 46, 93, 139, 200, 35, 152, 181, 44, 141, 118, 244, 139, 189,
        136, 47, 181, 52, 19, 209, 86, 114, 197, 137, 164, 181,
    ],
    [
        40, 23, 73, 77, 37, 176, 209, 183, 235, 236, 200, 180, 114, 31, 149, 225, 195, 224, 233,
        120, 192, 230, 186, 5, 198, 58, 194, 22, 103, 58, 32, 176,
    ],
    [
        6, 145, 221, 211, 203, 200, 190, 59, 185, 28, 165, 245, 37, 127, 198, 35, 201, 18, 171,
        213, 144, 114, 59, 165, 3, 118, 71, 186, 237, 23, 29, 228,
    ],
    [
        13, 119, 99, 90, 179, 59, 218, 184, 47, 201, 2, 28, 166, 172, 184, 174, 168, 204, 37, 99,
        144, 151, 166, 26, 25, 108, 118, 134, 170, 194, 209, 15,
    ],
    [
        2, 24, 228, 167, 106, 255, 169, 124, 55, 163, 82, 236, 43, 16, 172, 112, 45, 212, 136, 119,
        58, 237, 174, 44, 25, 175, 19, 88, 207, 59, 68, 49,
    ],
    [
        34, 146, 4, 113, 182, 175, 110, 154, 135, 193, 142, 67, 244, 138, 77, 4, 153, 64, 211, 240,
        160, 174, 247, 63, 246, 251, 180, 130, 80, 134, 69, 121,
    ],
    [
        17, 17, 176, 127, 81, 172, 152, 12, 232, 113, 93, 220, 185, 138, 2, 151, 222, 104, 2, 11,
        3, 122, 90, 91, 101, 241, 227, 190, 253, 77, 105, 225,
    ],
    [
        15, 123, 40, 209, 247, 124, 235, 50, 168, 21, 2, 25, 212, 49, 117, 220, 94, 93, 94, 163,
        81, 60, 188, 191, 133, 218, 172, 138, 102, 49, 254, 29,
    ],
    [
        30, 209, 19, 178, 108, 200, 254, 107, 4, 17, 208, 112, 159, 65, 102, 227, 197, 226, 36,
        230, 109, 247, 80, 151, 118, 73, 113, 218, 240, 66, 200, 201,
    ],
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::typedefs::hash::Hash;
    use crate::common::typedefs::serializable_pubkey::SerializablePubkey;
    use crate::ingester::persist::leaf_node::leaf_index_to_node_index;
    use crate::ingester::persist::{compute_parent_hash, MerkleProofWithContext};

    fn node_index_to_leaf_index(index: i64, tree_height: u32) -> i64 {
        index - 2_i64.pow(get_level_by_node_index(index, tree_height) as u32)
    }

    #[test]
    fn test_get_level_by_node_index() {
        // Tree of height 3 (root level is 0, max is 3)
        // Node indices in a binary tree: [1, 2, 3, 4, 5, 6, 7]
        assert_eq!(get_level_by_node_index(1, 33), 0); // Root node
        assert_eq!(get_level_by_node_index(2, 33), 1); // Level 1, left child of root
        assert_eq!(get_level_by_node_index(3, 33), 1); // Level 1, right child of root
        assert_eq!(get_level_by_node_index(4, 33), 2); // Level 2, left child of node 2
        assert_eq!(get_level_by_node_index(5, 33), 2); // Level 2, right child of node 2
        assert_eq!(get_level_by_node_index(6, 33), 2); // Level 2, left child of node 3
        assert_eq!(get_level_by_node_index(7, 33), 2); // Level 2, right child of node 3
    }

    // Test helper to convert byte arrays to hex strings for easier debugging
    fn bytes_to_hex(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<String>>()
            .join("")
    }

    // Helper to verify node index calculations
    fn verify_node_index_conversion(leaf_index: u32, tree_height: u32) -> bool {
        let node_index = leaf_index_to_node_index(leaf_index, tree_height);
        let recovered_leaf_index = node_index_to_leaf_index(node_index, tree_height);
        recovered_leaf_index == leaf_index as i64
    }

    #[test]
    fn test_zero_bytes_consistency() {
        // Verify that each level's hash in ZERO_BYTES is correctly computed from its children
        for level in (1..MAX_HEIGHT).rev() {
            let parent_hash = compute_parent_hash(
                ZERO_BYTES[level - 1].to_vec(),
                ZERO_BYTES[level - 1].to_vec(),
            )
            .unwrap();

            assert_eq!(
                parent_hash,
                ZERO_BYTES[level].to_vec(),
                "Zero bytes hash mismatch at level {}\nComputed: {}\nExpected: {}",
                level,
                bytes_to_hex(&parent_hash),
                bytes_to_hex(&ZERO_BYTES[level])
            );
        }
    }

    #[ignore = "todo check whether to keep"]
    #[test]
    fn test_debug_leaf_zero() {
        let leaf_index = 0u32;
        let tree_height = 32u32;
        let node_index = leaf_index_to_node_index(leaf_index, tree_height);
        let recovered_leaf_index = node_index_to_leaf_index(node_index, tree_height);

        println!("leaf_index: {}", leaf_index);
        println!("node_index: {}", node_index);
        println!(
            "level: {}",
            get_level_by_node_index(node_index, tree_height)
        );
        println!("recovered_leaf_index: {}", recovered_leaf_index);

        assert_eq!(recovered_leaf_index, leaf_index as i64);
    }

    #[ignore = "todo check whether to keep"]
    #[test]
    fn test_debug_max_leaf() {
        let leaf_index = u32::MAX;
        let tree_height = 32u32;
        let node_index = leaf_index_to_node_index(leaf_index, tree_height);
        let recovered_leaf_index = node_index_to_leaf_index(node_index, tree_height);

        println!("max test:");
        println!("leaf_index: {} (u32)", leaf_index);
        println!("node_index: {} (i64)", node_index);
        println!("2^(tree_height-1): {} (i64)", 2_i64.pow(tree_height - 1));
        println!(
            "level: {}",
            get_level_by_node_index(node_index, tree_height)
        );
        println!("recovered_leaf_index: {} (i64)", recovered_leaf_index);

        assert_eq!(recovered_leaf_index, leaf_index as i64);
    }

    #[ignore = "todo check whether to keep"]
    #[test]
    fn test_leaf_index_conversions() {
        let test_cases = vec![
            (0u32, 32u32),          // First leaf in height 32 tree
            (1u32, 32u32),          // Second leaf
            (4294967295u32, 32u32), // Last possible leaf in u32
            (2147483647u32, 32u32), // i32::MAX
            (2147483648u32, 32u32), // i32::MAX + 1
            (0u32, 3u32),           // Small tree test
            (1u32, 3u32),
            (2u32, 3u32),
            (3u32, 3u32),
        ];

        for (leaf_index, tree_height) in test_cases {
            assert!(
                verify_node_index_conversion(leaf_index, tree_height),
                "Conversion failed for leaf_index={}, tree_height={}",
                leaf_index,
                tree_height
            );
        }
    }

    #[test]
    fn test_proof_validation_components() {
        // Test case for first non-existent leaf (index 0)
        let test_leaf_index = 0u32;
        let tree_height = 32u32;
        // Create proof components
        let node_index = leaf_index_to_node_index(test_leaf_index, tree_height);
        let proof_path = get_proof_path(node_index, false);

        println!("Test leaf index: {}", test_leaf_index);
        println!("Node index: {}", node_index);
        println!("Proof path: {:?}", proof_path);

        // Verify proof path length
        assert_eq!(proof_path.len(), tree_height as usize);

        // Test level calculation for proof path nodes
        for &idx in &proof_path {
            let level = get_level_by_node_index(idx, tree_height);
            println!("Node {} is at level {}", idx, level);
            assert!(level < tree_height as i64);
        }

        // Manually compute root hash using proof path
        let mut current_hash = ZERO_BYTES[0].to_vec(); // Start with leaf level zero bytes

        for (idx, _) in proof_path.iter().enumerate() {
            let is_left = (node_index >> idx) & 1 == 0;
            let sibling_hash = ZERO_BYTES[idx].to_vec();

            let (left_child, right_child) = if is_left {
                (current_hash.clone(), sibling_hash)
            } else {
                (sibling_hash, current_hash.clone())
            };

            current_hash = compute_parent_hash(left_child, right_child).unwrap();

            println!(
                "Level {}: Computed hash: {}",
                idx,
                bytes_to_hex(&current_hash)
            );
            println!(
                "         Expected:     {}",
                bytes_to_hex(&ZERO_BYTES[idx + 1])
            );

            // Verify against precalculated ZERO_BYTES
            assert_eq!(
                current_hash,
                ZERO_BYTES[idx + 1].to_vec(),
                "Hash mismatch at level {}",
                idx + 1
            );
        }
    }

    #[test]
    fn test_validate_proof() {
        let test_leaf_index = 0u32;
        let merkle_tree = SerializablePubkey::try_from(vec![0u8; 32]).unwrap();

        // Create a proof for testing
        let mut proof = Vec::new();
        for i in 0..31 {
            // One less than tree height since root is separate
            proof.push(Hash::try_from(ZERO_BYTES[i].to_vec()).unwrap());
        }

        let proof_context = MerkleProofWithContext {
            proof,
            root: Hash::try_from(ZERO_BYTES[31].to_vec()).unwrap(),
            leaf_index: test_leaf_index,
            hash: Hash::try_from(ZERO_BYTES[0].to_vec()).unwrap(),
            merkle_tree,
            root_seq: 0,
        };

        // Validate the proof
        let result = proof_context.validate();
        assert!(result.is_ok(), "Proof validation failed: {:?}", result);
    }

    #[test]
    fn test_validate_leaf_index() {
        assert!(validate_leaf_index(0, 27));
        assert!(validate_leaf_index((1 << 26) - 1, 27));
        assert!(!validate_leaf_index(1 << 26, 27));
        assert!(validate_leaf_index(0, 33));
    }

    #[test]
    fn test_merkle_proof_length() {
        assert_eq!(get_merkle_proof_length(27), 26);
        assert_eq!(get_merkle_proof_length(33), 32);
    }
}
