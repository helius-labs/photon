use crate::{
    api::{error::PhotonApiError, method::get_validity_proof::get_validity_proof},
    common::typedefs::serializable_pubkey::SerializablePubkey,
};
use borsh::BorshDeserialize;
use itertools::Itertools;
use sea_orm::{DatabaseBackend, DatabaseConnection, QueryOrder, Statement, TransactionTrait};

use super::common::{GetValidityProofRequestV2, GetValidityProofResponseV2};
use crate::common::typedefs::hash::Hash;
use crate::dao::generated::accounts;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};

const MAX_ALLOWED_HASHES: usize = 8;

pub async fn get_validity_proof_v2(
    conn: &DatabaseConnection,
    prover_url: &str,
    mut request: GetValidityProofRequestV2,
) -> Result<GetValidityProofResponseV2, PhotonApiError> {

    if request.hashes.len() > MAX_ALLOWED_HASHES {
        return Err(PhotonApiError::ValidationError(format!(
            "Too many hashes. Max allowed: {}",
            MAX_ALLOWED_HASHES
        )));
    }

    let tx = conn.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }
    // Determine which hashes are still in the queue -> prove by index
    // filter those and call get_validity_proof
    // insert hashes into return object in correct position
    let hashes = request
        .hashes
        .iter()
        .map(|h| h.to_vec())
        .collect::<Vec<Vec<u8>>>();
    let hashes_len = hashes.len();

    let accounts = accounts::Entity::find()
        .filter(
            accounts::Column::Hash
                .is_in(hashes.to_vec())
                .and(accounts::Column::Spent.eq(false)),
        )
        .all(&tx)
        .await?;

    // It's fine because we can't have more than 8 elements in request.hashes
    let accounts = accounts.iter().sorted_by(|a, b| {
        let hash_index_a = request.hashes.iter().position(|x| x.0.as_slice() == a.hash).unwrap();
        let hash_index_b = request.hashes.iter().position(|x| x.0.as_slice() == b.hash).unwrap();
        hash_index_a.cmp(&hash_index_b)
    }).collect::<Vec<_>>();

    if accounts.len() != hashes_len {
        let all_accounts = accounts::Entity::find().all(&tx).await?;
        all_accounts
            .iter()
            .for_each(|x| tracing::info!("account {:?}", x));
        return Err(PhotonApiError::ValidationError(format!(
            "Not all hashes exist. (Might be spent) input hashes {:?} found hashes {:?} with leaf indices {:?}",
           hashes, accounts
                .iter()
                .map(|x| x.hash.clone())
                .collect::<Vec<Vec<u8>>>(),
            accounts.iter().map(|x| x.leaf_index).collect::<Vec<i64>>(),
        )));
    }

    let mut removed_indices = 0;

    for (index, account) in accounts.iter().enumerate() {
        if account.in_output_queue {
            request.hashes.remove(index - removed_indices);
            removed_indices += 1;
        }
    }

    let mut v2_response: GetValidityProofResponseV2 =
        if request.hashes.is_empty() && request.newAddressesWithTrees.is_empty() {
            GetValidityProofResponseV2::default()
        } else {
            get_validity_proof(conn, prover_url, request.into())
                .await?
                .into()
        };
    accounts
        .iter()
        .try_for_each(|x| -> Result<(), PhotonApiError> {
            v2_response.value.queues.push(
                SerializablePubkey::try_from_slice(x.queue.as_slice())
                    .map_err(|e| {
                        PhotonApiError::ValidationError(format!(
                            "Error converting queue pubkey to SerializablePubkey: {:?}",
                            e
                        ))
                    })?
                    .to_string(),
            );
            Ok(())
        })?;

    // Add data of skipped accounts.
    for (index, account) in accounts
        .iter()
        .enumerate()
        .filter(|(_, x)| x.in_output_queue)
    {
        v2_response
            .value
            .leafIndices
            .insert(index, account.leaf_index as u32);
        v2_response
            .value
            .leaves
            .insert(index, Hash::new(account.hash.as_slice())?.to_string());
        v2_response.value.merkleTrees.insert(
            index,
            SerializablePubkey::try_from_slice(account.tree.as_slice())
                .unwrap_or(SerializablePubkey::default())
                .to_string(),
        );
        // proof by index has no root.
        v2_response.value.rootIndices.insert(index, None.into());
        v2_response.value.roots.insert(index, "".to_string());
    }
    Ok(v2_response)
}
