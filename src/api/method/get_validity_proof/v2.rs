use crate::{
    api::{error::PhotonApiError, method::get_validity_proof::get_validity_proof},
    common::typedefs::serializable_pubkey::SerializablePubkey,
};
use borsh::BorshDeserialize;

use sea_orm::{DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};

use super::common::GetValidityProofResponseV2;
use crate::api::method::get_validity_proof::common::GetValidityProofRequest;
use crate::common::typedefs::hash::Hash;
use crate::dao::generated::accounts;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};

pub async fn get_validity_proof_v2(
    conn: &DatabaseConnection,
    prover_url: &str,
    mut request: GetValidityProofRequest,
) -> Result<GetValidityProofResponseV2, PhotonApiError> {
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
                .is_in(hashes)
                .and(accounts::Column::Spent.eq(false)),
        )
        .all(&tx)
        .await?;
    if accounts.len() != hashes_len {
        return Err(PhotonApiError::ValidationError(
            "Not all hashes exist. (Might be spent)".to_string(),
        ));
    }

    for (num_removed, (index, _)) in accounts
        .iter()
        .enumerate()
        .filter(|(_, x)| x.in_output_queue)
        .enumerate()
    {
        request.hashes.remove(index - num_removed);
    }

    let mut v2_response: GetValidityProofResponseV2 =
        if request.hashes.is_empty() && request.newAddresses.is_empty() {
            GetValidityProofResponseV2::default()
        } else {
            get_validity_proof(conn, prover_url, request).await?.into()
        };
    v2_response.value.queues = accounts
        .iter()
        .map(|x| {
            SerializablePubkey::try_from_slice(x.queue.as_ref().unwrap().as_slice())
                .unwrap()
                .to_string()
        })
        .collect::<Vec<String>>();
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
        v2_response.value.leaves.insert(
            index,
            Hash::new(account.hash.as_slice()).unwrap().to_string(),
        );
        v2_response.value.merkleTrees.insert(
            index,
            SerializablePubkey::try_from_slice(account.tree.as_slice())
                .unwrap()
                .to_string(),
        );
        // proof by index has no root.
        v2_response.value.rootIndices.insert(index, None);
        v2_response.value.roots.insert(index, "".to_string());
    }
    Ok(v2_response)
}
