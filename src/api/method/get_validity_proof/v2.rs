use crate::{
    api::error::PhotonApiError,
    common::typedefs::serializable_pubkey::SerializablePubkey,
    ingester::persist::persisted_state_tree::{
        get_multiple_compressed_leaf_proofs, MerkleProofWithContext,
    },
};
use borsh::BorshSerialize;
use light_compressed_account::hash_chain::create_two_inputs_hash_chain;
use reqwest::Client;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};

use crate::api::method::{
    get_multiple_new_address_proofs::{
        get_multiple_new_address_proofs_helper, AddressWithTree, MerkleContextWithNewAddressProof,
        ADDRESS_TREE_ADDRESS,
    },
    utils::Context,
};

use crate::api::method::get_validity_proof::common::{
    convert_inclusion_proofs_to_hex, convert_non_inclusion_merkle_proof_to_hex, hash_to_hex,
    negate_and_compress_proof, proof_from_json_struct, CompressedProofWithContext,
    GetValidityProofRequest, GetValidityProofResponse, GnarkProofJson, HexBatchInputsForProver,
    STATE_TREE_QUEUE_SIZE,
};

fn get_public_input_hash(
    account_proofs: &[MerkleProofWithContext],
    new_address_proofs: &[MerkleContextWithNewAddressProof],
) -> [u8; 32] {
    let account_hashes: Vec<[u8; 32]> = account_proofs
        .iter()
        .map(|x| x.hash.to_vec().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let account_roots: Vec<[u8; 32]> = account_proofs
        .iter()
        .map(|x| x.root.to_vec().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let inclusion_hash_chain: [u8; 32] =
        create_two_inputs_hash_chain(&account_roots, &account_hashes).unwrap();
    let new_address_hashes: Vec<[u8; 32]> = new_address_proofs
        .iter()
        .map(|x| x.address.try_to_vec().unwrap().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let new_address_roots: Vec<[u8; 32]> = new_address_proofs
        .iter()
        .map(|x| x.root.to_vec().clone().try_into().unwrap())
        .collect::<Vec<[u8; 32]>>();
    let non_inclusion_hash_chain =
        create_two_inputs_hash_chain(&new_address_roots, &new_address_hashes).unwrap();
    let public_input_hash = if non_inclusion_hash_chain != [0u8; 32] {
        non_inclusion_hash_chain
    } else if inclusion_hash_chain != [0u8; 32] {
        inclusion_hash_chain
    } else {
        create_two_inputs_hash_chain(&[inclusion_hash_chain], &[non_inclusion_hash_chain]).unwrap()
    };
    public_input_hash
}

pub async fn get_validity_proof_v2(
    conn: &DatabaseConnection,
    prover_url: &str,
    mut request: GetValidityProofRequest,
) -> Result<GetValidityProofResponse, PhotonApiError> {
    if request.hashes.is_empty()
        && request.newAddresses.is_empty()
        && request.newAddressesWithTrees.is_empty()
    {
        return Err(PhotonApiError::ValidationError(
            "No hashes or new addresses provided for proof generation".to_string(),
        ));
    }
    if !request.newAddressesWithTrees.is_empty() && !request.newAddresses.is_empty() {
        return Err(PhotonApiError::ValidationError(
            "Cannot provide both newAddresses and newAddressesWithTree".to_string(),
        ));
    }
    if !request.newAddresses.is_empty() {
        request.newAddressesWithTrees = request
            .newAddresses
            .iter()
            .map(|new_address| AddressWithTree {
                address: *new_address,
                tree: SerializablePubkey::from(ADDRESS_TREE_ADDRESS),
            })
            .collect();
    }

    let context = Context::extract(conn).await?;
    let client = Client::new();
    let tx = conn.begin().await?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        tx.execute(Statement::from_string(
            tx.get_database_backend(),
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;".to_string(),
        ))
        .await?;
    }

    let account_proofs = match !request.hashes.is_empty() {
        true => get_multiple_compressed_leaf_proofs(&tx, request.hashes).await?,
        false => {
            vec![]
        }
    };
    let new_address_proofs = match !request.newAddressesWithTrees.is_empty() {
        true => get_multiple_new_address_proofs_helper(&tx, request.newAddressesWithTrees).await?,
        false => {
            vec![]
        }
    };
    tx.commit().await?;
    let state_tree_height = if account_proofs.is_empty() {
        0
    } else {
        account_proofs[0].proof.len() as u32
    };
    let address_tree_height = if new_address_proofs.is_empty() {
        0
    } else {
        new_address_proofs[0].proof.len() as u32
    };
    let circuit_type = if state_tree_height != 0 && address_tree_height != 0 {
        "combined".to_string()
    } else if state_tree_height != 0 {
        "inclusion".to_string()
    } else if address_tree_height != 0 {
        "non-inclusion".to_string()
    } else {
        return Err(PhotonApiError::ValidationError(
            "No proofs found for the given hashes or new addresses".to_string(),
        ));
    };

    // TODO: add mainnet option which creates legacy proofs
    let public_input_hash = if circuit_type == "inclusion" && state_tree_height == 32 {
        hash_to_hex(
            &get_public_input_hash(&account_proofs, &new_address_proofs)
                .try_into()
                .unwrap(),
        )
    } else {
        String::new()
    };

    let batch_inputs = HexBatchInputsForProver {
        public_input_hash,
        state_tree_height,
        address_tree_height,
        circuit_type,
        input_compressed_accounts: convert_inclusion_proofs_to_hex(account_proofs.clone()),
        new_addresses: convert_non_inclusion_merkle_proof_to_hex(new_address_proofs.clone()),
    };

    let inclusion_proof_url = format!("{}/prove", prover_url);
    let json_body = serde_json::to_string(&batch_inputs).map_err(|e| {
        PhotonApiError::UnexpectedError(format!("Got an error while serializing the request {}", e))
    })?;

    let res = client
        .post(&inclusion_proof_url)
        .body(json_body.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Error fetching proof {}", e)))?;

    if !res.status().is_success() {
        return Err(PhotonApiError::UnexpectedError(format!(
            "Error fetching proof {:?}",
            res.text().await,
        )));
    }

    let text = res
        .text()
        .await
        .map_err(|e| PhotonApiError::UnexpectedError(format!("Error fetching proof {}", e)))?;

    let proof: GnarkProofJson = serde_json::from_str(&text).map_err(|e| {
        PhotonApiError::UnexpectedError(format!(
            "Got an error while deserializing the response {}",
            e
        ))
    })?;

    let proof = proof_from_json_struct(proof);

    #[allow(non_snake_case)]
    let compressedProof = negate_and_compress_proof(proof);

    let compressed_proof_with_context = CompressedProofWithContext {
        compressedProof,
        roots: account_proofs
            .iter()
            .map(|x| x.root.clone().to_string())
            .chain(
                new_address_proofs
                    .iter()
                    .map(|x| x.root.clone().to_string()),
            )
            .collect(),
        rootIndices: account_proofs
            .iter()
            .map(|x| x.rootSeq)
            .chain(new_address_proofs.iter().map(|x| x.rootSeq))
            .map(|x| x % STATE_TREE_QUEUE_SIZE)
            .collect(),
        leafIndices: account_proofs
            .iter()
            .map(|x| x.leafIndex)
            .chain(new_address_proofs.iter().map(|x| x.lowElementLeafIndex))
            .collect(),
        leaves: account_proofs
            .iter()
            .map(|x| x.hash.clone().to_string())
            .chain(
                new_address_proofs
                    .iter()
                    .map(|x| x.address.clone().to_string()),
            )
            .collect(),
        merkleTrees: account_proofs
            .iter()
            .map(|x| x.merkleTree.clone().to_string())
            .chain(
                new_address_proofs
                    .iter()
                    .map(|x| x.merkleTree.clone().to_string()),
            )
            .collect(),
    };
    Ok(GetValidityProofResponse {
        value: compressed_proof_with_context,
        context,
    })
}
