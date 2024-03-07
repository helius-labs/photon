use crate::{
    error,
    parser::bundle::{EventBundle, PublicTransactionEventBundle},
};
use borsh::BorshSerialize;
use dao::{
    generated::{state_trees, token_owners, utxos},
    typedefs::hash::Hash,
};
use light_merkle_tree_event::{ChangelogEvent, PathNode};
use log::debug;
use psp_compressed_pda::utxo::Utxo;
use psp_compressed_token::{AccountState, TokenTlvData};
use sea_orm::{
    sea_query::OnConflict, ActiveModelTrait, ConnectionTrait, DatabaseConnection,
    DatabaseTransaction, DbBackend, EntityTrait, QueryTrait, Set, TransactionTrait,
};

use error::IngesterError;

pub async fn persist_bundle(
    conn: &DatabaseConnection,
    event: EventBundle,
) -> Result<(), IngesterError> {
    match event {
        EventBundle::PublicTransactionEvent(e) => {
            debug!("Persisting PublicTransactionEvent: {:?}", e);
            persist_state_transition(conn, e).await
        }
    }
}

struct PathUpdate<'a> {
    tree: [u8; 32],
    path: &'a Vec<PathNode>,
    seq: i64,
}

async fn persist_state_transition(
    conn: &DatabaseConnection,
    bundle: PublicTransactionEventBundle,
) -> Result<(), IngesterError> {
    let PublicTransactionEventBundle {
        in_utxos,
        transaction,
        out_utxos,
        changelogs,
        slot,
    } = bundle;
    let changelogs = changelogs.changelogs;
    let txn = conn.begin().await?;
    // TODO: Do batch inserts here
    let slot = slot as i64;
    for in_utxo in in_utxos {
        spend_input_utxo(&txn, &in_utxo, slot).await?;
    }
    let path_updates = changelogs
        .iter()
        .map(|cl| match cl {
            ChangelogEvent::V1(cl) => {
                let tree_id = cl.id.clone();
                cl.paths.iter().map(move |p| PathUpdate {
                    tree: tree_id.clone(),
                    path: p,
                    seq: cl.seq as i64,
                })
            }
        })
        .flatten()
        .collect::<Vec<_>>();

    if out_utxos.len() != path_updates.len() {
        return Err(IngesterError::MalformedEvent {
            msg: format!(
                "Number of path updates did not match the number of output UTXOs (txn: {})",
                transaction,
            ),
        });
    }

    // TODO: Do batch inserts here
    for i in 0..out_utxos.len() {
        let out_utxo = &out_utxos[i];
        let path = &path_updates[i];
        let tree = path.tree;
        let seq = path.seq;
        append_output_utxo(&txn, out_utxo, slot, tree, seq as i64).await?;
    }
    persist_path_updates(&txn, path_updates, slot).await?;
    txn.commit().await?;
    Ok(())
}

async fn spend_input_utxo(
    txn: &DatabaseTransaction,
    in_utxo: &Utxo,
    slot_updated: i64,
) -> Result<(), IngesterError> {
    let hash = in_utxo.hash();

    utxos::Entity::insert(utxos::ActiveModel {
        hash: Set(hash.to_vec()),
        data: Set(vec![]),
        owner: Set(vec![]),
        lamports: Set(0),
        spent: Set(true),
        slot_updated: Set(slot_updated),
        ..Default::default()
    })
    .on_conflict(
        OnConflict::column(utxos::Column::Hash)
            .update_columns([
                utxos::Column::Hash,
                utxos::Column::Data,
                utxos::Column::Lamports,
                utxos::Column::Spent,
                utxos::Column::SlotUpdated,
            ])
            .to_owned(),
    )
    .exec(txn)
    .await?;

    Ok(())
}

async fn append_output_utxo(
    txn: &DatabaseTransaction,
    out_utxo: &Utxo,
    slot_updated: i64,
    tree: [u8; 32],
    seq: i64,
) -> Result<(), IngesterError> {
    let model = utxos::ActiveModel {
        hash: Set(out_utxo.hash().to_vec()),
        data: Set(out_utxo
            .data
            .clone()
            .map(|tlv| {
                tlv.try_to_vec().map_err(|_| {
                    IngesterError::ParserError(format!(
                        "Failed to serialize TLV for UXTO: {}",
                        Hash::from(out_utxo.hash())
                    ))
                })
            })
            .unwrap_or(Ok(Vec::new()))?),
        tree: Set(Some(tree.to_vec())),
        owner: Set(out_utxo.owner.as_ref().to_vec()),
        lamports: Set(out_utxo.lamports as i64),
        spent: Set(false),
        slot_updated: Set(slot_updated),
        seq: Set(Some(seq as i64)),
        ..Default::default()
    };

    // The state tree is append-only for output UTXOs.
    // Therefore we never anticpate conflicts unless the record is already inserted
    // or has been marked as spent.
    let stmt = utxos::Entity::insert(model)
        .on_conflict(
            OnConflict::column(utxos::Column::Hash)
                .do_nothing()
                .to_owned(),
        )
        .build(DbBackend::Postgres);
    txn.execute(stmt).await?;

    Ok(())
}

async fn persist_path_updates(
    txn: &DatabaseTransaction,
    paths: Vec<PathUpdate<'_>>,
    slot_updated: i64,
) -> Result<(), IngesterError> {
    let mut seen = std::collections::HashSet::new();
    let mut items = Vec::new();
    // Only include the latest state for each node.
    for path in paths.iter().rev() {
        for (i, node) in path.path.iter().enumerate() {
            let node_idx = node.index as i64;
            let depth = path.path.len() as u32 - 1;
            let leaf_idx = if i == 0 {
                Some(node_idx_to_leaf_idx(node_idx, depth))
            } else {
                None
            };
            let key = (path.tree, node_idx);
            match seen.insert(key) {
                true => items.push(state_trees::ActiveModel {
                    tree: Set(path.tree.to_vec()),
                    level: Set(i as i64),
                    node_idx: Set(node_idx),
                    hash: Set(node.node.to_vec()),
                    leaf_idx: Set(leaf_idx),
                    seq: Set(path.seq as i64),
                    slot_updated: Set(slot_updated),
                    ..Default::default()
                }),
                false => {}
            }
        }
    }

    let mut query = state_trees::Entity::insert_many(items)
        .on_conflict(
            OnConflict::columns([state_trees::Column::Tree, state_trees::Column::NodeIdx])
                .update_columns([
                    state_trees::Column::Hash,
                    state_trees::Column::Seq,
                    state_trees::Column::SlotUpdated,
                ])
                .to_owned(),
        )
        .build(DbBackend::Postgres);
    query.sql = format!("{} WHERE excluded.seq > state_trees.seq", query.sql);
    txn.execute(query).await?;

    Ok(())
}

fn node_idx_to_leaf_idx(index: i64, tree_height: u32) -> i64 {
    index - 2i64.pow(tree_height)
}

// Work in progress. I am using the latest TokenTlvData, but haven't parsed in
// the latest PublicTransactionState bundles.
pub async fn persist_token_data(
    conn: &DatabaseConnection,
    token_tlv_data: TokenTlvData,
) -> Result<(), IngesterError> {
    let txn = conn.begin().await?;
    let TokenTlvData {
        mint,
        owner,
        amount,
        delegate,
        state,
        is_native,
        delegated_amount,
        close_authority,
    } = token_tlv_data;

    let model = token_owners::ActiveModel {
        mint: Set(mint.to_bytes().to_vec()),
        owner: Set(owner.to_bytes().to_vec()),
        amount: Set(amount as i64),
        delegate: Set(delegate.map(|d| d.to_bytes().to_vec())),
        frozen: Set(state == AccountState::Frozen),
        delegated_amount: Set(delegated_amount as i64),
        is_native: Set(is_native.map(|n| n as i64)),
        close_authority: Set(close_authority.map(|c| c.to_bytes().to_vec())),
        ..Default::default()
    };
    // TODO: We are not addressing the case where the record already exists.
    model.insert(&txn).await?;
    txn.commit().await?;
    Ok(())
}
