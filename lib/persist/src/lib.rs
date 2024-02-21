use dao::generated::{state_trees, utxos};
use log::{debug, warn};
use parser::bundle::{ChangelogEvent, EventBundle, PublicStateTransitionBundle, UTXOEvent};
use sea_orm::{
    sea_query::OnConflict, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    DbBackend, DbErr, EntityTrait, QueryFilter, QueryTrait, Set, TransactionTrait,
};
use solana_sdk::signature::Signature;

use crate::error::PersistError;
pub mod error;

pub async fn persist_bundle(
    conn: &DatabaseConnection,
    event: EventBundle,
) -> Result<(), PersistError> {
    match event {
        EventBundle::PublicStateTransition(e) => {
            debug!("Persisting PublicStateTransitionBundle: {:?}", e);
            persist_state_transition(conn, e).await
        }
        EventBundle::PublicNullifier(e) => {
            debug!("Persisting PublicNullifierBundle: {:?}", e);
            Err(PersistError::EventNotImplemented {
                event_type: "PublicNullifierBundle".to_string(),
            })
        }
    }
}

async fn persist_state_transition(
    conn: &DatabaseConnection,
    bundle: PublicStateTransitionBundle,
) -> Result<(), PersistError> {
    let PublicStateTransitionBundle {
        in_utxos,
        out_utxos,
        changelogs,
        transaction,
        slot_updated,
    } = bundle;

    if out_utxos.len() != changelogs.len() {
        return Err(PersistError::MalformedEvent {
            msg: format!(
                "Number of changelogs did not match the number of output UTXOs (txn: {})",
                transaction,
            ),
        });
    }

    let txn = conn.begin().await?;
    for in_utxo in in_utxos {
        spend_input_utxo(&txn, &in_utxo, slot_updated, transaction).await?;
    }
    for i in 0..out_utxos.len() {
        let out_utxo = &out_utxos[i];
        let cl_event = &changelogs[i];
        append_output_utxo(&txn, out_utxo, slot_updated).await?;
        persist_changelog_event(&txn, cl_event, slot_updated).await?;
    }
    txn.commit().await?;
    Ok(())
}

async fn spend_input_utxo(
    txn: &DatabaseTransaction,
    in_utxo: &UTXOEvent,
    slot_updated: i64,
    transaction: Signature,
) -> Result<(), PersistError> {
    let UTXOEvent { hash, seq, .. } = in_utxo;
    let model = utxos::ActiveModel {
        data: Set(vec![]),
        spent: Set(true),
        owner: Set(vec![]),
        slot_updated: Set(slot_updated),
        seq: Set(*seq),
        ..Default::default()
    };

    // Optimistically update the UTXO.
    let res = utxos::Entity::update(model)
        .filter(utxos::Column::Hash.eq(hash.to_vec()))
        .exec(txn)
        .await;

    // If data arrived out of order, the UTXO may not yet exist.
    // In this case we insert a placeholder marking the UTXO as spent.
    if let Err(e) = res {
        match e {
            DbErr::RecordNotFound(_) => {
                warn!("Input UTXO not found (hash: {}, txn: {}", hash, transaction);
                utxos::Entity::insert(utxos::ActiveModel {
                    hash: Set(hash.to_vec()),
                    data: Set(vec![]),
                    owner: Set(vec![]),
                    lamports: Set(None),
                    spent: Set(true),
                    slot_updated: Set(slot_updated),
                    seq: Set(*seq),
                    ..Default::default()
                })
                .on_conflict(
                    OnConflict::column(utxos::Column::Hash)
                        .update_columns([
                            utxos::Column::Hash,
                            utxos::Column::Data,
                            utxos::Column::Lamports,
                            utxos::Column::Spent,
                            utxos::Column::Seq,
                            utxos::Column::SlotUpdated,
                        ])
                        .to_owned(),
                )
                .exec(txn)
                .await?;
            }
            _ => return Err(e.into()),
        }
    }
    Ok(())
}

async fn append_output_utxo(
    txn: &DatabaseTransaction,
    out_utxo: &parser::bundle::UTXOEvent,
    slot_updated: i64,
) -> Result<(), PersistError> {
    let model = utxos::ActiveModel {
        hash: Set(out_utxo.hash.to_vec()),
        data: Set(out_utxo.data.clone()),
        tree: Set(out_utxo.tree.as_ref().to_vec()),
        account: Set(out_utxo.account.map(|a| a.as_ref().to_vec())),
        owner: Set(out_utxo.owner.as_ref().to_vec()),
        lamports: Set(out_utxo.lamports),
        spent: Set(false),
        seq: Set(out_utxo.seq),
        slot_updated: Set(slot_updated),
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

async fn persist_changelog_event(
    txn: &DatabaseTransaction,
    event: &parser::bundle::ChangelogEvent,
    slot_updated: i64,
) -> Result<(), PersistError> {
    let ChangelogEvent { path, tree, seq } = event;
    let depth = path.len() - 1;
    let items = path
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let node_idx = p.index as i64;
            let leaf_idx = if i == 0 {
                Some(node_idx_to_leaf_idx(node_idx, depth as u32))
            } else {
                None
            };
            state_trees::ActiveModel {
                tree: Set(tree.as_ref().to_vec()),
                level: Set(i as i64),
                node_idx: Set(node_idx),
                hash: Set(p.hash.to_vec()),
                leaf_idx: Set(leaf_idx),
                seq: Set(*seq as i64),
                slot_updated: Set(slot_updated),
                ..Default::default()
            }
        })
        .collect::<Vec<_>>();

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
