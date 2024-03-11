use super::{
    error,
    parser::bundle::{EventBundle, PublicTransactionEventBundle},
};
use crate::dao::{
    generated::{state_trees, token_owners, utxos},
    typedefs::hash::Hash,
};
use borsh::{BorshDeserialize, BorshSerialize};
use light_merkle_tree_event::{ChangelogEvent, PathNode};
use log::debug;
use psp_compressed_pda::utxo::Utxo;
use psp_compressed_token::{AccountState, TokenTlvData};
use sea_orm::{
    sea_query::OnConflict, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    QueryTrait, Set, TransactionTrait,
};

use error::IngesterError;
use solana_program::pubkey;
use solana_sdk::pubkey::Pubkey;

const COMPRESSED_TOKEN_PROGRAM: Pubkey = pubkey!("9sixVEthz2kMSKfeApZXHwuboT6DZuT6crAYJTciUCqE");

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

pub struct UtxoWithParsedTokenData {
    pub utxo: Utxo,
    pub token_data: Option<TokenTlvData>,
}

fn parse_token_data(utxo: &Utxo) -> Result<Option<TokenTlvData>, IngesterError> {
    Ok(match &utxo.data {
        Some(tlv) => {
            let tlv_data_element = tlv.tlv_elements.get(0);
            match tlv_data_element {
                Some(data_element) if data_element.owner == COMPRESSED_TOKEN_PROGRAM => {
                    let token_data = Some(
                        TokenTlvData::try_from_slice(&data_element.data).map_err(|_| {
                            IngesterError::ParserError("Failed to parse token data".to_string())
                        })?,
                    );
                    if tlv.tlv_elements.len() > 1 {
                        return Err(IngesterError::MalformedEvent {
                            msg: format!(
                                "More than one TLV element found in UTXO: {}",
                                Hash::from(utxo.hash())
                            ),
                        });
                    }
                    token_data
                }
                _ => None,
            }
        }
        None => None,
    })
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
        spent: Set(true),
        data: Set(vec![]),
        owner: Set(vec![]),
        lamports: Set(0),
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

    let token_data = parse_token_data(in_utxo)?;

    if token_data.is_some() {
        token_owners::Entity::insert(token_owners::ActiveModel {
            hash: Set(hash.to_vec()),
            spent: Set(true),
            amount: Set(0),
            slot_updated: Set(slot_updated),
            ..Default::default()
        })
        .on_conflict(
            OnConflict::column(token_owners::Column::Hash)
                .update_columns([
                    token_owners::Column::Hash,
                    token_owners::Column::Amount,
                    token_owners::Column::Spent,
                ])
                .to_owned(),
        )
        .exec(txn)
        .await?;
    }

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

    // The state tree is append-only so conflicts only occur if a record is already inserted or
    // marked as spent spent.
    utxos::Entity::insert(model)
        .on_conflict(
            OnConflict::column(utxos::Column::Hash)
                .do_nothing()
                .to_owned(),
        )
        .exec(txn)
        .await?;

    if let Some(token_data) = parse_token_data(out_utxo)? {
        persist_token_data(txn, out_utxo.hash().into(), slot_updated, token_data).await?;
    }

    Ok(())
}

pub async fn persist_token_data(
    txn: &DatabaseTransaction,
    hash: Hash,
    slot_updated: i64,
    token_tlv_data: TokenTlvData,
) -> Result<(), IngesterError> {
    let TokenTlvData {
        mint,
        owner,
        amount,
        delegate,
        state,
        is_native,
        delegated_amount,
    } = token_tlv_data;

    let model = token_owners::ActiveModel {
        hash: Set(hash.into()),
        mint: Set(mint.to_bytes().to_vec()),
        owner: Set(owner.to_bytes().to_vec()),
        amount: Set(amount as i64),
        delegate: Set(delegate.map(|d| d.to_bytes().to_vec())),
        frozen: Set(state == AccountState::Frozen),
        delegated_amount: Set(delegated_amount as i64),
        is_native: Set(is_native.map(|n| n as i64)),
        spent: Set(false),
        slot_updated: Set(slot_updated),
        ..Default::default()
    };

    token_owners::Entity::insert(model)
        .on_conflict(
            OnConflict::column(token_owners::Column::Hash)
                .do_nothing()
                .to_owned(),
        )
        .exec(txn)
        .await?;

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
        .build(txn.get_database_backend());
    query.sql = format!("{} WHERE excluded.seq > state_trees.seq", query.sql);
    txn.execute(query).await?;

    Ok(())
}

fn node_idx_to_leaf_idx(index: i64, tree_height: u32) -> i64 {
    index - 2i64.pow(tree_height)
}
