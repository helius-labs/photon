use self::state_update::{EnrichedPathNode, EnrichedUtxo, StateUpdate, UtxoWithSlot};

use super::{error, parser::bundle::EventBundle};
use crate::dao::{
    generated::{state_trees, token_owners, utxos},
    typedefs::hash::Hash,
};
use borsh::{BorshDeserialize, BorshSerialize};
use psp_compressed_pda::utxo::Utxo;
use psp_compressed_token::{AccountState, TokenTlvData};
use sea_orm::{
    sea_query::OnConflict, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryTrait, Set,
};

use error::IngesterError;
use solana_program::pubkey;
use solana_sdk::pubkey::Pubkey;
pub mod state_update;

const COMPRESSED_TOKEN_PROGRAM: Pubkey = pubkey!("9sixVEthz2kMSKfeApZXHwuboT6DZuT6crAYJTciUCqE");

pub async fn persist_bundle(
    txn: &DatabaseTransaction,
    event: EventBundle,
) -> Result<(), IngesterError> {
    persist_state_update(txn, event.try_into().unwrap()).await
}

pub async fn persist_state_update(
    txn: &DatabaseTransaction,
    state_update: StateUpdate,
) -> Result<(), IngesterError> {
    let StateUpdate {
        in_utxos,
        out_utxos,
        path_nodes,
    } = state_update;

    spend_input_utxos(txn, &in_utxos).await?;
    append_output_utxo(txn, &out_utxos).await?;
    persist_path_nodes(txn, path_nodes).await?;

    Ok(())
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

async fn spend_input_utxos(
    txn: &DatabaseTransaction,
    in_utxos: &Vec<UtxoWithSlot>,
) -> Result<(), IngesterError> {
    let in_utxo_models: Vec<utxos::ActiveModel> = in_utxos
        .iter()
        .map(|utxo| utxos::ActiveModel {
            hash: Set(utxo.utxo.hash().to_vec()),
            spent: Set(true),
            data: Set(vec![]),
            owner: Set(vec![]),
            lamports: Set(0),
            slot_updated: Set(utxo.slot),
            ..Default::default()
        })
        .collect();

    utxos::Entity::insert_many(in_utxo_models)
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

    let mut models = Vec::new();
    for in_utxo in in_utxos {
        let token_data = parse_token_data(&in_utxo.utxo)?;
        if token_data.is_some() {
            models.push(token_owners::ActiveModel {
                hash: Set(in_utxo.utxo.hash().to_vec()),
                spent: Set(true),
                amount: Set(0),
                slot_updated: Set(in_utxo.slot),
                ..Default::default()
            });
        }
    }

    token_owners::Entity::insert_many(models)
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

    Ok(())
}

// Note: I highly dislike this name but it's a consequence of the confusing compression abstractions.
//       I will fix once we clean up the compression abstractions.
pub struct EnrichedTokenData {
    pub token_tlv_data: TokenTlvData,
    pub hash: Hash,
    pub slot_updated: i64,
}

async fn append_output_utxo(
    txn: &DatabaseTransaction,
    out_utxos: &Vec<EnrichedUtxo>,
) -> Result<(), IngesterError> {
    let mut out_utxo_models = Vec::new();
    let mut token_datas = Vec::new();

    for out_utxo in out_utxos {
        let EnrichedUtxo {
            utxo: out_utxo,
            tree,
            seq,
        } = out_utxo;

        let UtxoWithSlot {
            utxo: out_utxo,
            slot,
        } = out_utxo;

        out_utxo_models.push(utxos::ActiveModel {
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
            slot_updated: Set(*slot),
            seq: Set(Some(*seq)),
            ..Default::default()
        });

        if let Some(token_data) = parse_token_data(out_utxo)? {
            token_datas.push(EnrichedTokenData {
                token_tlv_data: token_data,
                hash: Hash::from(out_utxo.hash()),
                slot_updated: *slot,
            });
        }
    }

    // The state tree is append-only so conflicts only occur if a record is already inserted or
    // marked as spent spent.
    //
    // We first build the query and then execute it because SeaORM has a bug where it always throws
    // an error if we do not insert a record in an insert statement. However, in this case, it's
    // expected not to insert anything if the key already exists.
    let query = utxos::Entity::insert_many(out_utxo_models)
        .on_conflict(
            OnConflict::column(utxos::Column::Hash)
                .do_nothing()
                .to_owned(),
        )
        .build(txn.get_database_backend());
    txn.execute(query).await?;

    Ok(())
}

pub async fn persist_token_datas(
    txn: &DatabaseTransaction,
    token_datas: Vec<EnrichedTokenData>,
) -> Result<(), IngesterError> {
    let token_models = token_datas
        .into_iter()
        .map(
            |EnrichedTokenData {
                 token_tlv_data,
                 hash,
                 slot_updated,
             }| {
                token_owners::ActiveModel {
                    hash: Set(hash.into()),
                    mint: Set(token_tlv_data.mint.to_bytes().to_vec()),
                    owner: Set(token_tlv_data.owner.to_bytes().to_vec()),
                    amount: Set(token_tlv_data.amount as i64),
                    delegate: Set(token_tlv_data.delegate.map(|d| d.to_bytes().to_vec())),
                    frozen: Set(token_tlv_data.state == AccountState::Frozen),
                    delegated_amount: Set(token_tlv_data.delegated_amount as i64),
                    is_native: Set(token_tlv_data.is_native.map(|n| n as i64)),
                    spent: Set(false),
                    slot_updated: Set(slot_updated),
                    ..Default::default()
                }
            },
        )
        .collect::<Vec<_>>();

    // We first build the query and then execute it because SeaORM has a bug where it always throws
    // an error if we do not insert a record in an insert statement. However, in this case, it's
    // expected not to insert anything if the key already exists.
    let query = token_owners::Entity::insert_many(token_models)
        .on_conflict(
            OnConflict::column(token_owners::Column::Hash)
                .do_nothing()
                .to_owned(),
        )
        .build(txn.get_database_backend());
    txn.execute(query).await?;

    Ok(())
}

async fn persist_path_nodes(
    txn: &DatabaseTransaction,
    nodes: Vec<EnrichedPathNode>,
) -> Result<(), IngesterError> {
    let node_models = nodes
        .into_iter()
        .map(|node| state_trees::ActiveModel {
            tree: Set(node.tree.to_vec()),
            level: Set(node.level as i64),
            node_idx: Set(node.node.index as i64),
            hash: Set(node.node.node.to_vec()),
            leaf_idx: Set(Some(node_idx_to_leaf_idx(
                node.node.index as i64,
                node.level as u32,
            ))),
            seq: Set(node.seq as i64),
            slot_updated: Set(node.slot),
            ..Default::default()
        })
        .collect::<Vec<_>>();

    // We first build the query and then execute it because SeaORM has a bug where it always throws
    // an error if we do not insert a record in an insert statement. However, in this case, it's
    // expected not to insert anything if the key already exists.
    let mut query = state_trees::Entity::insert_many(node_models)
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
