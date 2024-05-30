// use std::collections::{BTreeMap, HashMap};

// use sea_orm::{
//     sea_query::OnConflict, ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait,
//     QuerySelect, Set, Statement,
// };

// use crate::{dao::generated::indexed_trees, ingester::error::IngesterError};

// pub struct PersistedIndexedMerkledTree {
//     txn: DatabaseTransaction,
// }

// impl PersistedIndexedMerkledTree {
//     pub async fn new(txn: DatabaseTransaction) -> Result<Self, IngesterError> {
//         let instance = Self { txn };
//         instance.multi_append(vec![0]).await?;
//         Ok(instance)
//     }

//     pub async fn multi_append(&self, values: Vec<i64>) -> Result<(), IngesterError> {
//         if self.txn.get_database_backend() == DatabaseBackend::Postgres {
//             self.txn
//                 .execute(Statement::from_string(
//                     self.txn.get_database_backend(),
//                     "LOCK TABLE indexed_trees IN EXCLUSIVE MODE;".to_string(),
//                 ))
//                 .await
//                 .map_err(|e| {
//                     IngesterError::DatabaseError(format!("Failed to lock state_trees table: {}", e))
//                 })?;
//         }

//         // Get the max index from the database
//         let max_index = self
//             .txn
//             .query_one(Statement::from_string(
//                 self.txn.get_database_backend(),
//                 "SELECT MAX(index) as index FROM indexed_trees".to_string(),
//             ))
//             .await
//             .map_err(|e| IngesterError::DatabaseError(format!("Failed to execute query: {}", e)))?
//             .ok_or(IngesterError::DatabaseError(
//                 "No max index found".to_string(),
//             ))?;

//         let mut current_index: i64 = max_index.try_get("", "index")?;
//         let mut indexed_tree = self.query_next_smallest_elements(values.clone()).await?;
//         let mut elements_to_update: HashMap<i64, indexed_trees::Model> = HashMap::new();

//         for value in values {
//             current_index += 1;

//             let mut indexed_element = indexed_trees::Model {
//                 index: current_index + 1,
//                 value,
//                 next_index: 0,
//                 next_value: 0,
//             };

//             let next_largest = indexed_tree
//                 .range(..value) // This ranges from the start up to, but not including, `key`
//                 .next_back() // Gets the last element in the range, which is the largest key less than `key`
//                 .map(|(_, v)| v.clone());

//             if let Some(mut next_largest) = next_largest {
//                 indexed_element.next_index = next_largest.index;
//                 indexed_element.next_value = next_largest.value;

//                 next_largest.next_index = current_index;
//                 next_largest.next_value = value;

//                 elements_to_update.insert(next_largest.index, next_largest.clone());
//                 indexed_tree.insert(next_largest.value, next_largest);
//             }

//             elements_to_update.insert(current_index, indexed_element.clone());
//             indexed_tree.insert(value, indexed_element);
//         }

//         let active_elements = elements_to_update
//             .values()
//             .map(|x| indexed_trees::ActiveModel {
//                 index: Set(x.index),
//                 value: Set(x.value),
//                 next_index: Set(x.next_index),
//                 next_value: Set(x.next_value),
//             });

//         indexed_trees::Entity::insert_many(active_elements).on_conflict(
//             OnConflict::columns([indexed_trees::Column::Index])
//                 .update_columns([
//                     indexed_trees::Column::NextIndex,
//                     indexed_trees::Column::NextValue,
//                 ])
//                 .to_owned(),
//         );

//         Ok(())
//     }

//     pub async fn get_values(
//         &self,
//         values: Vec<i64>,
//     ) -> Result<Vec<indexed_trees::Model>, IngesterError> {
//         let response = indexed_trees::Entity::find().all().exec(&self.txn).await?;
//         Ok(response)
//     }

//     pub async fn query_next_smallest_elements(
//         &self,
//         values: Vec<i64>,
//     ) -> Result<BTreeMap<i64, indexed_trees::Model>, IngesterError> {
//         let sql_statements = values.iter().map(|value| {
//             format!(
//                 "SELECT * FROM indexed_trees WHERE value < {} ORDER BY value DESC LIMIT 1",
//                 value
//             )
//         });

//         let full_query = sql_statements.collect::<Vec<String>>().join(" UNION ALL ");
//         let response = self
//             .txn
//             .query_all(Statement::from_string(
//                 self.txn.get_database_backend(),
//                 full_query,
//             ))
//             .await
//             .map_err(|e| IngesterError::DatabaseError(format!("Failed to execute query: {}", e)))?;

//         let mut indexed_tree: BTreeMap<i64, indexed_trees::Model> = BTreeMap::new();
//         for row in response {
//             let model = indexed_trees::Model {
//                 index: row.try_get("", "index")?,
//                 value: row.try_get("", "value")?,
//                 next_index: row.try_get("", "next_index")?,
//                 next_value: row.try_get("", "next_value")?,
//             };
//             indexed_tree.insert(model.value, model);
//         }
//         Ok(indexed_tree)
//     }

//     // Map response to a vector of indexed_trees
// }
