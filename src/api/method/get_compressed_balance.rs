use crate::dao::generated::accounts;
use sea_orm::{DatabaseConnection, EntityTrait, QueryFilter, QuerySelect};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::super::error::PhotonApiError;
use super::utils::{parse_decimal, AccountDataTable, LamportModel};
use super::utils::{CompressedAccountRequest, Context};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
// We do not use generics to simplify documentation generation.
pub struct GetCompressedAccountBalance {
    pub context: Context,
    pub value: u64,
}

impl GetCompressedAccountBalance {
    pub fn adjusted_schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        let mut schema = GetCompressedAccountBalance::schema().1;
        let object = match schema {
            utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(ref mut object)) => {
                let example = serde_json::to_value(GetCompressedAccountBalance {
                    context: { Context { slot: 1 } },
                    value: 1,
                })
                .unwrap();
                object.default = Some(example.clone());
                object.example = Some(example);
                object.description = Some("Response for compressed account balance".to_string());
                object.clone()
            }
            _ => unimplemented!(),
        };
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(object))
    }

}
pub async fn get_compressed_balance(
    conn: &DatabaseConnection,
    request: CompressedAccountRequest,
) -> Result<GetCompressedAccountBalance, PhotonApiError> {
    let context = Context::extract(conn).await?;
    let id = request.parse_id()?;

    let balance = accounts::Entity::find()
        .select_only()
        .column(accounts::Column::Lamports)
        .filter(id.filter(AccountDataTable::Accounts))
        .into_model::<LamportModel>()
        .one(conn)
        .await?
        .ok_or(id.not_found_error())?;

    Ok(GetCompressedAccountBalance {
        value: parse_decimal(balance.lamports)?,
        context,
    })
}
