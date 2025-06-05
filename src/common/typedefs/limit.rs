use crate::api::method::utils::PAGE_LIMIT;
use jsonrpsee_core::Serialize;
use serde::{de, Deserialize, Deserializer};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct Limit(pub(crate) u64);

impl Limit {
    pub fn new(value: u64) -> Result<Self, &'static str> {
        if value > PAGE_LIMIT {
            Err("Value must be less than or equal to 1000")
        } else {
            Ok(Limit(value))
        }
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl Default for Limit {
    fn default() -> Self {
        Limit(PAGE_LIMIT)
    }
}

impl<'de> Deserialize<'de> for Limit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        if value > PAGE_LIMIT {
            Err(de::Error::invalid_value(
                de::Unexpected::Unsigned(value),
                &"a value less than or equal to 1000",
            ))
        } else {
            Ok(Limit(value))
        }
    }
}
