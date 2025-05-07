use serde::de::Visitor;
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use serde_json::Number;
use std::fmt;
use utoipa::{
    openapi::{KnownFormat, ObjectBuilder, RefOr, Schema, SchemaFormat, SchemaType},
    ToSchema,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default, Copy, PartialOrd, Ord)]
#[serde(transparent)]
pub struct UnsignedInteger(pub u64);

impl<'__s> ToSchema<'__s> for UnsignedInteger {
    fn schema() -> (&'__s str, RefOr<Schema>) {
        let schema = Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Integer)
                .default(Some(serde_json::Value::Number(Number::from(100))))
                .example(Some(serde_json::Value::Number(serde_json::Number::from(
                    100,
                ))))
                .format(Some(SchemaFormat::KnownFormat(KnownFormat::UInt64)))
                .build(),
        );
        ("UnsignedInteger", RefOr::T(schema))
    }
}

impl<'de> Deserialize<'de> for UnsignedInteger {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct UnsignedIntegerVisitor;

        impl<'de> Visitor<'de> for UnsignedIntegerVisitor {
            type Value = UnsignedInteger;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an unsigned integer or string containing an unsigned integer")
            }

            fn visit_u64<E>(self, value: u64) -> Result<UnsignedInteger, E>
            where
                E: Error,
            {
                Ok(UnsignedInteger(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<UnsignedInteger, E>
            where
                E: Error,
            {
                value
                    .parse::<u64>()
                    .map(UnsignedInteger)
                    .map_err(|e| Error::custom(format!("Invalid unsigned integer value: {}", e)))
            }
        }

        deserializer.deserialize_any(UnsignedIntegerVisitor)
    }
}

impl anchor_lang::AnchorDeserialize for UnsignedInteger {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
        if buf.len() < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Buffer underflow",
            ));
        }
        let (int_bytes, rest) = buf.split_at(8);
        *buf = rest;
        let value = u64::from_le_bytes(int_bytes.try_into().expect("slice with incorrect length"));
        Ok(UnsignedInteger(value))
    }

    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut buffer = [0u8; 8]; // Adjusting the size for u64
        reader.read_exact(&mut buffer)?;
        let value = u64::from_le_bytes(buffer);
        Ok(UnsignedInteger(value))
    }
}

impl anchor_lang::AnchorSerialize for UnsignedInteger {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
        writer.write_all(&self.0.to_le_bytes())
    }
}
