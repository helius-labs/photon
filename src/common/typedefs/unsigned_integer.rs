use serde::{Deserialize, Serialize};
use serde_json::Number;
use utoipa::{
    openapi::{KnownFormat, ObjectBuilder, RefOr, Schema, SchemaFormat, SchemaType},
    ToSchema,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, Copy, PartialOrd, Ord)]
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
