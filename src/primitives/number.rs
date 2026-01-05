use crate::{primitives::ByteSize, util};
use serde::{de, ser};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct VarInt32(i32);

impl AsRef<i32> for VarInt32 {
    fn as_ref(&self) -> &i32 {
        &self.0
    }
}

impl std::ops::Deref for VarInt32 {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ByteSize for VarInt32 {
    fn byte_size(&self) -> usize {
        util::encode_varint_i32(self.0).len()
    }
}

impl ser::Serialize for VarInt32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let bytes = util::encode_varint_i32(self.0);
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> de::Deserialize<'de> for VarInt32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct VarInt32Visitor;

        impl<'de> de::Visitor<'de> for VarInt32Visitor {
            type Value = VarInt32;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a varint encoded i32")
            }

            fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let v = util::decode_varint_i32(value)
                    .map_err(|e| de::Error::custom(format!("failed to decode varint i32: {e}")))?;
                Ok(VarInt32(v))
            }
        }

        deserializer.deserialize_byte_buf(VarInt32Visitor)
    }
}
