use crate::{KafkaError, util};

use serde::de::{self, Visitor};
use serde::forward_to_deserialize_any;
use std::io;

pub(crate) struct Deserializer<R: io::Read> {
    reader: R,
}

impl<R: io::Read> Deserializer<R> {
    pub(crate) fn new(reader: R) -> Self {
        Deserializer { reader }
    }
}

impl<'de, R: io::Read> de::Deserializer<'de> for &mut Deserializer<R> {
    type Error = KafkaError;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(KafkaError::DeserializationError(
            "deserialize_any is not supported".to_string(),
        ))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut length_bytes = [0u8; 2];
        self.reader.read_exact(&mut length_bytes)?;
        let length = i16::from_be_bytes(length_bytes);
        if length < 0 {
            return Err(KafkaError::DeserializationError(
                "negative string length".to_string(),
            ));
        }
        let mut string_bytes = vec![0u8; length as usize];
        self.reader.read_exact(&mut string_bytes)?;
        visitor.visit_bytes(&string_bytes)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut byte = [0u8; 1];
        self.reader.read_exact(&mut byte)?;
        visitor.visit_bool(byte[0] == 1)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut byte = [0u8; 1];
        self.reader.read_exact(&mut byte)?;
        visitor.visit_i8(i8::from_be_bytes(byte))
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut bytes = [0u8; 2];
        self.reader.read_exact(&mut bytes)?;
        visitor.visit_i16(i16::from_be_bytes(bytes))
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut bytes = [0u8; 4];
        self.reader.read_exact(&mut bytes)?;
        visitor.visit_i32(i32::from_be_bytes(bytes))
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut bytes = [0u8; 8];
        self.reader.read_exact(&mut bytes)?;
        visitor.visit_i64(i64::from_be_bytes(bytes))
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut byte = [0u8; 1];
        self.reader.read_exact(&mut byte)?;
        visitor.visit_u8(byte[0])
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut bytes = [0u8; 2];
        self.reader.read_exact(&mut bytes)?;
        visitor.visit_u16(u16::from_be_bytes(bytes))
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut bytes = [0u8; 4];
        self.reader.read_exact(&mut bytes)?;
        visitor.visit_u32(u32::from_be_bytes(bytes))
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let v = util::decode_unsigned_varint(&mut self.reader)?;
        println!("Deserialized u64: {}", v);
        visitor.visit_u64(v)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut seq_access = SeqAccess {
            deserializer: self,
            len,
        };
        visitor.visit_seq(&mut seq_access)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(fields.len(), visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    forward_to_deserialize_any! {
        f32 f64 char unit bytes byte_buf
        unit_struct newtype_struct map
        enum identifier ignored_any option seq
    }
}

struct SeqAccess<'a, R: io::Read> {
    deserializer: &'a mut Deserializer<R>,
    len: usize,
}

impl<'de, 'a, R: io::Read> de::SeqAccess<'de> for SeqAccess<'a, R> {
    type Error = KafkaError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        if self.len == 0 {
            return Ok(None);
        }
        self.len -= 1;
        let value = seed.deserialize(&mut *self.deserializer)?;
        Ok(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestStruct {
        a: i32,
        b: bool,
        c: String,
    }

    #[test]
    fn test_deserializer() {
        let data: Vec<u8> = vec![
            0, 0, 0, 42, // a: i32 = 42
            1,  // b: bool = true
            0, 5, // c: String length = 5
            b'H', b'e', b'l', b'l', b'o', // c: String = "Hello"
        ];

        let mut deserializer = Deserializer::new(&data[..]);
        let result: TestStruct = Deserialize::deserialize(&mut deserializer).unwrap();

        assert_eq!(
            result,
            TestStruct {
                a: 42,
                b: true,
                c: "Hello".to_string(),
            }
        );
    }
}
