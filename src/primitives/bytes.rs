use crate::{
    de::{ByteSeed, VarintLenSeed},
    util,
};
use serde::{
    de,
    ser::{self, SerializeSeq},
};
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KafkaBytes(Vec<u8>);

impl_as_ref!(KafkaBytes, [u8]);

impl ser::Serialize for KafkaBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        let len = self.as_ref().len() as i32;
        seq.serialize_element(&len)?;
        seq.serialize_element(self.as_ref())?;
        seq.end()
    }
}

impl<'de> de::Deserialize<'de> for KafkaBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct KafkaBytesVisitor;

        impl<'de> de::Visitor<'de> for KafkaBytesVisitor {
            type Value = KafkaBytes;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("KafkaBytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let len: i32 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::custom("expected length element"))?;
                if len < 0 {
                    return Err(de::Error::custom("length cannot be negative"));
                }
                let byte_seed = ByteSeed::new(len as usize);
                let bytes: Vec<u8> = seq
                    .next_element_seed(byte_seed)?
                    .ok_or_else(|| de::Error::custom("expected byte array element"))?;
                Ok(KafkaBytes(bytes))
            }
        }

        deserializer.deserialize_tuple(2, KafkaBytesVisitor)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KafkaCompactBytes(Vec<u8>);

impl_as_ref!(KafkaCompactBytes, [u8]);

impl ser::Serialize for KafkaCompactBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        let varint = util::encode_unsigned_varint(self.as_ref().len() + 1);
        seq.serialize_element(&varint)?;
        seq.serialize_element(self.as_ref())?;
        seq.end()
    }
}

impl<'de> de::Deserialize<'de> for KafkaCompactBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct KafkaCompactBytesVisitor;

        impl<'de> de::Visitor<'de> for KafkaCompactBytesVisitor {
            type Value = KafkaCompactBytes;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("KafkaCompactBytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let len: usize = seq
                    .next_element_seed(VarintLenSeed)?
                    .ok_or_else(|| de::Error::custom("expected length element"))?;
                if len == 0 {
                    return Err(de::Error::custom("length cannot be zero"));
                }
                let byte_seed = ByteSeed::new(len - 1);
                let bytes: Vec<u8> = seq
                    .next_element_seed(byte_seed)?
                    .ok_or_else(|| de::Error::custom("expected byte array element"))?;
                Ok(KafkaCompactBytes(bytes))
            }
        }

        deserializer.deserialize_tuple(2, KafkaCompactBytesVisitor)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KafkaNullableBytes(Option<Vec<u8>>);

impl_as_ref!(KafkaNullableBytes, Option<Vec<u8>>);

impl ser::Serialize for KafkaNullableBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self.as_ref() {
            Some(b) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                let len = b.len() as i32;
                seq.serialize_element(&len)?;
                seq.serialize_element(b.as_slice())?;
                seq.end()
            }
            None => {
                let mut seq = serializer.serialize_seq(Some(1))?;
                let len: i32 = -1;
                seq.serialize_element(&len)?;
                seq.end()
            }
        }
    }
}

impl<'de> de::Deserialize<'de> for KafkaNullableBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct KafkaNullableBytesVisitor;

        impl<'de> de::Visitor<'de> for KafkaNullableBytesVisitor {
            type Value = KafkaNullableBytes;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("KafkaNullableBytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let len: i32 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::custom("expected length element"))?;
                if len < 0 {
                    return Ok(KafkaNullableBytes(None));
                }
                let byte_seed = ByteSeed::new(len as usize);
                let bytes: Vec<u8> = seq
                    .next_element_seed(byte_seed)?
                    .ok_or_else(|| de::Error::custom("expected byte array element"))?;
                Ok(KafkaNullableBytes(Some(bytes)))
            }
        }

        deserializer.deserialize_tuple(2, KafkaNullableBytesVisitor)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KafkaCompactNullableBytes(Option<Vec<u8>>);

impl_as_ref!(KafkaCompactNullableBytes, Option<Vec<u8>>);

impl ser::Serialize for KafkaCompactNullableBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self.as_ref() {
            Some(b) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                let varint = util::encode_unsigned_varint(b.len() + 1);
                seq.serialize_element(&varint)?;
                seq.serialize_element(b.as_slice())?;
                seq.end()
            }
            None => {
                let mut seq = serializer.serialize_seq(Some(1))?;
                let varint = util::encode_unsigned_varint(0);
                seq.serialize_element(&varint)?;
                seq.end()
            }
        }
    }
}

impl<'de> de::Deserialize<'de> for KafkaCompactNullableBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct KafkaCompactNullableBytesVisitor;

        impl<'de> de::Visitor<'de> for KafkaCompactNullableBytesVisitor {
            type Value = KafkaCompactNullableBytes;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("KafkaCompactNullableBytes")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let len: usize = seq
                    .next_element_seed(VarintLenSeed)?
                    .ok_or_else(|| de::Error::custom("expected length element"))?;
                if len == 0 {
                    return Ok(KafkaCompactNullableBytes(None));
                }
                let byte_seed = ByteSeed::new(len - 1);
                let bytes: Vec<u8> = seq
                    .next_element_seed(byte_seed)?
                    .ok_or_else(|| de::Error::custom("expected byte array element"))?;
                Ok(KafkaCompactNullableBytes(Some(bytes)))
            }
        }

        deserializer.deserialize_tuple(2, KafkaCompactNullableBytesVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{de::KafkaDeserializer, ser::KafkaSerializer};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestBytes {
        value: KafkaBytes,
    }

    #[test]
    fn test_kafka_bytes_serialization() {
        let data = TestBytes {
            value: KafkaBytes(b"Hello".to_vec()),
        };
        let mut buf = Vec::new();
        let mut serializer = KafkaSerializer::new(0, &mut buf);
        data.serialize(&mut serializer).unwrap();
        assert_eq!(
            buf,
            vec![0x00, 0x00, 0x00, 0x05, b'H', b'e', b'l', b'l', b'o']
        );
    }

    #[test]
    fn test_kafka_bytes_deserialization() {
        let data: Vec<u8> = vec![0x00, 0x00, 0x00, 0x05, b'H', b'e', b'l', b'l', b'o'];
        let mut reader = &data[..];
        let mut deserializer = KafkaDeserializer::new(&mut reader);
        let result: TestBytes = Deserialize::deserialize(&mut deserializer).unwrap();
        assert_eq!(
            result,
            TestBytes {
                value: KafkaBytes(b"Hello".to_vec()),
            }
        );
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestCompactBytes {
        value: KafkaCompactBytes,
    }

    #[test]
    fn test_kafka_compact_bytes_serialization() {
        let data = TestCompactBytes {
            value: KafkaCompactBytes(b"World".to_vec()),
        };
        let mut buf = Vec::new();
        let mut serializer = KafkaSerializer::new(0, &mut buf);
        data.serialize(&mut serializer).unwrap();
        assert_eq!(buf, vec![0x06, b'W', b'o', b'r', b'l', b'd']);
    }

    #[test]
    fn test_kafka_compact_bytes_deserialization() {
        let data: Vec<u8> = vec![0x06, b'W', b'o', b'r', b'l', b'd'];
        let mut reader = &data[..];
        let mut deserializer = KafkaDeserializer::new(&mut reader);
        let result: TestCompactBytes = Deserialize::deserialize(&mut deserializer).unwrap();
        assert_eq!(
            result,
            TestCompactBytes {
                value: KafkaCompactBytes(b"World".to_vec()),
            }
        );
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestNullableBytes {
        value: KafkaNullableBytes,
    }

    #[test]
    fn test_kafka_nullable_bytes_serialization() {
        let data = TestNullableBytes {
            value: KafkaNullableBytes(Some(b"Nullable".to_vec())),
        };
        let mut buf = Vec::new();
        let mut serializer = KafkaSerializer::new(0, &mut buf);
        data.serialize(&mut serializer).unwrap();
        assert_eq!(
            buf,
            vec![
                0x00, 0x00, 0x00, 0x08, b'N', b'u', b'l', b'l', b'a', b'b', b'l', b'e'
            ]
        );

        let data_none = TestNullableBytes {
            value: KafkaNullableBytes(None),
        };
        let mut buf_none = Vec::new();
        let mut serializer_none = KafkaSerializer::new(0, &mut buf_none);
        data_none.serialize(&mut serializer_none).unwrap();
        assert_eq!(buf_none, vec![0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_kafka_nullable_bytes_deserialization() {
        let data: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x08, b'N', b'u', b'l', b'l', b'a', b'b', b'l', b'e',
        ];
        let mut reader = &data[..];
        let mut deserializer = KafkaDeserializer::new(&mut reader);
        let result: TestNullableBytes = Deserialize::deserialize(&mut deserializer).unwrap();
        assert_eq!(
            result,
            TestNullableBytes {
                value: KafkaNullableBytes(Some(b"Nullable".to_vec())),
            }
        );

        let data_none: Vec<u8> = vec![0xFF, 0xFF, 0xFF, 0xFF];
        let mut reader_none = &data_none[..];
        let mut deserializer_none = KafkaDeserializer::new(&mut reader_none);
        let result_none: TestNullableBytes =
            Deserialize::deserialize(&mut deserializer_none).unwrap();
        assert_eq!(
            result_none,
            TestNullableBytes {
                value: KafkaNullableBytes(None),
            }
        );
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestCompactNullableBytes {
        value: KafkaCompactNullableBytes,
    }

    #[test]
    fn test_kafka_compact_nullable_bytes_serialization() {
        let data = TestCompactNullableBytes {
            value: KafkaCompactNullableBytes(Some(b"Compact".to_vec())),
        };
        let mut buf = Vec::new();
        let mut serializer = KafkaSerializer::new(0, &mut buf);
        data.serialize(&mut serializer).unwrap();
        assert_eq!(buf, vec![0x08, b'C', b'o', b'm', b'p', b'a', b'c', b't']);

        let data_none = TestCompactNullableBytes {
            value: KafkaCompactNullableBytes(None),
        };
        let mut buf_none = Vec::new();
        let mut serializer_none = KafkaSerializer::new(0, &mut buf_none);
        data_none.serialize(&mut serializer_none).unwrap();
        assert_eq!(buf_none, vec![0x00]);
    }

    #[test]
    fn test_kafka_compact_nullable_bytes_deserialization() {
        let data: Vec<u8> = vec![0x08, b'C', b'o', b'm', b'p', b'a', b'c', b't'];
        let mut reader = &data[..];
        let mut deserializer = KafkaDeserializer::new(&mut reader);
        let result: TestCompactNullableBytes = Deserialize::deserialize(&mut deserializer).unwrap();
        assert_eq!(
            result,
            TestCompactNullableBytes {
                value: KafkaCompactNullableBytes(Some(b"Compact".to_vec())),
            }
        );

        let data_none: Vec<u8> = vec![0x00];
        let mut reader_none = &data_none[..];
        let mut deserializer_none = KafkaDeserializer::new(&mut reader_none);
        let result_none: TestCompactNullableBytes =
            Deserialize::deserialize(&mut deserializer_none).unwrap();
        assert_eq!(
            result_none,
            TestCompactNullableBytes {
                value: KafkaCompactNullableBytes(None),
            }
        );
    }
}
