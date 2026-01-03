use crate::{
    de::{ByteSeed, VarintLenSeed},
    util,
};
use serde::{
    de,
    ser::{self, SerializeSeq},
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KafkaCompactStr(String);

impl_as_ref!(KafkaCompactStr, str);

impl ser::Serialize for KafkaCompactStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        let s = self.as_ref();
        let varint = util::encode_unsigned_varint(s.len() + 1);
        seq.serialize_element(&varint)?;
        seq.serialize_element(s.as_bytes())?;
        seq.end()
    }
}

impl<'de> de::Deserialize<'de> for KafkaCompactStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct KafkaCompactStrVisitor;

        impl<'de> de::Visitor<'de> for KafkaCompactStrVisitor {
            type Value = KafkaCompactStr;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a compact Kafka string")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let varint_len = seq
                    .next_element_seed(VarintLenSeed)?
                    .ok_or_else(|| de::Error::custom("expected string length"))?;

                if varint_len == 0 {
                    return Err(de::Error::custom("null compact string is not allowed"));
                }

                let str_len = (varint_len - 1) as usize;
                let bytes = seq
                    .next_element_seed(ByteSeed::new(str_len))?
                    .ok_or_else(|| de::Error::custom("expected string bytes"))?;
                let s = String::from_utf8(bytes)
                    .map_err(|e| de::Error::custom(format!("invalid UTF-8 string: {}", e)))?;

                Ok(KafkaCompactStr(s))
            }
        }

        deserializer.deserialize_tuple(2, KafkaCompactStrVisitor)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KafkaNullableStr(Option<String>);

impl_as_ref!(KafkaNullableStr, Option<String>);

impl ser::Serialize for KafkaNullableStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self.as_ref() {
            Some(s) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                let len = s.len() as i16;
                seq.serialize_element(&len)?;
                seq.serialize_element(s.as_bytes())?;
                seq.end()
            }
            None => {
                let mut seq = serializer.serialize_seq(Some(1))?;
                let len: i16 = -1;
                seq.serialize_element(&len)?;
                seq.end()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KafkaCompactNullableStr(Option<String>);

impl_as_ref!(KafkaCompactNullableStr, Option<String>);

impl ser::Serialize for KafkaCompactNullableStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self.as_ref() {
            Some(s) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                let varint = util::encode_unsigned_varint(s.len() + 1);
                seq.serialize_element(&varint)?;
                seq.serialize_element(s.as_bytes())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{de::KafkaDeserializer, ser::KafkaSerializer};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestCompactStr {
        value: KafkaCompactStr,
    }

    #[test]
    fn test_kafka_compact_str_serialization() {
        let data = TestCompactStr {
            value: KafkaCompactStr("hello".to_string()),
        };
        let mut buffer: Vec<u8> = Vec::new();
        {
            let mut serializer = KafkaSerializer::new(0, &mut buffer);
            data.serialize(&mut serializer).unwrap();
        }
        assert_eq!(buffer, vec![6u8, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_kafka_compact_str_deserialization() {
        let data: Vec<u8> = vec![6u8, b'h', b'e', b'l', b'l', b'o'];
        let mut reader = &data[..];
        let mut deserializer = KafkaDeserializer::new(&mut reader);
        let result: TestCompactStr = Deserialize::deserialize(&mut deserializer).unwrap();
        assert_eq!(
            result,
            TestCompactStr {
                value: KafkaCompactStr("hello".to_string()),
            }
        );
    }
}
