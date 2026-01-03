use crate::util;
use serde::ser::{self, SerializeSeq};

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
