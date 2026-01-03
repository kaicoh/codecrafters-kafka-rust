use crate::util;
use serde::ser::{self, SerializeSeq};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KafkaArray<T>(Option<Vec<T>>);

impl<T> AsRef<Option<Vec<T>>> for KafkaArray<T> {
    fn as_ref(&self) -> &Option<Vec<T>> {
        &self.0
    }
}

impl<T> ser::Serialize for KafkaArray<T>
where
    T: ser::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self.as_ref() {
            Some(vec) => {
                let mut seq = serializer.serialize_seq(Some(1 + vec.len()))?;
                let len = vec.len() as i32;
                seq.serialize_element(&len)?;
                for item in vec {
                    seq.serialize_element(item)?;
                }
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
pub(crate) struct KafkaCompactArray<T>(Option<Vec<T>>);

impl<T> AsRef<Option<Vec<T>>> for KafkaCompactArray<T> {
    fn as_ref(&self) -> &Option<Vec<T>> {
        &self.0
    }
}

impl<T> ser::Serialize for KafkaCompactArray<T>
where
    T: ser::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self.as_ref() {
            Some(vec) => {
                let mut seq = serializer.serialize_seq(Some(1 + vec.len()))?;
                let varint = util::encode_unsigned_varint(vec.len() + 1);
                seq.serialize_element(&varint)?;
                for item in vec {
                    seq.serialize_element(item)?;
                }
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
