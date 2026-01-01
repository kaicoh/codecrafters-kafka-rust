// Ref: https://kafka.apache.org/41/design/protocol/#protocol-primitive-types

use crate::util;

use serde::{
    Serialize,
    ser::{self, SerializeSeq},
};

macro_rules! impl_as_ref {
    ($name:ty, $inner:ty) => {
        impl AsRef<$inner> for $name {
            fn as_ref(&self) -> &$inner {
                &self.0
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CompactString(String);

impl_as_ref!(CompactString, String);

impl ser::Serialize for CompactString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        let varint = util::encode_unsigned_varint(self.0.len() + 1);
        seq.serialize_element(&varint)?;
        seq.serialize_element(self.as_ref().as_bytes())?;
        seq.end()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct NullableString(Option<String>);

impl_as_ref!(NullableString, Option<String>);

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CompactNullableString(Option<String>);

impl_as_ref!(CompactNullableString, Option<String>);

impl ser::Serialize for CompactNullableString {
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Bytes(Vec<u8>);

impl_as_ref!(Bytes, [u8]);

impl ser::Serialize for Bytes {
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
pub(crate) struct CompactBytes(Vec<u8>);

impl_as_ref!(CompactBytes, [u8]);

impl ser::Serialize for CompactBytes {
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
pub(crate) struct NullableBytes(Option<Vec<u8>>);

impl_as_ref!(NullableBytes, Option<Vec<u8>>);

impl ser::Serialize for NullableBytes {
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
pub(crate) struct CompactNullableBytes(Option<Vec<u8>>);

impl_as_ref!(CompactNullableBytes, Option<Vec<u8>>);

impl ser::Serialize for CompactNullableBytes {
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Array<T>(Option<Vec<T>>);

impl<T> AsRef<Option<Vec<T>>> for Array<T> {
    fn as_ref(&self) -> &Option<Vec<T>> {
        &self.0
    }
}

impl<T> ser::Serialize for Array<T>
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
pub(crate) struct CompactArray<T>(Option<Vec<T>>);

impl<T> AsRef<Option<Vec<T>>> for CompactArray<T> {
    fn as_ref(&self) -> &Option<Vec<T>> {
        &self.0
    }
}

impl<T> ser::Serialize for CompactArray<T>
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
