use crate::KafkaError;

use super::{Message, ResponseBody, ResponseHeader};
use serde::ser::{self, Serialize, SerializeSeq};
use std::io;

impl Serialize for ResponseHeader {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            ResponseHeader::V0 { collation_id } => serializer.serialize_i32(*collation_id),
        }
    }
}

impl Serialize for ResponseBody {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self {
            ResponseBody::ApiVersions => serializer.serialize_unit(),
        }
    }
}

impl Serialize for Message<ResponseHeader, ResponseBody> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        seq.serialize_element(&self.header)?;
        if let Some(body) = self.body.as_ref() {
            seq.serialize_element(body)?;
        }
        seq.end()
    }
}

impl Message<ResponseHeader, ResponseBody> {
    pub fn send<W: io::Write>(&self, writer: &mut W) -> Result<(), KafkaError> {
        let message_size = self.message_size();
        let mut serializer = BytesSerializer {
            message_size,
            writer,
        };
        self.serialize(&mut serializer)
    }
}

impl ser::Error for KafkaError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        KafkaError::SerializationError(msg.to_string())
    }
}

struct BytesSerializer<W> {
    message_size: i32,
    writer: W,
}

impl<'a, W: io::Write> ser::Serializer for &'a mut BytesSerializer<W> {
    type Ok = ();
    type Error = KafkaError;

    type SerializeSeq = BytesSerializeSeq<'a, W>;
    type SerializeTuple = BytesSerializeSeq<'a, W>;
    type SerializeTupleStruct = BytesSerializeSeq<'a, W>;
    type SerializeTupleVariant = BytesSerializeSeq<'a, W>;
    type SerializeMap = BytesSerializeSeq<'a, W>;
    type SerializeStruct = BytesSerializeSeq<'a, W>;
    type SerializeStructVariant = BytesSerializeSeq<'a, W>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        let byte = if v { 1u8 } else { 0u8 };
        self.writer.write_all(&[byte])?;
        Ok(())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support u8 serialization".into(),
        ))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support u64 serialization".into(),
        ))
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support f32 serialization".into(),
        ))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_char(self, c: char) -> Result<Self::Ok, Self::Error> {
        let mut buf = [0; 4];
        let s = c.encode_utf8(&mut buf);
        self.serialize_str(s)
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        let len = v.len() as i16;
        self.writer.write_all(&len.to_be_bytes())?;
        self.writer.write_all(v.as_bytes())?;
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let len = v.len() as i32;
        self.writer.write_all(&len.to_be_bytes())?;
        self.writer.write_all(v)?;
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        let len: i16 = -1;
        self.writer.write_all(&len.to_be_bytes())?;
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        // No bytes to write for unit type
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support unit struct serialization".into(),
        ))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support unit variant serialization".into(),
        ))
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(KafkaError::SerializationError(
            "Not support newtype struct serialization".into(),
        ))
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(KafkaError::SerializationError(
            "Not support newtype variant serialization".into(),
        ))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.writer.write_all(&self.message_size.to_be_bytes())?;
        Ok(BytesSerializeSeq { serializer: self })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support ser::SerializeMap".into(),
        ))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support ser::SerializeStruct".into(),
        ))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support ser::SerializeStructVariant".into(),
        ))
    }
}

struct BytesSerializeSeq<'a, W> {
    serializer: &'a mut BytesSerializer<W>,
}

impl<'a, W: io::Write> ser::SerializeSeq for BytesSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: io::Write> ser::SerializeTuple for BytesSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: io::Write> ser::SerializeTupleStruct for BytesSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: io::Write> ser::SerializeTupleVariant for BytesSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: io::Write> ser::SerializeMap for BytesSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(KafkaError::SerializationError(
            "Not support ser::SerializeMap".into(),
        ))
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(KafkaError::SerializationError(
            "Not support ser::SerializeMap".into(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support ser::SerializeMap".into(),
        ))
    }
}

impl<'a, W: io::Write> ser::SerializeStruct for BytesSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_field<T>(
        &mut self,
        _key: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(KafkaError::SerializationError(
            "Not support ser::SerializeStruct".into(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support ser::SerializeStruct".into(),
        ))
    }
}

impl<'a, W: io::Write> ser::SerializeStructVariant for BytesSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_field<T>(
        &mut self,
        _key: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + ser::Serialize,
    {
        Err(KafkaError::SerializationError(
            "Not support ser::SerializeStructVariatn".into(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Not support ser::SerializeStructVariatn".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ResponseBody, ResponseHeader};
    use super::*;

    #[test]
    fn test_serialize_response_header_v0() {
        let header = ResponseHeader::V0 { collation_id: 42 };
        let message = Message::<ResponseHeader, ResponseBody>::new(header, None);
        let mut buffer: Vec<u8> = Vec::new();
        let message_size = message.message_size();
        let mut serializer = BytesSerializer {
            message_size,
            writer: &mut buffer,
        };
        message.serialize(&mut serializer).unwrap();
        let expected_bytes: Vec<u8> = vec![
            0, 0, 0, 4, // message size
            0, 0, 0, 42, // collation_id
        ];
        assert_eq!(buffer, expected_bytes);
    }
}
