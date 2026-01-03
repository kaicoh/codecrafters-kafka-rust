use crate::KafkaError;

use serde::ser;
use std::io;

impl ser::Error for KafkaError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        KafkaError::SerializationError(msg.to_string())
    }
}

pub(crate) struct KafkaSerializer<W: io::Write> {
    message_size: i32,
    writer: W,
}

impl<W: io::Write> KafkaSerializer<W> {
    pub(crate) fn new(message_size: i32, writer: W) -> Self {
        KafkaSerializer {
            message_size,
            writer,
        }
    }
}

impl<'a, W: io::Write> ser::Serializer for &'a mut KafkaSerializer<W> {
    type Ok = ();
    type Error = KafkaError;

    type SerializeSeq = KafkaSerializeSeq<'a, W>;
    type SerializeTuple = KafkaSerializeSeq<'a, W>;
    type SerializeTupleStruct = KafkaSerializeSeq<'a, W>;
    type SerializeTupleVariant = KafkaSerializeSeq<'a, W>;
    type SerializeMap = KafkaSerializeSeq<'a, W>;
    type SerializeStruct = KafkaSerializeSeq<'a, W>;
    type SerializeStructVariant = KafkaSerializeSeq<'a, W>;

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

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(&[v])?;
        Ok(())
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
            "u64 serialization is not supported".to_string(),
        ))
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "f32 serialization is not supported".to_string(),
        ))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(&v.to_be_bytes())?;
        Ok(())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        let mut buf = [0u8; 4];
        let s = v.encode_utf8(&mut buf);
        self.serialize_str(s)
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        let str_bytes = v.as_bytes();
        let str_len = str_bytes.len() as i16;
        self.serialize_i16(str_len)?;
        self.writer.write_all(str_bytes)?;
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.writer.write_all(v)?;
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_i16(-1)
    }

    fn serialize_some<T: ?Sized + ser::Serialize>(
        self,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Unit serialization is not supported".to_string(),
        ))
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Unit struct serialization is not supported".to_string(),
        ))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Unit variant serialization is not supported".to_string(),
        ))
    }

    fn serialize_newtype_struct<T: ?Sized + ser::Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + ser::Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        Err(KafkaError::SerializationError(
            "Newtype variant serialization is not supported".to_string(),
        ))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(KafkaSerializeSeq { serializer: self })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(KafkaSerializeSeq { serializer: self })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(KafkaSerializeSeq { serializer: self })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(KafkaSerializeSeq { serializer: self })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(KafkaSerializeSeq { serializer: self })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(KafkaSerializeSeq { serializer: self })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(KafkaSerializeSeq { serializer: self })
    }
}

pub struct KafkaSerializeSeq<'a, W: io::Write> {
    serializer: &'a mut KafkaSerializer<W>,
}

impl<'a, W: io::Write> ser::SerializeSeq for KafkaSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_element<T: ?Sized + ser::Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: io::Write> ser::SerializeTuple for KafkaSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_element<T: ?Sized + ser::Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: io::Write> ser::SerializeTupleStruct for KafkaSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_field<T: ?Sized + ser::Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: io::Write> ser::SerializeTupleVariant for KafkaSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_field<T: ?Sized + ser::Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: io::Write> ser::SerializeMap for KafkaSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_key<T: ?Sized + ser::Serialize>(&mut self, _key: &T) -> Result<(), Self::Error> {
        Ok(())
    }

    fn serialize_value<T: ?Sized + ser::Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: io::Write> ser::SerializeStruct for KafkaSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_field<T: ?Sized + ser::Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: io::Write> ser::SerializeStructVariant for KafkaSerializeSeq<'a, W> {
    type Ok = ();
    type Error = KafkaError;

    fn serialize_field<T: ?Sized + ser::Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(&mut *self.serializer)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
