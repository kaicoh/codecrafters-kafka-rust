mod ser;

use crate::{KafkaError, Result};
use std::io;

#[derive(Debug, Clone, PartialEq)]
pub struct Message<H: Header> {
    header: H,
}

impl<H: Header> Message<H> {
    pub fn new(header: H) -> Self {
        Message { header }
    }

    fn message_size(&self) -> i32 {
        self.header.byte_size()
    }
}

impl Message<RequestHeader> {
    pub fn from_reader<R: io::Read>(reader: &mut R) -> Result<Self> {
        let _size = read_i32(reader)?;
        let header = RequestHeader::from_reader(reader)?;
        Ok(Message::new(header))
    }

    pub fn collaration_id(&self) -> i32 {
        self.header.collaration_id()
    }
}

pub trait Header {
    fn byte_size(&self) -> i32;
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseHeader {
    V0 { collation_id: i32 },
}

impl ResponseHeader {
    pub fn new_v0(correlation_id: i32) -> Self {
        ResponseHeader::V0 {
            collation_id: correlation_id,
        }
    }
}

impl Header for ResponseHeader {
    fn byte_size(&self) -> i32 {
        match self {
            ResponseHeader::V0 { .. } => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RequestHeader {
    V2 {
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
    },
}

impl Header for RequestHeader {
    fn byte_size(&self) -> i32 {
        match self {
            RequestHeader::V2 { client_id, .. } => {
                let client_id_size = match client_id {
                    Some(id) => 2 + id.len() as i32, // 2 bytes for length prefix
                    None => 2,                       // 2 bytes for -1 length
                };
                2 + 2 + 4 + client_id_size // api_key + api_version + correlation_id + client_id
            }
        }
    }
}

impl RequestHeader {
    pub fn from_reader<R: io::Read>(reader: &mut R) -> Result<Self> {
        let api_key = read_i16(reader)?;
        let api_version = read_i16(reader)?;
        let correlation_id = read_i32(reader)?;
        let client_id = read_string(reader)?;

        Ok(RequestHeader::V2 {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }

    pub fn collaration_id(&self) -> i32 {
        match self {
            RequestHeader::V2 { correlation_id, .. } => *correlation_id,
        }
    }
}

fn read_i16<R: io::Read>(reader: &mut R) -> Result<i16> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf)?;
    Ok(i16::from_be_bytes(buf))
}

fn read_i32<R: io::Read>(reader: &mut R) -> Result<i32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_string<R: io::Read>(reader: &mut R) -> Result<Option<String>> {
    let length = read_i16(reader)?;
    if length < 0 {
        return Ok(None);
    }
    let mut buf = vec![0u8; length as usize];
    reader.read_exact(&mut buf)?;
    let string =
        String::from_utf8(buf).map_err(|e| KafkaError::DeserializationError(e.to_string()))?;
    Ok(Some(string))
}
