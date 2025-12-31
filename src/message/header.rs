use super::util;
use crate::Result;
use std::io;

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
        let api_key = util::read_i16(reader)?;
        let api_version = util::read_i16(reader)?;
        let correlation_id = util::read_i32(reader)?;
        let client_id = util::read_string(reader)?;

        Ok(RequestHeader::V2 {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }

    pub fn api_key(&self) -> i16 {
        match self {
            RequestHeader::V2 { api_key, .. } => *api_key,
        }
    }

    pub fn api_version(&self) -> i16 {
        match self {
            RequestHeader::V2 { api_version, .. } => *api_version,
        }
    }

    pub fn collaration_id(&self) -> i32 {
        match self {
            RequestHeader::V2 { correlation_id, .. } => *correlation_id,
        }
    }

    pub fn client_id(&self) -> Option<&str> {
        match self {
            RequestHeader::V2 { client_id, .. } => client_id.as_deref(),
        }
    }
}
