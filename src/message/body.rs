use crate::Result;
use std::io;

pub trait Body {
    fn byte_size(&self) -> i32;
}

#[derive(Debug, Clone, PartialEq)]
pub enum RequestBody {
    ApiVersions,
}

impl Body for RequestBody {
    fn byte_size(&self) -> i32 {
        match self {
            RequestBody::ApiVersions => 0,
        }
    }
}

impl RequestBody {
    pub fn from_reader<R: io::Read>(
        _reader: &mut R,
        _api_key: i16,
        _api_version: i16,
    ) -> Result<Option<Self>> {
        //match api_key {
        //    18 => Ok(RequestBody::ApiVersions),
        //    _ => Err(KafkaError::DeserializationError(format!(
        //        "Unsupported api_key for RequestBody: {}",
        //        api_key
        //    ))),
        //}
        unimplemented!()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResponseBody {
    ApiVersions,
}

impl Body for ResponseBody {
    fn byte_size(&self) -> i32 {
        match self {
            ResponseBody::ApiVersions => 0,
        }
    }
}
