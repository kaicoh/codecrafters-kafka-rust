mod de;
mod ser;

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
