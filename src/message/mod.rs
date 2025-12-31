mod body;
mod header;
mod ser;
mod util;

use crate::Result;
use std::io;

pub use header::{Header, RequestHeader, ResponseHeader};

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
        let _size = util::read_i32(reader)?;
        let header = RequestHeader::from_reader(reader)?;
        Ok(Message::new(header))
    }

    pub fn collaration_id(&self) -> i32 {
        self.header.collaration_id()
    }
}
