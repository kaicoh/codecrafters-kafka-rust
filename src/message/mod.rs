mod body;
mod header;
mod ser;
mod util;

use crate::Result;
use std::io;

pub use body::{Body, RequestBody, ResponseBody};
pub use header::{Header, RequestHeader, ResponseHeader};

#[derive(Debug, Clone, PartialEq)]
pub struct Message<H: Header, B: Body> {
    header: H,
    body: Option<B>,
}

impl<H: Header, B: Body> Message<H, B> {
    pub fn new(header: H, body: Option<B>) -> Self {
        Message { header, body }
    }

    fn message_size(&self) -> i32 {
        self.header.byte_size() + self.body.as_ref().map_or(0, |b| b.byte_size())
    }
}

impl<B: Body> Message<RequestHeader, B> {
    pub fn collaration_id(&self) -> i32 {
        self.header.collaration_id()
    }
}

impl Message<RequestHeader, RequestBody> {
    pub fn from_reader<R: io::Read>(reader: &mut R) -> Result<Self> {
        let _size = util::read_i32(reader)?;
        let header = RequestHeader::from_reader(reader)?;
        let body = RequestBody::from_reader(reader, header.api_key(), header.api_version())?;
        Ok(Message::new(header, body))
    }
}
