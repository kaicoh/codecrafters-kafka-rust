use crate::Result;
use serde::{Serialize, de, ser};
use std::io::Write;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Message<H, B>
where
    H: MessageHeader,
    B: MessageBody,
{
    pub(crate) header: H,
    pub(crate) body: Option<B>,
}

impl<H, B> Message<H, B>
where
    H: MessageHeader,
    B: MessageBody,
{
    pub(crate) fn new(header: H, body: Option<B>) -> Self {
        Self { header, body }
    }

    pub(crate) fn byte_size(&self) -> usize {
        let header_size = self.header.byte_size();
        let body_size = match self.body.as_ref() {
            Some(body) => body.byte_size(),
            None => 0,
        };
        header_size + body_size
    }

    pub fn api_key(&self) -> i16 {
        self.header.api_key()
    }

    pub fn api_version(&self) -> i16 {
        self.header.api_version()
    }

    pub fn correlation_id(&self) -> i32 {
        self.header.correlation_id()
    }

    pub fn send<W>(&self, writer: &mut W) -> Result<()>
    where
        W: Write,
    {
        let message_size = self.byte_size() as i32;
        writer.write_all(&message_size.to_be_bytes())?;

        let mut serializer = crate::ser::Serializer::new(writer);
        self.serialize(&mut serializer)?;

        Ok(())
    }
}

pub trait MessageHeader: ser::Serialize + de::DeserializeOwned {
    fn byte_size(&self) -> usize;
    fn api_key(&self) -> i16;
    fn api_version(&self) -> i16;
    fn correlation_id(&self) -> i32;
}

pub trait MessageBody: ser::Serialize + de::DeserializeOwned {
    fn byte_size(&self) -> usize;
}
