use bytes::{Bytes, BytesMut};
use std::iter::Extend;

#[derive(Debug, Clone)]
pub struct Message {
    header: Header,
}

impl Message {
    pub fn new(header: Header) -> Self {
        Message { header }
    }

    fn message_size(&self) -> i32 {
        self.header.byte_size() as i32
    }
}

impl Into<Bytes> for Message {
    fn into(self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(4 + self.message_size() as usize);
        bytes.extend(&self.message_size().to_be_bytes());
        bytes.extend(Into::<Bytes>::into(self.header));
        bytes.freeze()
    }
}

#[derive(Debug, Clone)]
pub enum Header {
    V0 { collation_id: i32 },
}

impl Header {
    pub fn new_v0(collation_id: i32) -> Self {
        Header::V0 { collation_id }
    }

    fn byte_size(&self) -> usize {
        match self {
            Header::V0 { .. } => 4,
        }
    }
}

impl Into<Bytes> for Header {
    fn into(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.byte_size());
        match self {
            Header::V0 { collation_id } => {
                buf.extend_from_slice(&collation_id.to_be_bytes());
            }
        }
        Bytes::from(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_serialization() {
        let header = Header::V0 { collation_id: 7 };
        let message = Message { header };
        let bytes: Bytes = message.into();
        let expected = Bytes::from_static(&[
            0, 0, 0, 4, // message size (4 bytes for collation_id)
            0, 0, 0, 7, // collation_id
        ]);
        assert_eq!(bytes, expected);
    }
}
