mod api;
pub(crate) mod de;
mod error;
pub(crate) mod primitives;
pub(crate) mod ser;
pub(crate) mod util;

use ser::Serializer;
use serde::Serialize;

pub use error::KafkaError;
pub type Result<T> = std::result::Result<T, KafkaError>;

use std::io::{Read, Write};

pub fn handle_stream<S: Read + Write>(mut stream: S) -> Result<()> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf)?;

    let size = i32::from_be_bytes(size_buf);
    if size < 0 {
        return Err(KafkaError::DeserializationError(
            "negative frame size".to_string(),
        ));
    }

    let mut frame = vec![0u8; size as usize];
    stream.read_exact(&mut frame)?;

    let msg = api::handle(frame)?;

    let mut serializer = Serializer::new(&mut stream);
    msg.serialize(&mut serializer)?;

    Ok(())
}
