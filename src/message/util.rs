use crate::{KafkaError, Result};
use std::io;

pub(crate) fn read_i16<R: io::Read>(reader: &mut R) -> Result<i16> {
    let mut buf = [0u8; 2];
    reader.read_exact(&mut buf)?;
    Ok(i16::from_be_bytes(buf))
}

pub(crate) fn read_i32<R: io::Read>(reader: &mut R) -> Result<i32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

pub(crate) fn read_string<R: io::Read>(reader: &mut R) -> Result<Option<String>> {
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
