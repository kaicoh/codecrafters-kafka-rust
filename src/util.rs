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
    let string = String::from_utf8(buf)?;
    Ok(Some(string))
}

pub(crate) fn read_compact_string<R: io::Read>(reader: &mut R) -> Result<String> {
    let len = unsigned_varint(reader)? as usize;
    let mut buf = vec![0u8; len - 1]; // Subtract 1 for the null terminator
    reader.read_exact(&mut buf)?;
    let string = String::from_utf8(buf)?;
    Ok(string)
}

pub(crate) fn compact_string_size(s: &str) -> usize {
    let str_len = s.len();
    let len_bytes = encode_unsigned_varint(str_len + 1); // +1 for null terminator
    len_bytes.len() + str_len
}

pub(crate) fn unsigned_varint<R: io::Read>(reader: &mut R) -> Result<u64> {
    let mut bytes: Vec<u8> = Vec::new();

    for byte in LengthBytes::new(reader) {
        let byte = byte?;
        bytes.push(byte);
    }

    let mut len: u64 = bytes.pop().ok_or_else(|| {
        KafkaError::DeserializationError("Failed to read unsinged varint length".to_string())
    })? as u64;

    while let Some(byte) = bytes.pop() {
        len = (len << 7) | (byte as u64);
    }

    Ok(len)
}

struct LengthBytes<'a, R: io::Read + 'a> {
    reader: &'a mut R,
    finished: bool,
}

impl<'a, R: io::Read + 'a> LengthBytes<'a, R> {
    pub fn new(reader: &'a mut R) -> Self {
        LengthBytes {
            reader,
            finished: false,
        }
    }
}

impl<'a, R: io::Read + 'a> Iterator for LengthBytes<'a, R> {
    type Item = Result<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let mut buf = [0u8; 1];
        let byte = match self.reader.read_exact(&mut buf) {
            Ok(_) => buf[0],
            Err(e) => {
                self.finished = true;
                return Some(Err(KafkaError::IoError(e)));
            }
        };

        if byte & 0b1000_0000 == 0 {
            self.finished = true;
        }

        Some(Ok(byte & 0b0111_1111))
    }
}

pub(crate) fn encode_unsigned_varint(length: usize) -> Vec<u8> {
    let mut len = length;
    let mut bytes = Vec::new();

    loop {
        let mut byte = (len & 0b0111_1111) as u8;
        len >>= 7;
        if len > 0 {
            byte |= 0b1000_0000; // Set continuation bit
        }
        bytes.push(byte);
        if len == 0 {
            break;
        }
    }

    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_length_bytes() {
        let data = vec![0b1000_0001, 0b0000_0010];
        let mut reader: &[u8] = &data;
        let mut generator = LengthBytes::new(&mut reader);
        assert_eq!(generator.next().unwrap().unwrap(), 0b0000_0001);
        assert_eq!(generator.next().unwrap().unwrap(), 0b0000_0010);
        assert!(generator.next().is_none());
    }

    #[test]
    fn test_unsigned_varint() {
        let data = vec![0b1001_0110, 0b0000_0001];
        let mut reader: &[u8] = &data;
        let length = unsigned_varint(&mut reader).unwrap();
        assert_eq!(length, 150);

        let data = vec![0b0000_0110];
        let mut reader: &[u8] = &data;
        let length = unsigned_varint(&mut reader).unwrap();
        assert_eq!(length, 6);
    }

    #[test]
    fn test_encode_unsigned_varint() {
        let length = 150;
        let encoded = encode_unsigned_varint(length);
        assert_eq!(encoded, vec![0b1001_0110, 0b0000_0001]);

        let length = 257;
        let encoded = encode_unsigned_varint(length);
        assert_eq!(encoded, vec![0b1000_0001, 0b0000_0010]);

        let length = 6;
        let encoded = encode_unsigned_varint(length);
        assert_eq!(encoded, vec![0b0000_0110]);
    }
}
