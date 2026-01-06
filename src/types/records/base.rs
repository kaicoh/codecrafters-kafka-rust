use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct RecordBatch {
    pub(crate) base_offset: i64,
    pub(crate) batch_length: i32,
    pub(crate) partition_leader_epoch: i32,
    pub(crate) magic: u8,
    pub(crate) crc: u32,
    pub(crate) attributes: i16,
    pub(crate) last_offset_delta: i32,
    pub(crate) first_timestamp: i64,
    pub(crate) max_timestamp: i64,
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
    pub(crate) base_sequence: i32,
    pub(crate) records: Array<Record>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Record {
    pub(crate) length: Varint,
    pub(crate) attributes: u8,
    pub(crate) timestamp_delta: VarLong,
    pub(crate) offset_delta: Varint,
    pub(crate) key: VarintBytes,
    pub(crate) value: Value,
    pub(crate) headers: CompactArray<Header>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Header {
    pub(crate) key: VarintString,
    pub(crate) value: VarintBytes,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum Value {}
