use crate::{
    KafkaError, Result,
    de::Deserializer,
    types::{RecordBatch, RecordVariant},
};
use std::fs::File;
use std::io::Read;
use std::path::Path;

mod api_versions;
mod describe_topic_partitions;
mod fetch;
mod request;
mod response;

use request::{RequestHeaderV1, RequestHeaderV2};
use response::{ErrorCode, Message, ResponseBody, ResponseHeader};

const API_KEY_FETCH: i16 = 1;
const API_KEY_API_VERSIONS: i16 = 18;
const API_KEY_DESCRIBE_TOPIC_PARTITIONS: i16 = 75;

pub(crate) fn handle(bytes: Vec<u8>) -> Result<Message> {
    if bytes.len() < 4 {
        return Err(KafkaError::DeserializationError(
            "Request too short to contain correlation ID".to_string(),
        ));
    }

    let api_key: i16 = i16::from_be_bytes([bytes[0], bytes[1]]);
    let api_version: i16 = i16::from_be_bytes([bytes[2], bytes[3]]);

    route_request(api_key, api_version, Deserializer::new(&bytes[..]))
}

fn route_request<R: Read>(api_key: i16, api_version: i16, de: Deserializer<R>) -> Result<Message> {
    match api_key {
        API_KEY_FETCH => fetch::run(api_version, de),
        API_KEY_API_VERSIONS => api_versions::run(api_version, de),
        API_KEY_DESCRIBE_TOPIC_PARTITIONS => describe_topic_partitions::run(api_version, de),
        _ => Err(KafkaError::UnsupportedVersion {
            api_key,
            api_version,
        }),
    }
}

fn read_meta_records() -> Result<Vec<RecordVariant>> {
    read_meta("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
}

fn read_meta<P: AsRef<Path>>(path: P) -> Result<Vec<RecordVariant>> {
    let file = File::open(path)?;
    let records: Vec<RecordVariant> = RecordBatch::from_reader(file)?
        .into_iter()
        .flat_map(|record_batch| {
            record_batch
                .into_iter()
                .map(|record| record.value.into_inner().value)
        })
        .collect();
    Ok(records)
}
