use crate::{
    Result,
    de::Deserializer,
    types::{
        ByteSizeExt, CompactArray, CompactNullableBytes, CompactString, RecordVariant,
        TaggedFields, Uuid,
    },
};

use super::{
    API_KEY_FETCH, ErrorCode, Message, RequestHeaderV2, ResponseBody, ResponseHeader,
    read_meta_records,
};
use serde::{Deserialize, Serialize};
use std::io::Read;

pub(crate) fn run<R: Read>(api_version: i16, mut de: Deserializer<R>) -> Result<Message> {
    match api_version {
        16 => {
            let req_header: RequestHeaderV2 = Deserialize::deserialize(&mut de)?;
            let req_body: FetchRequestBody = Deserialize::deserialize(&mut de)?;

            let res_header = ResponseHeader::V1 {
                correlation_id: req_header.correlation_id,
                tagged_fields: TaggedFields::new(None),
            };

            let topics = req_body.topics.as_opt_slice();

            if topics.is_none() || topics.is_some_and(|t| t.is_empty()) {
                let res_body = ResponseBody::Fetch(FetchResponseBody {
                    throttle_time_ms: 0,
                    error_code: ErrorCode::NoError,
                    session_id: 0,
                    responses: CompactArray::new(Some(vec![])),
                    tagged_fields: TaggedFields::new(None),
                });
                return Ok(Message::new(res_header, Some(res_body)));
            }

            let records = read_meta_records()?;

            let responses = req_body
                .topics
                .into_iter()
                .map(|topic| FetchResponseTopic {
                    id: topic.id,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(make_response(topic.id, records.as_slice()))
                        .collect(),
                    tagged_fields: TaggedFields::new(None),
                })
                .collect();

            let res_body = ResponseBody::Fetch(FetchResponseBody {
                throttle_time_ms: 0,
                error_code: ErrorCode::NoError,
                session_id: 0,
                responses,
                tagged_fields: TaggedFields::new(None),
            });

            Ok(Message::new(res_header, Some(res_body)))
        }
        _ => Err(crate::KafkaError::UnsupportedVersion {
            api_key: API_KEY_FETCH,
            api_version,
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct FetchRequestBody {
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    session_id: i32,
    session_epoch: i32,
    topics: CompactArray<FetchRequestTopic>,
    forgotten_topics: CompactArray<ForgottenTopic>,
    rack_id: CompactString,
    tagged_fields: TaggedFields,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct FetchRequestTopic {
    id: Uuid,
    partitions: CompactArray<FetchRequestPartition>,
    tagged_fields: TaggedFields,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct FetchRequestPartition {
    partition_index: i32,
    current_leader_epoch: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    log_start_offset: i64,
    partition_max_bytes: i32,
    tagged_fields: TaggedFields,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct ForgottenTopic {
    id: Uuid,
    partitions: CompactArray<i32>,
    tagged_fields: TaggedFields,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FetchResponseBody {
    throttle_time_ms: i32,
    error_code: ErrorCode,
    session_id: i32,
    responses: CompactArray<FetchResponseTopic>,
    tagged_fields: TaggedFields,
}

impl ByteSizeExt for FetchResponseBody {
    fn byte_size(&self) -> usize {
        self.throttle_time_ms.byte_size()
            + self.error_code.byte_size()
            + self.session_id.byte_size()
            + self.responses.byte_size()
            + self.tagged_fields.byte_size()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FetchResponseTopic {
    id: Uuid,
    partitions: CompactArray<FetchResponsePartition>,
    tagged_fields: TaggedFields,
}

impl ByteSizeExt for FetchResponseTopic {
    fn byte_size(&self) -> usize {
        self.id.byte_size() + self.partitions.byte_size() + self.tagged_fields.byte_size()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct FetchResponsePartition {
    partition_index: i32,
    error_code: ErrorCode,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: CompactArray<AbortedTransaction>,
    preferred_read_replica: i32,
    records: CompactNullableBytes,
    tagged_fields: TaggedFields,
}

impl ByteSizeExt for FetchResponsePartition {
    fn byte_size(&self) -> usize {
        self.partition_index.byte_size()
            + self.error_code.byte_size()
            + self.high_watermark.byte_size()
            + self.last_stable_offset.byte_size()
            + self.log_start_offset.byte_size()
            + self.aborted_transactions.byte_size()
            + self.preferred_read_replica.byte_size()
            + self.records.byte_size()
            + self.tagged_fields.byte_size()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct AbortedTransaction {
    producer_id: i64,
    first_offset: i64,
    tagged_fields: TaggedFields,
}

impl ByteSizeExt for AbortedTransaction {
    fn byte_size(&self) -> usize {
        self.producer_id.byte_size()
            + self.first_offset.byte_size()
            + self.tagged_fields.byte_size()
    }
}

fn make_response(
    topic_id: Uuid,
    slice: &[RecordVariant],
) -> Box<dyn Fn(FetchRequestPartition) -> FetchResponsePartition + '_> {
    Box::new(move |partition: FetchRequestPartition| {
        match slice.iter().find(|record| {
            if let RecordVariant::Partition(p) = record {
                p.partition_id == partition.partition_index && p.topic_id == topic_id
            } else {
                false
            }
        }) {
            Some(RecordVariant::Partition(_)) => FetchResponsePartition {
                partition_index: partition.partition_index,
                error_code: ErrorCode::NoError,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_transactions: CompactArray::new(None),
                preferred_read_replica: -1,
                records: CompactNullableBytes::new(Some(vec![])),
                tagged_fields: TaggedFields::new(None),
            },
            _ => FetchResponsePartition {
                partition_index: partition.partition_index,
                error_code: ErrorCode::UnknownTopicId,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_transactions: CompactArray::new(None),
                preferred_read_replica: 0,
                records: CompactNullableBytes::new(None),
                tagged_fields: TaggedFields::new(None),
            },
        }
    })
}
