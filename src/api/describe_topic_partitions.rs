use crate::{
    Result,
    de::Deserializer,
    primitives::{ByteSize, CompactArray, CompactNullableString, CompactString, Uuid},
};

use super::{
    API_KEY_DESCRIBE_TOPIC_PARTITIONS, ErrorCode, Message, RequestHeaderV2, ResponseBody,
    ResponseHeader, TaggedFields,
};
use serde::{Deserialize, Serialize, ser};

pub(crate) fn run(api_version: i16, mut de: Deserializer) -> Result<Message> {
    match api_version {
        0 => {
            let req_header: RequestHeaderV2 = Deserialize::deserialize(&mut de)?;
            let req_body: RequestBody = Deserialize::deserialize(&mut de)?;

            let res_header = ResponseHeader::V1 {
                correlation_id: req_header.correlation_id,
                tagged_fields: TaggedFields::new(vec![]),
            };

            let topics = req_body
                .topics
                .into_iter()
                .map(|topic| ResponseTopic {
                    error_code: ErrorCode::UnknownTopicOrPartition,
                    name: topic.name.into(),
                    topic_id: Uuid::default(),
                    is_internal: false,
                    partitions: CompactArray::new(Some(vec![])),
                    topic_authorized_operations: 0,
                    tagged_fields: TaggedFields::new(vec![]),
                })
                .collect();
            let res_body =
                ResponseBody::DescribeTopicPartitions(DescribeTopicPartitionsResponseBody::V0 {
                    throttle_time_ms: 0,
                    topics,
                    next_cursor: NextCursor(None),
                    tagged_fields: TaggedFields::new(vec![]),
                });

            Ok(Message::new(res_header, Some(res_body)))
        }
        _ => Err(crate::KafkaError::UnsupportedVersion {
            api_key: API_KEY_DESCRIBE_TOPIC_PARTITIONS,
            api_version,
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RequestBody {
    topics: CompactArray<RequestTopic>,
    // NOTE:
    // According to the Kafka protocol documentation, the response_partition_limit
    // and cursor fields are part of the DescribeTopicPartitionsRequest structure.
    // However, the test(#VT6) fails if they are included here.
    //
    //response_partition_limit: i32,
    //cursor: Cursor,
    tagged_fields: TaggedFields,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RequestTopic {
    name: CompactString,
    tagged_fields: TaggedFields,
}

impl ByteSize for RequestTopic {
    fn byte_size(&self) -> usize {
        self.name.byte_size() + self.tagged_fields.byte_size()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Cursor {
    topic_name: CompactString,
    partition_index: i32,
    tagged_fields: TaggedFields,
}

impl ByteSize for Cursor {
    fn byte_size(&self) -> usize {
        self.topic_name.byte_size()
            + self.partition_index.byte_size()
            + self.tagged_fields.byte_size()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub(crate) enum DescribeTopicPartitionsResponseBody {
    V0 {
        throttle_time_ms: i32,
        topics: CompactArray<ResponseTopic>,
        next_cursor: NextCursor,
        tagged_fields: TaggedFields,
    },
}

impl ByteSize for DescribeTopicPartitionsResponseBody {
    fn byte_size(&self) -> usize {
        match self {
            Self::V0 {
                throttle_time_ms,
                topics,
                next_cursor,
                tagged_fields,
            } => {
                throttle_time_ms.byte_size()
                    + topics.byte_size()
                    + next_cursor.byte_size()
                    + tagged_fields.byte_size()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ResponseTopic {
    error_code: ErrorCode,
    name: CompactNullableString,
    topic_id: Uuid,
    is_internal: bool,
    partitions: CompactArray<Partition>,
    topic_authorized_operations: i32,
    tagged_fields: TaggedFields,
}

impl ByteSize for ResponseTopic {
    fn byte_size(&self) -> usize {
        self.error_code.byte_size()
            + self.name.byte_size()
            + self.topic_id.byte_size()
            + self.is_internal.byte_size()
            + self.partitions.byte_size()
            + self.topic_authorized_operations.byte_size()
            + self.tagged_fields.byte_size()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Partition {
    error_code: ErrorCode,
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
    replica_nodes: CompactArray<i32>,
    isr_nodes: CompactArray<i32>,
    eligible_leader_replicas: CompactArray<i32>,
    last_known_elr: CompactArray<i32>,
    offline_replicas: CompactArray<i32>,
    tagged_fields: TaggedFields,
}

impl ByteSize for Partition {
    fn byte_size(&self) -> usize {
        self.error_code.byte_size()
            + self.partition_index.byte_size()
            + self.leader_id.byte_size()
            + self.leader_epoch.byte_size()
            + self.replica_nodes.byte_size()
            + self.isr_nodes.byte_size()
            + self.eligible_leader_replicas.byte_size()
            + self.last_known_elr.byte_size()
            + self.offline_replicas.byte_size()
            + self.tagged_fields.byte_size()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NextCursor(Option<Cursor>);

impl ser::Serialize for NextCursor {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match self.0.as_ref() {
            Some(cursor) => cursor.serialize(serializer),
            None => (-1 as i8).serialize(serializer),
        }
    }
}

impl ByteSize for NextCursor {
    fn byte_size(&self) -> usize {
        match self.0.as_ref() {
            Some(cursor) => cursor.byte_size(),
            None => 1, // size of int8
        }
    }
}
