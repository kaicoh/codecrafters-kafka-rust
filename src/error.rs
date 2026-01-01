use serde::ser::{Serialize, Serializer};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("Serialization Error: {0}")]
    SerializationError(String),

    #[error("Deserialization Error: {0}")]
    DeserializationError(String),

    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
}

impl KafkaError {
    pub fn error_code(&self) -> ErrorCode {
        match self {
            KafkaError::DeserializationError(_) => ErrorCode::CorruptMessage,
            _ => ErrorCode::UnknownServerError,
        }
    }
}

impl From<std::string::FromUtf8Error> for KafkaError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        KafkaError::DeserializationError(err.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ErrorCode {
    UnknownServerError = -1,
    NoError = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageSizeTooLarge = 10,
    StaleControllerEpochCode = 11,
    OffsetMetadataTooLargeCode = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinatorForConsumerCode = 16,
    InvalidTopicException = 17,
    RecordListTooLargeException = 18,
    NotEnoughReplicasException = 19,
    NotEnoughReplicasAfterAppendException = 20,
    InvalidRequiredAcksException = 21,
    IllegalGenerationException = 22,
    InconsistentGroupProtocolException = 23,
    InvalidGroupIdException = 24,
    UnknownMemberIdException = 25,
    InvalidSessionTimeoutException = 26,
    RebalanceInProgressException = 27,
    InvalidCommitOffsetSizeException = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
}

impl Serialize for ErrorCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i16(*self as i16)
    }
}
