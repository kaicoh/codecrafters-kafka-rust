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
