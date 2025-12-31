use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("Serialization Error: {0}")]
    SerializationError(String),

    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
}
