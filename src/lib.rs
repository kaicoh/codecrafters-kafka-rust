mod error;
mod message;

pub use error::KafkaError;
pub use message::{Header, Message, RequestHeader, ResponseHeader};

pub type Result<T> = std::result::Result<T, KafkaError>;
