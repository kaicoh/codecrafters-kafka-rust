mod error;
pub(crate) mod primitives;
pub(crate) mod ser;
pub(crate) mod util;

pub use error::{ErrorCode, KafkaError};
pub type Result<T> = std::result::Result<T, KafkaError>;
