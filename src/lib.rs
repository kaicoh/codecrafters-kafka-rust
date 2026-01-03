pub(crate) mod de;
mod error;
pub mod message;
pub(crate) mod primitives;
pub(crate) mod ser;
pub(crate) mod util;

pub use error::{ErrorCode, KafkaError};
pub use message::Message;
pub type Result<T> = std::result::Result<T, KafkaError>;
