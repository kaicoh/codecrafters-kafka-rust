mod error;
mod message;

pub use error::KafkaError;
pub use message::{
    Body, Header, Message, RequestBody, RequestHeader, ResponseBody, ResponseHeader,
};

pub type Result<T> = std::result::Result<T, KafkaError>;
pub type Request = Message<RequestHeader, RequestBody>;
pub type Response = Message<ResponseHeader, ResponseBody>;
