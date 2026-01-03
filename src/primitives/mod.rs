// Ref: https://kafka.apache.org/41/design/protocol/#protocol-primitive-types
macro_rules! impl_as_ref {
    ($name:ty, $inner:ty) => {
        impl AsRef<$inner> for $name {
            fn as_ref(&self) -> &$inner {
                &self.0
            }
        }
    };
}

mod array;
mod bytes;
mod string;

pub(crate) use array::*;
pub(crate) use bytes::*;
pub(crate) use string::*;
