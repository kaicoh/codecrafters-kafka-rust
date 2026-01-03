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

pub(crate) trait PrimitiveExt {
    fn byte_size(&self) -> usize;
}

impl PrimitiveExt for bool {
    fn byte_size(&self) -> usize {
        1
    }
}

impl PrimitiveExt for i8 {
    fn byte_size(&self) -> usize {
        1
    }
}

impl PrimitiveExt for i16 {
    fn byte_size(&self) -> usize {
        2
    }
}

impl PrimitiveExt for i32 {
    fn byte_size(&self) -> usize {
        4
    }
}

impl PrimitiveExt for i64 {
    fn byte_size(&self) -> usize {
        8
    }
}

impl PrimitiveExt for u8 {
    fn byte_size(&self) -> usize {
        1
    }
}

impl PrimitiveExt for u16 {
    fn byte_size(&self) -> usize {
        2
    }
}

impl PrimitiveExt for u32 {
    fn byte_size(&self) -> usize {
        4
    }
}

impl PrimitiveExt for f64 {
    fn byte_size(&self) -> usize {
        8
    }
}

impl PrimitiveExt for String {
    fn byte_size(&self) -> usize {
        2 + self.len()
    }
}
