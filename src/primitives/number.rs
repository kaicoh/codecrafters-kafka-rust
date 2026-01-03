use paste::paste;
use serde::ser;

macro_rules! kafka_num {
    ($(($name:ident, $ty:ty)),*) => {
        $(
            paste! {
                #[derive(Debug, Clone, PartialEq)]
                pub(crate) struct $name($ty);

                impl_as_ref!($name, $ty);

                impl ser::Serialize for $name {
                    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                    where
                        S: ser::Serializer,
                    {
                        serializer.[<serialize_ $ty>](*self.as_ref())
                    }
                }
            }
        )*
    };
}

kafka_num! {
    (KafkaInt8, i8),
    (KafkaInt16, i16),
    (KafkaInt32, i32),
    (KafkaInt64, i64),
    (KafkaUInt16, u16),
    (KafkaUInt32, u32),
    (KafkaFloat64, f64)
}
