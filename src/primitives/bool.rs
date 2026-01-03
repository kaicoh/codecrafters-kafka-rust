use serde::ser;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct KafkaBool(bool);

impl_as_ref!(KafkaBool, bool);

impl ser::Serialize for KafkaBool {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        let byte = if *self.as_ref() { 1u8 } else { 0u8 };
        serializer.serialize_u8(byte)
    }
}
