use std::fmt;

use base64::{engine::general_purpose::STANDARD, Engine};
use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserializer, Serializer};

pub(crate) fn serialize<Output: Serializer>(
    bytes: &[u8],
    output: Output,
) -> Result<Output::Ok, Output::Error> {
    output.serialize_str(&STANDARD.encode(bytes))
}

pub(crate) fn deserialize<'input, Input: Deserializer<'input>>(
    input: Input,
) -> Result<Vec<u8>, Input::Error> {
    struct Payload;

    impl<'input> Visitor<'input> for Payload {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("base64 payload or legacy byte array")
        }

        fn visit_str<Failure: Error>(self, encoded: &str) -> Result<Self::Value, Failure> {
            STANDARD.decode(encoded).map_err(Failure::custom)
        }

        fn visit_seq<Sequence: SeqAccess<'input>>(
            self,
            mut sequence: Sequence,
        ) -> Result<Self::Value, Sequence::Error> {
            let mut bytes = Vec::new();
            while let Some(byte) = sequence.next_element::<u8>()? {
                bytes.push(byte);
            }
            Ok(bytes)
        }
    }

    input.deserialize_any(Payload)
}
