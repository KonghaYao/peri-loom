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

/// The same convention for a list of bodies. A batched field cannot borrow the
/// scalar adapter, and leaving it as `Vec<Vec<u8>>` would put a multi-megabyte
/// batch on the wire as an array of integer arrays.
pub(crate) mod batch {
    use std::fmt;

    use serde::de::{DeserializeSeed, Deserializer, SeqAccess, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Serialize, Serializer};

    pub(crate) fn serialize<Output: Serializer>(
        segments: &[Vec<u8>],
        output: Output,
    ) -> Result<Output::Ok, Output::Error> {
        let mut sequence = output.serialize_seq(Some(segments.len()))?;
        for segment in segments {
            sequence.serialize_element(&Encoded(segment))?;
        }
        sequence.end()
    }

    struct Encoded<'body>(&'body [u8]);

    impl Serialize for Encoded<'_> {
        fn serialize<Output: Serializer>(
            &self,
            output: Output,
        ) -> Result<Output::Ok, Output::Error> {
            super::serialize(self.0, output)
        }
    }

    pub(crate) fn deserialize<'input, Input: Deserializer<'input>>(
        input: Input,
    ) -> Result<Vec<Vec<u8>>, Input::Error> {
        struct Batch;

        impl<'input> Visitor<'input> for Batch {
            type Value = Vec<Vec<u8>>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("array of base64 payloads or legacy byte arrays")
            }

            fn visit_seq<Sequence: SeqAccess<'input>>(
                self,
                mut sequence: Sequence,
            ) -> Result<Self::Value, Sequence::Error> {
                let mut segments = Vec::new();
                while let Some(segment) = sequence.next_element_seed(Element)? {
                    segments.push(segment);
                }
                Ok(segments)
            }
        }

        input.deserialize_seq(Batch)
    }

    struct Element;

    impl<'input> DeserializeSeed<'input> for Element {
        type Value = Vec<u8>;

        fn deserialize<Input: Deserializer<'input>>(
            self,
            input: Input,
        ) -> Result<Self::Value, Input::Error> {
            super::deserialize(input)
        }
    }
}
