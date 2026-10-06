use anyhow::{ensure, Result};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(super) fn sha256(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

pub(super) fn encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

pub(super) fn decode(value: &str) -> Result<String> {
    let mut bytes = Vec::new();
    let mut remaining = value.as_bytes();
    while let Some((&first, rest)) = remaining.split_first() {
        if first == b'%' {
            ensure!(rest.len() >= 2, "invalid URL encoding");
            bytes.push(u8::from_str_radix(std::str::from_utf8(&rest[..2])?, 16)?);
            remaining = &rest[2..];
        } else {
            bytes.push(first);
            remaining = rest;
        }
    }
    Ok(String::from_utf8(bytes)?)
}

fn hmac(key: &[u8], value: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(value.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

pub(super) struct Credentials<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub region: &'a str,
}

pub(super) fn authorization(
    method: &str,
    path: &str,
    query: &str,
    headers: &BTreeMap<String, String>,
    credentials: Credentials<'_>,
) -> String {
    let timestamp = &headers["x-amz-date"];
    let date = &timestamp[..8];
    let scope = format!("{date}/{}/s3/aws4_request", credentials.region);
    let canonical_headers: String = headers
        .iter()
        .map(|(name, value)| {
            format!(
                "{name}:{}\n",
                value.split_whitespace().collect::<Vec<_>>().join(" ")
            )
        })
        .collect();
    let signed_headers = headers.keys().cloned().collect::<Vec<_>>().join(";");
    let canonical = format!(
        "{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{}",
        headers["x-amz-content-sha256"]
    );
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        sha256(canonical.as_bytes())
    );
    let date_key = hmac(format!("AWS4{}", credentials.secret_key).as_bytes(), date);
    let region_key = hmac(&date_key, credentials.region);
    let service_key = hmac(&region_key, "s3");
    let signing_key = hmac(&service_key, "aws4_request");
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={}",
        credentials.access_key,
        hex(&hmac(&signing_key, &to_sign))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aws_published_get_object_signature() {
        let headers = BTreeMap::from([
            ("host".into(), "examplebucket.s3.amazonaws.com".into()),
            ("range".into(), "bytes=0-9".into()),
            ("x-amz-content-sha256".into(), sha256(b"")),
            ("x-amz-date".into(), "20130524T000000Z".into()),
        ]);
        let signature = authorization(
            "GET",
            "/test.txt",
            "",
            &headers,
            Credentials {
                access_key: "AKIAIOSFODNN7EXAMPLE",
                secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                region: "us-east-1",
            },
        );
        assert!(
            signature.ends_with("f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
        );
    }
}
