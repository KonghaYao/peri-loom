use super::{
    signing::{self, Credentials},
    S3ObjectStore,
};
use anyhow::{bail, Result};
use reqwest::{header::HeaderMap, Method, StatusCode};
use std::{collections::BTreeMap, io::Read, time::Duration};

pub(super) struct Response {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub bytes: Vec<u8>,
}

impl S3ObjectStore {
    pub(super) fn request(
        &self,
        method: Method,
        key: Option<&str>,
        query: Vec<(&str, String)>,
        bytes: &[u8],
        extra: BTreeMap<String, String>,
    ) -> Result<Response> {
        let path = match key {
            Some(key) => format!("/{}/{key}", self.config.bucket),
            None => format!("/{}", self.config.bucket),
        };
        let mut query: Vec<_> = query
            .into_iter()
            .map(|(name, value)| (signing::encode(name), signing::encode(&value)))
            .collect();
        query.sort();
        let query = query
            .into_iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        let mut url = self.endpoint.clone();
        url.set_path(&path);
        if !query.is_empty() {
            url.set_query(Some(&query));
        }
        let origin = url.origin().ascii_serialization();
        let host = origin.split_once("://").expect("validated HTTP origin").1;
        for attempt in 0..=self.config.retries {
            if attempt > 0 {
                crate::slow_diagnostics::retry();
            }
            let mut headers = extra.clone();
            headers.insert("host".into(), host.into());
            headers.insert(
                "x-amz-date".into(),
                chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string(),
            );
            headers.insert("x-amz-content-sha256".into(), signing::sha256(bytes));
            if let Some(token) = &self.config.session_token {
                headers.insert("x-amz-security-token".into(), token.clone());
            }
            let authorization = signing::authorization(
                method.as_str(),
                &path,
                &query,
                &headers,
                Credentials {
                    access_key: &self.config.access_key,
                    secret_key: &self.config.secret_key,
                    region: &self.config.region,
                },
            );
            let mut request = self
                .client
                .request(method.clone(), url.clone())
                .body(bytes.to_vec())
                .header("authorization", authorization);
            for (name, value) in headers {
                request = request.header(name, value);
            }
            match request.send() {
                Ok(response) => {
                    let status = response.status();
                    let headers = response.headers().clone();
                    if (status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
                        && attempt < self.config.retries
                    {
                        drop(response);
                    } else if status.is_success() {
                        match response.bytes() {
                            Ok(bytes) => {
                                return Ok(Response {
                                    status,
                                    headers,
                                    bytes: bytes.to_vec(),
                                })
                            }
                            Err(_) if attempt < self.config.retries => (),
                            Err(_) => bail!("S3 response body read failed (network or timeout)"),
                        }
                    } else {
                        let mut bytes = Vec::new();
                        let _ = response.take(65536).read_to_end(&mut bytes);
                        return Ok(Response {
                            status,
                            headers,
                            bytes,
                        });
                    }
                }
                Err(error) => {
                    if attempt == self.config.retries
                        || !(error.is_timeout()
                            || error.is_connect()
                            || error.is_body()
                            || error.is_request())
                    {
                        bail!(
                            "S3 transport failure ({})",
                            if error.is_timeout() {
                                "timeout"
                            } else {
                                "network"
                            }
                        );
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(25 * (1u64 << attempt.min(6))));
        }
        unreachable!()
    }
}
