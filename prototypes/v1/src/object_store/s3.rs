use super::{
    signing, transport::Response, validate_key, validate_prefix, xml, GroupWrite, ObjectStore,
    PreconditionFailed, PutCondition, StoredObject,
};
use anyhow::{anyhow, ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use reqwest::{blocking::Client, header::HeaderValue, Method, StatusCode, Url};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    },
    thread,
    time::Duration,
};

/// Upper bound on concurrent `put` calls issued for one `put_group` body set.
///
/// Deliberately conservative: the group bodies are already-written payloads waiting on
/// network round trips, so a handful of connections hides most of the RTT without
/// turning one commit into a burst of sockets. The whole group is bounded by this, no
/// matter how many writes it carries.
const GROUP_UPLOAD_PARALLELISM: usize = 4;

#[derive(Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub timeout: Duration,
    pub retries: usize,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3Config")
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("timeout", &self.timeout)
            .field("retries", &self.retries)
            .finish_non_exhaustive()
    }
}

pub struct S3ObjectStore {
    pub(super) config: S3Config,
    pub(super) client: Client,
    pub(super) endpoint: Url,
}

impl S3ObjectStore {
    pub fn new(config: S3Config) -> Result<Self> {
        let endpoint = Url::parse(&config.endpoint).map_err(|_| anyhow!("invalid S3 endpoint"))?;
        ensure!(
            matches!(endpoint.scheme(), "http" | "https") && endpoint.host_str().is_some(),
            "invalid S3 endpoint scheme or host"
        );
        ensure!(
            endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.query().is_none()
                && endpoint.fragment().is_none()
                && endpoint.path() == "/",
            "S3 endpoint must be an origin without credentials"
        );
        ensure!(
            !config.bucket.is_empty()
                && config
                    .bucket
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b".-".contains(&byte))
                && config.bucket != "."
                && config.bucket != "..",
            "invalid S3 bucket"
        );
        ensure!(
            !config.region.is_empty()
                && config
                    .region
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
            "invalid S3 region"
        );
        ensure!(
            !config.access_key.is_empty()
                && config
                    .access_key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric()),
            "invalid S3 access key"
        );
        ensure!(!config.secret_key.is_empty(), "empty S3 secret key");
        if let Some(token) = &config.session_token {
            ensure!(
                !token.is_empty() && HeaderValue::from_str(token).is_ok(),
                "invalid S3 session token"
            );
        }
        ensure!(
            !config.timeout.is_zero() && config.retries <= 16,
            "invalid S3 timeout or retry limit"
        );
        let client = Client::builder()
            .no_proxy()
            .http1_only()
            .retry(reqwest::retry::never())
            .timeout(config.timeout)
            .connect_timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            config,
            client,
            endpoint,
        })
    }

    fn successful(response: Response) -> Result<Response> {
        let status = response.status;
        let code = Self::error_code(&response);
        if status == StatusCode::PRECONDITION_FAILED
            || (status == StatusCode::CONFLICT && code == "ConditionalRequestConflict")
        {
            return Err(PreconditionFailed.into());
        }
        ensure!(
            status.is_success(),
            "S3 request failed with HTTP {} ({code})",
            status.as_u16()
        );
        Ok(response)
    }

    fn error_code(response: &Response) -> &'static str {
        let document = String::from_utf8_lossy(&response.bytes);
        let codes = xml::values(&document, "Code").unwrap_or_default();
        match codes.first().map(String::as_str) {
            Some("NoSuchKey") => "NoSuchKey",
            Some("NoSuchBucket") => "NoSuchBucket",
            Some("AccessDenied") => "AccessDenied",
            Some("InvalidAccessKeyId") => "InvalidAccessKeyId",
            Some("SignatureDoesNotMatch") => "SignatureDoesNotMatch",
            Some("ConditionalRequestConflict") => "ConditionalRequestConflict",
            Some("RequestTimeTooSkewed") => "RequestTimeTooSkewed",
            Some("NotImplemented") => "NotImplemented",
            _ => "unclassified",
        }
    }

    fn etag(response: &Response) -> Result<String> {
        let etag = response
            .headers
            .get("etag")
            .ok_or_else(|| anyhow!("S3 response missing ETag"))?
            .to_str()?;
        ensure!(!etag.is_empty(), "S3 response has empty ETag");
        Ok(etag.to_owned())
    }
}

impl ObjectStore for S3ObjectStore {
    fn supports_strict_fencing(&self) -> bool {
        self.config.retries == 0
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        validate_key(key)?;
        let response = self.request(Method::GET, Some(key), vec![], b"", BTreeMap::new())?;
        if response.status == StatusCode::NOT_FOUND && Self::error_code(&response) == "NoSuchKey" {
            return Ok(None);
        }
        let response = Self::successful(response)?;
        let etag = Self::etag(&response)?;
        let checksum = response
            .headers
            .get("x-amz-meta-peri-sha256")
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()?;
        let server_checksum = if response
            .headers
            .get("x-amz-checksum-type")
            .is_some_and(|value| value == "COMPOSITE")
        {
            None
        } else {
            response
                .headers
                .get("x-amz-checksum-sha256")
                .map(|value| value.to_str().map(str::to_owned))
                .transpose()?
        };
        let bytes = response.bytes;
        if let Some(checksum) = checksum {
            ensure!(
                checksum == signing::sha256(&bytes),
                "S3 object checksum mismatch"
            );
        }
        if let Some(checksum) = server_checksum {
            ensure!(
                checksum == STANDARD.encode(Sha256::digest(&bytes)),
                "S3 server checksum mismatch"
            );
        }
        Ok(Some(StoredObject { bytes, etag }))
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        validate_key(key)?;
        let mut headers =
            BTreeMap::from([("x-amz-meta-peri-sha256".into(), signing::sha256(bytes))]);
        match condition {
            PutCondition::Any => (),
            PutCondition::Absent => {
                headers.insert("if-none-match".into(), "*".into());
            }
            PutCondition::Match(etag) => {
                validate_etag(&etag)?;
                headers.insert("if-match".into(), etag);
            }
        }
        Self::etag(&Self::successful(self.request(
            Method::PUT,
            Some(key),
            vec![],
            bytes,
            headers,
        )?)?)
    }

    /// Upload the immutable bodies concurrently, then publish the final write alone.
    ///
    /// S3 has no cross-object conditional write, so a group cannot become one atomic
    /// request. This adapter therefore executes the group itself: the writes before the
    /// last one go out concurrently — bounded by [`GROUP_UPLOAD_PARALLELISM`], and only
    /// when [`group_bodies_are_independent`] proves they cannot change each other's
    /// outcome — and the final write is then issued by this thread, after every body has
    /// been confirmed. The caller's ordering contract ("the write that makes the group
    /// visible comes last") is what that serialized final step upholds: the group's
    /// visible write never goes out while a body it could reference is unconfirmed.
    ///
    /// Failures stay fail-closed: a body error stops handing out new work, joins the
    /// bodies already dispatched, and returns the error, so an Unknown network outcome is
    /// never reported as success and the final write is not attempted. On error an
    /// arbitrary subset of the independent bodies may have landed; they are
    /// content-addressed and idempotent, and
    /// [`ObjectStore::group_rejection_publishes_nothing`] stays `false` because a rejected
    /// group does not prove that nothing became visible. Nothing is retried beyond
    /// `S3Config::retries`.
    fn put_group(&self, writes: &[GroupWrite]) -> Result<Vec<String>> {
        let Some((root, bodies)) = writes.split_last() else {
            return Ok(Vec::new());
        };
        if bodies.is_empty() {
            return Ok(vec![self.put(
                &root.key,
                &root.bytes,
                root.condition.clone(),
            )?]);
        }
        if !group_bodies_are_independent(root, bodies) {
            // A conditional body, a repeated key, or a body sharing the CAS key can make
            // one write's outcome depend on another. Those groups keep the trait's
            // serial, order-preserving behaviour instead of a reordering nobody checked.
            return writes
                .iter()
                .map(|write| self.put(&write.key, &write.bytes, write.condition.clone()))
                .collect();
        }

        let next = AtomicUsize::new(0);
        let failed = AtomicBool::new(false);
        let errors = Mutex::new(None);
        let etags = Mutex::new(vec![None; bodies.len()]);
        let worker_count = bodies.len().min(GROUP_UPLOAD_PARALLELISM);

        thread::scope(|scope| {
            let mut workers = Vec::with_capacity(worker_count);
            for _ in 0..worker_count {
                workers.push(scope.spawn(|| -> Result<()> {
                    loop {
                        if failed.load(Ordering::Acquire) {
                            return Ok(());
                        }
                        let index = next.fetch_add(1, Ordering::AcqRel);
                        let Some(write) = bodies.get(index) else {
                            return Ok(());
                        };
                        match self.put(&write.key, &write.bytes, write.condition.clone()) {
                            Ok(etag) => {
                                etags.lock().expect("S3 group etag lock poisoned")[index] =
                                    Some(etag);
                            }
                            Err(error) => {
                                failed.store(true, Ordering::Release);
                                let mut first =
                                    errors.lock().expect("S3 group error lock poisoned");
                                if first.is_none() {
                                    *first = Some(error);
                                }
                                return Ok(());
                            }
                        }
                    }
                }));
            }
            for worker in workers {
                if worker.join().is_err() {
                    failed.store(true, Ordering::Release);
                    let mut first = errors.lock().expect("S3 group error lock poisoned");
                    if first.is_none() {
                        *first = Some(anyhow!("S3 group upload worker panicked"));
                    }
                }
            }
        });

        if let Some(error) = errors.lock().expect("S3 group error lock poisoned").take() {
            return Err(error);
        }
        let body_etags = etags
            .into_inner()
            .expect("S3 group etag lock poisoned")
            .into_iter()
            .map(|etag| etag.expect("successful S3 group upload missing ETag"))
            .collect::<Vec<_>>();
        let root_etag = self.put(&root.key, &root.bytes, root.condition.clone())?;
        let mut result = body_etags;
        result.push(root_etag);
        Ok(result)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        validate_key(key)?;
        let mut headers = BTreeMap::new();
        if let Some(etag) = expected {
            validate_etag(etag)?;
            headers.insert("if-match".into(), etag.to_owned());
        }
        Self::successful(self.request(Method::DELETE, Some(key), vec![], b"", headers)?)?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        let mut keys = BTreeSet::new();
        let mut seen_tokens = BTreeSet::new();
        let mut token = None;
        loop {
            let mut query = vec![
                ("list-type", "2".into()),
                ("encoding-type", "url".into()),
                ("prefix", prefix.to_owned()),
            ];
            if let Some(token) = token.take() {
                query.push(("continuation-token", token));
            }
            let response =
                Self::successful(self.request(Method::GET, None, query, b"", BTreeMap::new())?)?;
            let document =
                String::from_utf8(response.bytes).map_err(|_| anyhow!("invalid S3 list UTF-8"))?;
            ensure!(
                document.contains("<ListBucketResult") && document.contains("</ListBucketResult>"),
                "invalid S3 list response"
            );
            let encoded = xml::values(&document, "EncodingType")?
                .first()
                .is_some_and(|encoding| encoding == "url");
            for key in xml::values(&document, "Key")? {
                let key = if encoded { signing::decode(&key)? } else { key };
                validate_key(&key)?;
                ensure!(
                    key.starts_with(prefix),
                    "S3 returned key outside requested prefix"
                );
                keys.insert(key);
            }
            let truncated = xml::values(&document, "IsTruncated")?;
            ensure!(
                truncated.len() == 1 && matches!(truncated[0].as_str(), "true" | "false"),
                "invalid S3 pagination flag"
            );
            if truncated[0] == "false" {
                break;
            }
            let tokens = xml::values(&document, "NextContinuationToken")?;
            ensure!(
                tokens.len() == 1 && !tokens[0].is_empty() && seen_tokens.insert(tokens[0].clone()),
                "invalid S3 continuation token"
            );
            token = Some(tokens[0].clone());
        }
        Ok(keys.into_iter().collect())
    }
}

/// Whether the writes before a group's final one may be published concurrently.
///
/// The group contract expects every earlier write to be an immutable, content-addressed
/// body that is harmless to publish on its own, but a store adapter cannot verify that
/// from key and bytes alone. What it *can* check is that no body can change another
/// write's outcome: a body carries no condition that depends on the object state (`Any` /
/// `Absent` only), no two writes of the group target the same key, and no body targets
/// the key the final write owns. Groups that fail the check stay on the serial path, so
/// no caller that relied on write order can be reordered by this adapter.
fn group_bodies_are_independent(root: &GroupWrite, bodies: &[GroupWrite]) -> bool {
    let mut keys = BTreeSet::new();
    keys.insert(root.key.as_str());
    for write in bodies {
        if !matches!(write.condition, PutCondition::Any | PutCondition::Absent) {
            return false;
        }
        if !keys.insert(write.key.as_str()) {
            return false;
        }
    }
    true
}

fn validate_etag(etag: &str) -> Result<()> {
    ensure!(
        !etag.is_empty()
            && etag != "*"
            && !etag
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte == b',')
            && HeaderValue::from_str(etag).is_ok(),
        "invalid conditional ETag"
    );
    Ok(())
}
