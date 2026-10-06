use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::object_store::{LocalObjectStore, ObjectStore, S3Config, S3ObjectStore};

pub fn backend(location: &str) -> Result<Arc<dyn ObjectStore>> {
    if location != "s3" {
        return Ok(Arc::new(LocalObjectStore::open(Path::new(location))?));
    }
    let config = S3Config {
        endpoint: std::env::var("S3_ENDPOINT").context("S3_ENDPOINT is required")?,
        region: std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        bucket: std::env::var("S3_BUCKET").context("S3_BUCKET is required")?,
        access_key: std::env::var("S3_ACCESS_KEY_ID").context("S3_ACCESS_KEY_ID is required")?,
        secret_key: std::env::var("S3_SECRET_ACCESS_KEY")
            .context("S3_SECRET_ACCESS_KEY is required")?,
        session_token: std::env::var("S3_SESSION_TOKEN").ok(),
        timeout: Duration::from_secs(15),
        retries: 3,
    };
    Ok(Arc::new(S3ObjectStore::new(config)?))
}
