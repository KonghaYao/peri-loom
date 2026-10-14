use super::{
    grant_request, identifier,
    support::{Clock, ProbeStore},
};
use anyhow::Result;
use peri_loom_v1::{
    authority::{ownership::OwnershipAuthority, Authority},
    object_store::{ObjectStore, PutCondition, S3Config, S3ObjectStore, StoredObject},
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

struct UndeclaredRetryStore {
    inner: Arc<ProbeStore>,
}

impl ObjectStore for UndeclaredRetryStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        self.inner.put(key, bytes, condition)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[test]
fn undeclared_or_retrying_backend_is_refused_before_boot_dispatch() -> Result<()> {
    let clock = Clock::new(1000);
    let raw = ProbeStore::new(clock.clone());
    let hidden = Arc::new(UndeclaredRetryStore { inner: raw.clone() });
    assert!(!hidden.supports_strict_fencing());
    let authority = OwnershipAuthority::new(hidden, clock);
    let id = identifier("hidden-retry");
    assert!(authority.state(&id).is_err());
    assert_eq!(
        raw.put_count(),
        0,
        "preflight already dispatched durable boot operations"
    );
    assert!(grant_request(&authority, id, 10, 0, 1000, "must-not-grant").is_err());
    assert_eq!(raw.put_count(), 0);
    for retries in [0, 1, 3] {
        let store = S3ObjectStore::new(S3Config {
            endpoint: "http://127.0.0.1:9".into(),
            region: "us-east-1".into(),
            bucket: "independent-policy-no-network".into(),
            access_key: "DUMMY".into(),
            secret_key: "dummy-never-used-on-network".into(),
            session_token: None,
            timeout: Duration::from_millis(100),
            retries,
        })?;
        assert_eq!(store.supports_strict_fencing(), retries == 0);
    }
    Ok(())
}

struct LostBootAck {
    inner: Arc<ProbeStore>,
    armed: AtomicBool,
}

impl ObjectStore for LostBootAck {
    fn supports_strict_fencing(&self) -> bool {
        true
    }
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        self.inner.get(key)
    }
    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let result = self.inner.put(key, bytes, condition)?;
        if key.ends_with("/authority/root") && self.armed.swap(false, Ordering::SeqCst) {
            anyhow::bail!("independent boot root committed but response socket closed");
        }
        Ok(result)
    }
    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

#[test]
fn boot_lost_ack_does_not_reactivate_from_a_historical_receipt() -> Result<()> {
    let clock = Clock::new(1000);
    let raw = ProbeStore::new(clock.clone());
    let id = identifier("lost-boot-ack");
    let base = Authority::open(raw.clone(), id.namespace.clone())?;
    assert_eq!(base.snapshot()?.root.version, 1);
    let lost = Arc::new(LostBootAck {
        inner: raw.clone(),
        armed: AtomicBool::new(true),
    });
    let authority = OwnershipAuthority::new(lost, clock);
    let result = authority.state(&id);
    let committed = base.snapshot()?.root;
    assert_eq!(
        committed.version, 2,
        "fixture did not commit the ambiguous boot CAS"
    );
    assert_eq!(committed.ownership.as_ref().unwrap().authority_term, 1);
    assert!(
        result.is_err(),
        "boot receipt readback falsely proved terminal quiescence"
    );
    let dispatches = raw.put_count();
    assert!(authority.state(&id).is_err());
    assert!(grant_request(&authority, id, 10, 0, 1000, "ambiguous-boot-grant").is_err());
    assert_eq!(raw.put_count(), dispatches);
    assert_eq!(base.snapshot()?.root, committed);
    Ok(())
}
