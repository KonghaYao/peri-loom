use anyhow::{bail, Result};
use peri_loom_v1::{
    authority::Authority,
    namespace::Namespace,
    object_store::{MemoryObjectStore, ObjectStore, PutCondition, StoredObject},
};
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc, Mutex,
};

pub(super) type Hook = Box<dyn FnOnce() + Send>;

#[derive(Default)]
pub(super) struct FaultStore {
    pub inner: MemoryObjectStore,
    pub root_fault: AtomicU8,
    pub blob_fault: AtomicU8,
    pub segment_gets: AtomicUsize,
    pub before_root: Mutex<Option<Hook>>,
    pub after_root: Mutex<Option<Hook>>,
}

impl ObjectStore for FaultStore {
    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        let object = self.inner.get(key)?;
        if object
            .as_ref()
            .is_some_and(|object| object.bytes.len() == peri_loom_v1::manifest::SEGMENT_SIZE)
        {
            self.segment_gets.fetch_add(1, Ordering::SeqCst);
        }
        Ok(object)
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        let is_root = key.ends_with("/authority/root");
        if is_root {
            let hook = self.before_root.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let fault = if is_root {
            self.root_fault.swap(0, Ordering::SeqCst)
        } else if key.contains("/authority/blobs/") {
            self.blob_fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if fault == 2 {
            bail!("injected failure before persistence");
        }
        if key.contains("/authority/blobs/") {
            let (namespace, blob) = key.split_once("/authority/blobs/").unwrap();
            let root = self
                .inner
                .get(&format!("{namespace}/authority/root"))?
                .unwrap();
            let root: serde_json::Value = serde_json::from_slice(&root.bytes)?;
            assert!(
                root["root"]["uploads"][blob].is_object(),
                "physical PUT lacks durable intent"
            );
            assert!(
                root["root"]["blobs"][blob].is_object(),
                "physical PUT lacks durable registry"
            );
        }
        let etag = self.inner.put(key, bytes, condition)?;
        if is_root {
            let hook = self.after_root.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        if fault == 1 {
            bail!("injected lost acknowledgment after persistence");
        }
        Ok(etag)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        self.inner.delete(key, expected)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix)
    }
}

pub(super) fn namespace() -> Namespace {
    Namespace::new("tenant", "domain").unwrap()
}

pub(super) fn fixture() -> Result<(Arc<FaultStore>, Arc<Authority>)> {
    let store = Arc::new(FaultStore::default());
    let authority = Authority::open(store.clone(), namespace())?;
    Ok((store, authority))
}

pub(super) fn publish_head(
    store: &dyn ObjectStore,
    byte: u8,
    parent: Option<String>,
) -> Result<(String, String)> {
    let segment = vec![byte; peri_loom_v1::manifest::SEGMENT_SIZE];
    let hash = peri_loom_v1::manifest::digest(&segment);
    store.put(
        &namespace().key(&format!("segments/{hash}"))?,
        &segment,
        PutCondition::Absent,
    )?;
    let manifest = peri_loom_v1::manifest::Manifest {
        logical_db_size: segment.len() as u64,
        segments: std::collections::BTreeMap::from([(0, hash.clone())]),
        parent,
        ..Default::default()
    };
    let bytes = serde_json::to_vec(&manifest)?;
    let manifest_hash = peri_loom_v1::manifest::digest(&bytes);
    store.put(
        &namespace().key(&format!("manifests/{manifest_hash}"))?,
        &bytes,
        PutCondition::Absent,
    )?;
    store.put(
        &namespace().key("HEAD")?,
        &serde_json::to_vec(&serde_json::json!({"manifest":manifest_hash}))?,
        PutCondition::Any,
    )?;
    Ok((hash, manifest_hash))
}
