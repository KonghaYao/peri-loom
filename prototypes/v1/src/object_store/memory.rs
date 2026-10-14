use super::{
    check_condition, fresh_etag, validate_key, validate_prefix, ObjectStore, PreconditionFailed,
    PutCondition, StoredObject,
};
use anyhow::{anyhow, ensure, Result};
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Default)]
pub struct MemoryObjectStore {
    objects: Mutex<BTreeMap<String, StoredObject>>,
}

impl ObjectStore for MemoryObjectStore {
    fn supports_strict_fencing(&self) -> bool {
        true
    }

    fn get(&self, key: &str) -> Result<Option<StoredObject>> {
        validate_key(key)?;
        Ok(self
            .objects
            .lock()
            .map_err(|_| anyhow!("object store lock poisoned"))?
            .get(key)
            .cloned())
    }

    fn put(&self, key: &str, bytes: &[u8], condition: PutCondition) -> Result<String> {
        validate_key(key)?;
        let mut objects = self
            .objects
            .lock()
            .map_err(|_| anyhow!("object store lock poisoned"))?;
        check_condition(objects.get(key), &condition)?;
        let etag = fresh_etag(bytes);
        objects.insert(
            key.to_owned(),
            StoredObject {
                bytes: bytes.to_vec(),
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    fn delete(&self, key: &str, expected: Option<&str>) -> Result<()> {
        validate_key(key)?;
        let mut objects = self
            .objects
            .lock()
            .map_err(|_| anyhow!("object store lock poisoned"))?;
        if let (Some(expected), Some(current)) = (expected, objects.get(key)) {
            ensure!(current.etag == expected, PreconditionFailed);
        }
        objects.remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        Ok(self
            .objects
            .lock()
            .map_err(|_| anyhow!("object store lock poisoned"))?
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }
}
