use std::collections::BTreeMap;

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::request_reads;
use crate::namespace::Namespace;
use crate::object_store::{ObjectStore, PutCondition};

const PAGE_BYTES: usize = 16 * 1024;
const HASH_NIBBLES: u8 = 64;

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Page {
    Leaf {
        version: u8,
        depth: u8,
        entries: BTreeMap<String, Value>,
    },
    Branch {
        version: u8,
        depth: u8,
        children: BTreeMap<String, String>,
    },
}

pub(crate) struct PagedIndex<'a> {
    store: &'a dyn ObjectStore,
    namespace: &'a Namespace,
}

impl<'a> PagedIndex<'a> {
    pub(crate) fn new(store: &'a dyn ObjectStore, namespace: &'a Namespace) -> Self {
        Self { store, namespace }
    }

    pub(crate) fn verify_root(&self, root: Option<&str>) -> Result<()> {
        if let Some(hash) = root {
            self.read(hash, 0, &[])?;
        }
        Ok(())
    }

    pub(crate) fn get(&self, root: Option<&str>, key: &str) -> Result<Option<Value>> {
        validate_key(key)?;
        let mut current = root.map(str::to_owned);
        let mut prefix = Vec::new();
        while let Some(hash) = current {
            match self.read(&hash, prefix.len() as u8, &prefix)? {
                Page::Leaf { entries, .. } => return Ok(entries.get(key).cloned()),
                Page::Branch { children, .. } => {
                    let nibble = nibble(key, prefix.len() as u8)?;
                    prefix.push(nibble);
                    current = children.get(&nibble.to_string()).cloned();
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn insert_once(
        &self,
        root: Option<&str>,
        key: &str,
        value: Value,
    ) -> Result<String> {
        validate_key(key)?;
        self.insert_at(root, key, &value, 0, &[])
    }

    pub(crate) fn insert_many(
        &self,
        root: Option<&str>,
        entries: BTreeMap<String, Value>,
    ) -> Result<String> {
        ensure!(!entries.is_empty(), "empty index update");
        for key in entries.keys() {
            validate_key(key)?;
        }
        self.insert_many_at(root, entries, 0, &[])
    }

    pub(crate) fn build(&self, entries: BTreeMap<String, Value>) -> Result<Option<String>> {
        for key in entries.keys() {
            validate_key(key)?;
        }
        if entries.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.leaf_or_branch(entries, 0)?))
        }
    }

    fn insert_at(
        &self,
        root: Option<&str>,
        key: &str,
        value: &Value,
        depth: u8,
        prefix: &[u8],
    ) -> Result<String> {
        match root {
            None => self.leaf_or_branch(BTreeMap::from([(key.to_owned(), value.clone())]), depth),
            Some(hash) => match self.read(hash, depth, prefix)? {
                Page::Leaf { mut entries, .. } => {
                    if let Some(previous) = entries.get(key) {
                        ensure!(previous == value, "index key identity conflict");
                        return Ok(hash.to_owned());
                    }
                    entries.insert(key.to_owned(), value.clone());
                    self.leaf_or_branch(entries, depth)
                }
                Page::Branch { mut children, .. } => {
                    let selected = nibble(key, depth)?;
                    let mut next_prefix = prefix.to_vec();
                    next_prefix.push(selected);
                    let next = self.insert_at(
                        children.get(&selected.to_string()).map(String::as_str),
                        key,
                        value,
                        depth + 1,
                        &next_prefix,
                    )?;
                    children.insert(selected.to_string(), next);
                    self.write(&Page::Branch {
                        version: 1,
                        depth,
                        children,
                    })
                }
            },
        }
    }

    fn insert_many_at(
        &self,
        root: Option<&str>,
        updates: BTreeMap<String, Value>,
        depth: u8,
        prefix: &[u8],
    ) -> Result<String> {
        match root {
            None => self.leaf_or_branch(updates, depth),
            Some(hash) => match self.read(hash, depth, prefix)? {
                Page::Leaf { mut entries, .. } => {
                    let mut changed = false;
                    for (key, value) in updates {
                        if let Some(previous) = entries.get(&key) {
                            ensure!(previous == &value, "index key identity conflict");
                        } else {
                            entries.insert(key, value);
                            changed = true;
                        }
                    }
                    if changed {
                        self.leaf_or_branch(entries, depth)
                    } else {
                        Ok(hash.to_owned())
                    }
                }
                Page::Branch { mut children, .. } => {
                    let mut groups: BTreeMap<u8, BTreeMap<String, Value>> = BTreeMap::new();
                    for (key, value) in updates {
                        groups
                            .entry(nibble(&key, depth)?)
                            .or_default()
                            .insert(key, value);
                    }
                    let mut changed = false;
                    for (selected, group) in groups {
                        let slot = selected.to_string();
                        let mut next_prefix = prefix.to_vec();
                        next_prefix.push(selected);
                        let next = self.insert_many_at(
                            children.get(&slot).map(String::as_str),
                            group,
                            depth + 1,
                            &next_prefix,
                        )?;
                        if children.get(&slot) != Some(&next) {
                            children.insert(slot, next);
                            changed = true;
                        }
                    }
                    if changed {
                        self.write(&Page::Branch {
                            version: 1,
                            depth,
                            children,
                        })
                    } else {
                        Ok(hash.to_owned())
                    }
                }
            },
        }
    }

    fn leaf_or_branch(&self, entries: BTreeMap<String, Value>, depth: u8) -> Result<String> {
        let leaf = Page::Leaf {
            version: 1,
            depth,
            entries,
        };
        if serde_json::to_vec(&leaf)?.len() <= PAGE_BYTES {
            return self.write(&leaf);
        }
        ensure!(
            depth < HASH_NIBBLES,
            "index hash collision exceeds page budget"
        );
        let mut groups: BTreeMap<u8, BTreeMap<String, Value>> = BTreeMap::new();
        let Page::Leaf { entries, .. } = leaf else {
            unreachable!()
        };
        for (key, value) in entries {
            groups
                .entry(nibble(&key, depth)?)
                .or_default()
                .insert(key, value);
        }
        let mut children = BTreeMap::new();
        for (selected, group) in groups {
            children.insert(selected.to_string(), self.leaf_or_branch(group, depth + 1)?);
        }
        self.write(&Page::Branch {
            version: 1,
            depth,
            children,
        })
    }

    fn read(&self, hash: &str, depth: u8, prefix: &[u8]) -> Result<Page> {
        validate_hash(hash)?;
        let key = self
            .namespace
            .key(&format!("authority/index-pages/{hash}"))?;
        let backend = self.store as *const dyn ObjectStore as *const () as usize;
        let bytes = match request_reads::get_page(backend, &key) {
            Some(bytes) => bytes,
            None => {
                self.store
                    .get(&key)?
                    .context("published index page missing")?
                    .bytes
            }
        };
        ensure!(
            bytes.len() <= PAGE_BYTES && blake3::hash(&bytes).to_hex().as_str() == hash,
            "index page size or digest mismatch"
        );
        let page: Page = serde_json::from_slice(&bytes)?;
        match &page {
            Page::Leaf {
                version,
                depth: stored,
                entries,
            } => {
                ensure!(
                    *version == 1 && *stored == depth && !entries.is_empty(),
                    "invalid index leaf"
                );
                for key in entries.keys() {
                    validate_key(key)?;
                    ensure!(
                        prefix.iter().enumerate().all(|(position, expected)| {
                            nibble(key, position as u8).is_ok_and(|actual| actual == *expected)
                        }),
                        "index leaf outside selected path"
                    );
                }
            }
            Page::Branch {
                version,
                depth: stored,
                children,
            } => {
                ensure!(
                    *version == 1
                        && *stored == depth
                        && depth < HASH_NIBBLES
                        && !children.is_empty()
                        && children.len() <= 16,
                    "invalid index branch"
                );
                for (selected, child) in children {
                    let slot = selected.parse::<u8>()?;
                    ensure!(
                        slot < 16 && selected == &slot.to_string(),
                        "invalid index branch slot"
                    );
                    validate_hash(child)?;
                }
            }
        }
        request_reads::remember_page(backend, key, &bytes);
        Ok(page)
    }

    fn write(&self, page: &Page) -> Result<String> {
        let bytes = serde_json::to_vec(page)?;
        ensure!(bytes.len() <= PAGE_BYTES, "index page byte budget exceeded");
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let key = self
            .namespace
            .key(&format!("authority/index-pages/{hash}"))?;
        if self.store.put(&key, &bytes, PutCondition::Absent).is_err() {
            let existing = self
                .store
                .get(&key)?
                .context("index page upload unresolved")?;
            ensure!(existing.bytes == bytes, "immutable index page conflict");
        }
        request_reads::remember_page(
            self.store as *const dyn ObjectStore as *const () as usize,
            key,
            &bytes,
        );
        Ok(hash)
    }
}

fn validate_key(key: &str) -> Result<()> {
    ensure!(
        !key.is_empty()
            && key.len() <= 128
            && key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte)),
        "invalid index key"
    );
    Ok(())
}

fn validate_hash(hash: &str) -> Result<()> {
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "invalid index page identity"
    );
    Ok(())
}

fn nibble(key: &str, depth: u8) -> Result<u8> {
    ensure!(depth < HASH_NIBBLES, "index path exhausted");
    let digest = blake3::hash(key.as_bytes());
    let byte = digest.as_bytes()[(depth / 2) as usize];
    Ok(if depth.is_multiple_of(2) {
        byte >> 4
    } else {
        byte & 15
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::MemoryObjectStore;
    use serde_json::json;

    fn namespace() -> Namespace {
        Namespace::new("index", "test").unwrap()
    }

    #[test]
    fn many_receipts_keep_every_page_bounded_and_lookup_independent() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let mut root = None;
        for sequence in 0..1_000 {
            root = Some(index.insert_once(
                root.as_deref(),
                &format!("receipt-{sequence:05}"),
                json!(sequence),
            )?);
        }
        for sequence in 0..1_000 {
            assert_eq!(
                index.get(root.as_deref(), &format!("receipt-{sequence:05}"))?,
                Some(json!(sequence))
            );
        }
        assert_eq!(index.get(root.as_deref(), "absent")?, None);
        let prefix = namespace.key("authority/index-pages")?;
        let pages = store.list(&prefix)?;
        assert!(pages.len() > 1);
        for page in pages {
            assert!(store.get(&page)?.unwrap().bytes.len() <= PAGE_BYTES);
        }
        Ok(())
    }

    #[test]
    fn legacy_bulk_build_writes_pages_once_and_preserves_all_receipts() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let entries: BTreeMap<_, _> = (0..1_000)
            .map(|sequence| (format!("legacy-{sequence:05}"), json!(sequence)))
            .collect();
        let root = index.build(entries)?.unwrap();
        for sequence in 0..1_000 {
            assert_eq!(
                index.get(Some(&root), &format!("legacy-{sequence:05}"))?,
                Some(json!(sequence))
            );
        }
        let pages = store.list(&namespace.key("authority/index-pages")?)?;
        assert!(
            pages.len() <= 40,
            "bulk migration rewrote pages per receipt"
        );
        Ok(())
    }

    #[test]
    fn archive_batch_rewrites_each_affected_page_once() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let root = index
            .build(
                (0..1_000)
                    .map(|sequence| (format!("receipt-{sequence:05}"), json!(sequence)))
                    .collect(),
            )?
            .unwrap();
        let prefix = namespace.key("authority/index-pages")?;
        let before = store.list(&prefix)?.len();
        let updated = index.insert_many(
            Some(&root),
            (1_000..1_064)
                .map(|sequence| (format!("receipt-{sequence:05}"), json!(sequence)))
                .collect(),
        )?;
        assert!(store.list(&prefix)?.len() <= before + 32);
        assert_eq!(index.get(Some(&root), "receipt-01000")?, None);
        for sequence in 0..1_064 {
            assert_eq!(
                index.get(Some(&updated), &format!("receipt-{sequence:05}"))?,
                Some(json!(sequence))
            );
        }
        assert_eq!(
            index.insert_many(
                Some(&updated),
                BTreeMap::from([("receipt-01000".into(), json!(1000))])
            )?,
            updated
        );
        assert!(index
            .insert_many(
                Some(&updated),
                BTreeMap::from([("receipt-01000".into(), json!(0))])
            )
            .is_err());
        Ok(())
    }

    #[test]
    fn unpublished_and_corrupt_pages_never_become_valid_receipts() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let published = index.insert_once(None, "first", json!(1))?;
        let pending = index.insert_once(Some(&published), "second", json!(2))?;
        assert_eq!(index.get(Some(&published), "second")?, None);
        assert_eq!(index.get(Some(&pending), "second")?, Some(json!(2)));
        assert_eq!(
            index.insert_once(Some(&published), "first", json!(1))?,
            published
        );
        assert!(index
            .insert_once(Some(&published), "first", json!(3))
            .is_err());
        let key = namespace.key(&format!("authority/index-pages/{pending}"))?;
        store.put(&key, b"corrupt", PutCondition::Any)?;
        assert!(index.get(Some(&pending), "second").is_err());
        assert!(index
            .insert_once(Some(&pending), "third", json!(3))
            .is_err());
        Ok(())
    }

    #[test]
    fn single_oversized_receipt_is_rejected_before_publication() -> Result<()> {
        let store = MemoryObjectStore::default();
        let namespace = namespace();
        let index = PagedIndex::new(&store, &namespace);
        let value = json!("x".repeat(PAGE_BYTES));
        assert!(index.insert_once(None, "oversized", value).is_err());
        assert!(store
            .list(&namespace.key("authority/index-pages")?)?
            .is_empty());
        Ok(())
    }
}
