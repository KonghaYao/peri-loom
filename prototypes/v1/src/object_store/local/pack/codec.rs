use super::*;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

pub(super) const MAGIC: &[u8; 8] = b"PLPACK1\0";
pub(super) const HEADER_LEN: usize = 52;

/// Data-format failures are distinct from I/O failures. Optional compaction may
/// leave corrupt old data in place, but must not hide a device/permission error.
#[derive(Debug)]
pub(super) struct CorruptObject(&'static str);

impl std::fmt::Display for CorruptObject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for CorruptObject {}

fn decoded_json<T>(result: serde_json::Result<T>) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) if error.is_io() => Err(error.into()),
        Err(error) => {
            Err(anyhow::Error::new(error).context(CorruptObject("invalid stored object JSON")))
        }
    }
}

fn complete_read<T>(result: std::io::Result<T>) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(anyhow::Error::new(error).context(CorruptObject("truncated stored object")))
        }
        Err(error) => Err(error.into()),
    }
}

/// Match the legacy store's bulk read plus slice decode. `serde_json::from_reader`
/// on an unbuffered File issues a read per byte, which turns a root crossing the
/// small-object cutoff into tens of thousands of syscalls on every lookup.
pub(super) fn read_legacy(reader: &mut impl Read) -> Result<Envelope> {
    let mut encoded = Vec::new();
    reader.read_to_end(&mut encoded)?;
    let legacy: Envelope = decoded_json(serde_json::from_slice(&encoded))
        .context("invalid local object envelope or pack magic")?;
    validate_key(&legacy.key).context(CorruptObject("invalid stored object key"))?;
    ensure!(
        !legacy.etag.is_empty(),
        CorruptObject("missing local object etag")
    );
    ensure!(
        legacy.checksum == blake3::hash(&legacy.bytes).to_hex().as_str(),
        CorruptObject("local object checksum mismatch")
    );
    Ok(legacy)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    pub(super) key: String,
    pub(super) etag: String,
    pub(super) checksum: String,
    pub(super) offset: u64,
    pub(super) length: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Directory {
    pub(super) version: u32,
    pub(super) entries: Vec<Entry>,
}

pub(super) struct Reader {
    pub(super) file: File,
    pub(super) directory: Option<Arc<Directory>>,
    pub(super) payload_len: usize,
    pub(super) directory_bytes: usize,
    data_start: u64,
    legacy: Option<Envelope>,
}

impl Reader {
    pub(super) fn open(path: &Path) -> Result<Option<Self>> {
        Self::open_with_cache(path, None)
    }

    pub(super) fn open_cached(store: &LocalObjectStore, path: &Path) -> Result<Option<Self>> {
        Self::open_with_cache(path, Some(&store.physical.directory_cache))
    }

    fn open_with_cache(path: &Path, cache: Option<&cache::DirectoryCache>) -> Result<Option<Self>> {
        #[cfg(not(unix))]
        reject_symlink(path)?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // NONBLOCK prevents a replaced alias that names a FIFO from blocking
            // before the descriptor can be checked as a regular file.
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = match options.open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        Ok(Some(Self::from_file(file, cache)?))
    }

    fn from_file(mut file: File, cache: Option<&cache::DirectoryCache>) -> Result<Self> {
        let metadata = file.metadata()?;
        ensure!(metadata.is_file(), "nonregular store file rejected");
        let mut header = [0_u8; HEADER_LEN];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                ensure!(
                    &header[..8] != MAGIC,
                    CorruptObject("truncated local pack header")
                );
            }
            Err(error) => return Err(error.into()),
        }
        if &header[..8] != MAGIC {
            file.seek(SeekFrom::Start(0))?;
            let legacy = read_legacy(&mut file)?;
            return Ok(Self {
                file,
                payload_len: legacy.bytes.len(),
                legacy: Some(legacy),
                directory: None,
                directory_bytes: 0,
                data_start: 0,
            });
        }
        let directory_len = u32::from_le_bytes(header[8..12].try_into()?) as usize;
        let payload_len = u64::from_le_bytes(header[12..20].try_into()?);
        ensure!(
            directory_len > 0
                && directory_len <= MAX_DIRECTORY
                && payload_len <= MAX_PAYLOAD as u64,
            CorruptObject("local pack size budget exceeded")
        );
        let data_start = (HEADER_LEN + directory_len) as u64;
        ensure!(
            metadata.len() == data_start + payload_len,
            CorruptObject("local pack length mismatch")
        );
        let cache_key = cache::Key::new(&metadata, &header);
        if let Some(cache) = cache {
            if let Some(directory) = cache.get(&cache_key)? {
                return Ok(Self {
                    file,
                    directory: Some(directory),
                    payload_len: payload_len as usize,
                    directory_bytes: 0,
                    data_start,
                    legacy: None,
                });
            }
        }
        let mut encoded = vec![0; directory_len];
        complete_read(file.read_exact(&mut encoded))?;
        ensure!(
            blake3::hash(&encoded).as_bytes() == &header[20..52],
            CorruptObject("local pack directory checksum mismatch")
        );
        let directory: Directory = decoded_json(serde_json::from_slice(&encoded))?;
        ensure!(
            directory.version == 1
                && !directory.entries.is_empty()
                && directory.entries.len() <= MAX_ENTRIES,
            CorruptObject("unsupported local pack directory")
        );
        let mut keys = std::collections::BTreeSet::new();
        let mut expected_offset = 0_u64;
        for entry in &directory.entries {
            validate_key(&entry.key).context(CorruptObject("invalid stored object key"))?;
            ensure!(
                keys.insert(&entry.key),
                CorruptObject("duplicate local pack key")
            );
            ensure!(
                !entry.etag.is_empty() && entry.etag.len() <= 256,
                CorruptObject("invalid local pack etag")
            );
            ensure!(
                entry.checksum.len() == 64
                    && entry.checksum.bytes().all(|byte| byte.is_ascii_hexdigit()),
                CorruptObject("invalid local pack payload digest")
            );
            ensure!(
                entry.offset == expected_offset && entry.length <= MAX_SMALL_OBJECT as u64,
                CorruptObject("invalid local pack payload extent")
            );
            expected_offset = expected_offset
                .checked_add(entry.length)
                .context(CorruptObject("pack extent overflow"))?;
            ensure!(
                expected_offset <= payload_len,
                CorruptObject("local pack extent exceeds payload")
            );
        }
        ensure!(
            expected_offset == payload_len,
            CorruptObject("local pack unreferenced payload bytes")
        );
        let directory = Arc::new(directory);
        if let Some(cache) = cache {
            cache.insert(cache_key, directory.clone())?;
        }
        Ok(Self {
            file,
            directory: Some(directory),
            payload_len: payload_len as usize,
            directory_bytes: directory_len,
            data_start,
            legacy: None,
        })
    }

    pub(super) fn read_key(&mut self, key: &str) -> Result<StoredObject> {
        if let Some(legacy) = &self.legacy {
            ensure!(
                legacy.key == key,
                CorruptObject("local object key mismatch")
            );
            return Ok(StoredObject {
                bytes: legacy.bytes.clone(),
                etag: legacy.etag.clone(),
            });
        }
        let entry = self
            .directory
            .as_ref()
            .context("pack directory missing")?
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .context(CorruptObject("key absent from local pack"))?
            .clone();
        let mut bytes = vec![0; entry.length as usize];
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            complete_read(
                self.file
                    .read_exact_at(&mut bytes, self.data_start + entry.offset),
            )?;
        }
        #[cfg(not(unix))]
        {
            self.file
                .seek(SeekFrom::Start(self.data_start + entry.offset))?;
            complete_read(self.file.read_exact(&mut bytes))?;
        }
        ensure!(
            blake3::hash(&bytes).to_hex().as_str() == entry.checksum,
            CorruptObject("local pack payload checksum mismatch")
        );
        Ok(StoredObject {
            bytes,
            etag: entry.etag,
        })
    }

    /// A pack contains dead entries until compaction. Only the actual filename
    /// determines which one this live alias exposes.
    pub(super) fn key_for_alias(&self, path: &Path) -> Result<String> {
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .context("invalid object filename")?;
        let matches =
            |key: &str| format!("{}.obj", blake3::hash(key.as_bytes()).to_hex()) == filename;
        if let Some(legacy) = &self.legacy {
            ensure!(matches(&legacy.key), "local object filename mismatch");
            return Ok(legacy.key.clone());
        }
        self.directory
            .as_ref()
            .context("pack directory missing")?
            .entries
            .iter()
            .find(|entry| matches(&entry.key))
            .map(|entry| entry.key.clone())
            .context("local pack alias does not match a directory entry")
    }
}

/// The complete directory is bounded; payloads stay binary and are written from
/// the caller's slices instead of being copied into a pack-sized staging buffer.
pub(super) fn write(file: &mut File, items: &[(&str, &[u8], &str)]) -> Result<()> {
    ensure!(
        !items.is_empty() && items.len() <= MAX_ENTRIES,
        "pack entry budget exceeded"
    );
    let mut offset = 0_u64;
    let mut entries = Vec::with_capacity(items.len());
    for (key, bytes, etag) in items {
        ensure!(
            bytes.len() <= MAX_SMALL_OBJECT,
            "object exceeds small pack budget"
        );
        entries.push(Entry {
            key: (*key).to_owned(),
            etag: (*etag).to_owned(),
            checksum: blake3::hash(bytes).to_hex().to_string(),
            offset,
            length: bytes.len() as u64,
        });
        offset += bytes.len() as u64;
    }
    ensure!(offset <= MAX_PAYLOAD as u64, "pack payload budget exceeded");
    let directory = serde_json::to_vec(&Directory {
        version: 1,
        entries,
    })?;
    ensure!(
        directory.len() <= MAX_DIRECTORY,
        "pack directory budget exceeded"
    );
    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(MAGIC);
    header.extend_from_slice(&(directory.len() as u32).to_le_bytes());
    header.extend_from_slice(&offset.to_le_bytes());
    header.extend_from_slice(blake3::hash(&directory).as_bytes());
    file.write_all(&header)?;
    file.write_all(&directory)?;
    for (_, bytes, _) in items {
        file.write_all(bytes)?;
    }
    Ok(())
}
