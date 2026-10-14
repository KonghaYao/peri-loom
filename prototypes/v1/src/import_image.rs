use crate::manifest::{digest, Manifest, SEGMENT_SIZE};
use anyhow::{ensure, Context, Result};
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::Path;
use turso_core::{CipherMode, EncryptionContext, EncryptionKey};

pub struct StagedImage {
    pub manifest: Manifest,
    pub manifest_id: String,
}

pub fn stage_encrypted_image(
    source: &Path,
    destination: &Path,
    key: &EncryptionKey,
) -> Result<StagedImage> {
    let mut input = File::open(source).context("open SQLite image")?;
    let original = input.metadata()?;
    ensure!(original.is_file(), "SQLite image must be a regular file");
    ensure!(original.len() >= 512, "SQLite image is incomplete");

    let mut header = [0; 100];
    input.read_exact(&mut header)?;
    ensure!(
        &header[..16] == b"SQLite format 3\0",
        "invalid SQLite image header"
    );
    let encoded_page_size = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded_page_size == 1 {
        65536
    } else {
        usize::from(encoded_page_size)
    };
    ensure!(
        page_size.is_power_of_two() && (512..=65536).contains(&page_size),
        "invalid SQLite page size"
    );
    ensure!(
        SEGMENT_SIZE % page_size == 0,
        "SQLite pages do not fit in segments"
    );
    ensure!(
        header[18] == 1 && header[19] == 1,
        "SQLite image must use rollback journal format"
    );
    ensure!(
        header[20] == 48,
        "SQLite image must reserve 48 bytes per page"
    );
    ensure!(
        header[21..24] == [64, 32, 32],
        "invalid SQLite page fractions"
    );
    ensure!(
        original.len() % page_size as u64 == 0,
        "incomplete SQLite page"
    );
    let page_count = original.len() / page_size as u64;
    ensure!(page_count <= u32::MAX as u64, "SQLite page count overflow");
    ensure!(
        u64::from(u32::from_be_bytes(header[28..32].try_into()?)) == page_count,
        "SQLite header page count differs from file size"
    );
    ensure!(
        matches!(u32::from_be_bytes(header[56..60].try_into()?), 1..=3),
        "invalid SQLite text encoding"
    );

    let context = EncryptionContext::new(CipherMode::Aegis256, key, page_size)?;
    ensure!(
        context.required_reserved_bytes() == header[20],
        "incompatible encryption reserve"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        DirBuilder::new().mode(0o700).create(destination)?;
    }
    #[cfg(not(unix))]
    DirBuilder::new().create(destination)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        DirBuilder::new()
            .mode(0o700)
            .create(destination.join("segments"))?;
        DirBuilder::new()
            .mode(0o700)
            .create(destination.join("manifests"))?;
    }
    #[cfg(not(unix))]
    {
        fs::create_dir(destination.join("segments"))?;
        fs::create_dir(destination.join("manifests"))?;
    }

    let mut manifest = Manifest {
        logical_db_size: original.len(),
        page_size: page_size as u32,
        ..Manifest::default()
    };
    let mut segment = vec![0; SEGMENT_SIZE];
    let mut page = vec![0; page_size];
    let pages_per_segment = SEGMENT_SIZE / page_size;
    input.rewind()?;
    for page_index in 0..page_count {
        input.read_exact(&mut page).context("read SQLite page")?;
        if page_index == 0 {
            ensure!(
                page[..100] == header,
                "SQLite image header changed during staging"
            );
        }
        ensure!(
            page[page_size - 48..].iter().all(|byte| *byte == 0),
            "nonzero SQLite reserved page bytes"
        );
        let encrypted = context.encrypt_page(&page, usize::try_from(page_index + 1)?)?;
        let within_segment = (page_index as usize % pages_per_segment) * page_size;
        segment[within_segment..within_segment + page_size].copy_from_slice(&encrypted);
        if page_index % pages_per_segment as u64 == pages_per_segment as u64 - 1
            || page_index + 1 == page_count
        {
            let identifier = digest(&segment);
            let segment_index = page_index / pages_per_segment as u64;
            write_new(&destination.join("segments").join(&identifier), &segment)?;
            manifest.segments.insert(segment_index, identifier);
            segment.fill(0);
        }
    }
    let final_metadata = input.metadata()?;
    ensure!(
        final_metadata.len() == original.len()
            && final_metadata.modified()? == original.modified()?,
        "SQLite image changed during staging"
    );
    manifest.validate()?;
    let bytes = serde_json::to_vec(&manifest)?;
    let manifest_id = digest(&bytes);
    File::open(destination.join("segments"))?.sync_all()?;
    let manifests = destination.join("manifests");
    let pending = manifests.join(".pending");
    write_new(&pending, &bytes)?;
    fs::rename(pending, manifests.join(&manifest_id))?;
    File::open(manifests)?.sync_all()?;
    File::open(destination)?.sync_all()?;
    Ok(StagedImage {
        manifest,
        manifest_id,
    })
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
