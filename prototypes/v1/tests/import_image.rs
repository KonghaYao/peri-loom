use anyhow::{ensure, Result};
use peri_loom_v1::import_image::stage_encrypted_image;
use peri_loom_v1::manifest::{digest, SEGMENT_SIZE};
use std::fs;
use std::path::Path;
use std::process::Command;
use turso_core::{CipherMode, EncryptionContext, EncryptionKey};

fn sqlite(path: &Path, command: &str) -> Result<String> {
    let output = Command::new("sqlite3").arg(path).arg(command).output()?;
    ensure!(output.status.success(), "synthetic SQLite command failed");
    Ok(String::from_utf8(output.stdout)?)
}

fn fixture(root: &Path) -> Result<std::path::PathBuf> {
    let source = root.join("source.db");
    sqlite(
        &source,
        "CREATE TABLE synthetic (id INTEGER PRIMARY KEY, payload BLOB); INSERT INTO synthetic SELECT value, zeroblob(2048) FROM generate_series(1, 135);",
    )?;
    let image = root.join("image.db");
    let output = Command::new("sqlite3")
        .arg("-cmd")
        .arg(".filectrl reserve_bytes 48")
        .arg(&source)
        .arg(format!("VACUUM INTO '{}'", image.display()))
        .output()?;
    ensure!(
        output.status.success(),
        "synthetic SQLite conversion failed"
    );
    ensure!(
        sqlite(&image, "PRAGMA quick_check;")?.trim() == "ok",
        "synthetic SQLite image failed quick_check"
    );
    ensure!(
        sqlite(&image, "SELECT count(*) FROM synthetic;")?.trim() == "135",
        "synthetic row count changed"
    );
    Ok(image)
}

#[test]
fn synthetic_sqlite_pages_roundtrip_through_staged_segments() -> Result<()> {
    let root = tempfile::tempdir()?;
    let image = fixture(root.path())?;
    let plaintext = fs::read(&image)?;
    let page_size = usize::from(u16::from_be_bytes([plaintext[16], plaintext[17]]));
    let key = EncryptionKey::new_256([0x55; 32]);
    let staged_dir = root.path().join("staged");
    let staged = stage_encrypted_image(&image, &staged_dir, &key)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&staged_dir)?.permissions().mode() & 0o777,
            0o700
        );
    }
    assert_eq!(staged.manifest.page_size as usize, page_size);
    assert_eq!(staged.manifest.logical_db_size as usize, plaintext.len());
    assert_eq!(
        staged.manifest.segments.len(),
        plaintext.len().div_ceil(SEGMENT_SIZE)
    );
    let serialized = fs::read(staged_dir.join("manifests").join(&staged.manifest_id))?;
    assert_eq!(digest(&serialized), staged.manifest_id);
    assert_eq!(
        serde_json::from_slice::<peri_loom_v1::manifest::Manifest>(&serialized)?,
        staged.manifest
    );

    let context = EncryptionContext::new(CipherMode::Aegis256, &key, page_size)?;
    let mut recovered = Vec::new();
    for (segment_index, identifier) in &staged.manifest.segments {
        let bytes = fs::read(staged_dir.join("segments").join(identifier))?;
        assert_eq!(bytes.len(), SEGMENT_SIZE);
        assert_eq!(digest(&bytes), *identifier);
        for (within, page) in bytes.chunks_exact(page_size).enumerate() {
            let page_index = *segment_index as usize * (SEGMENT_SIZE / page_size) + within;
            if page_index * page_size < plaintext.len() {
                recovered.extend(context.decrypt_page(page, page_index + 1)?);
            } else {
                assert!(page.iter().all(|byte| *byte == 0));
            }
        }
    }
    assert_eq!(recovered, plaintext);
    Ok(())
}

#[test]
fn incomplete_or_incompatible_image_is_not_staged() -> Result<()> {
    let root = tempfile::tempdir()?;
    let image = fixture(root.path())?;
    let key = EncryptionKey::new_256([0x55; 32]);
    let mut bytes = fs::read(&image)?;
    bytes[20] = 0;
    let invalid = root.path().join("invalid.db");
    fs::write(&invalid, &bytes)?;
    assert!(stage_encrypted_image(&invalid, &root.path().join("invalid-stage"), &key).is_err());
    assert!(!root.path().join("invalid-stage").exists());

    bytes = fs::read(&image)?;
    bytes.pop();
    fs::write(&invalid, &bytes)?;
    assert!(stage_encrypted_image(&invalid, &root.path().join("truncated-stage"), &key).is_err());
    assert!(!root.path().join("truncated-stage").exists());

    bytes = fs::read(&image)?;
    let page_size = u16::from_be_bytes([bytes[16], bytes[17]]) as usize;
    bytes[page_size - 1] = 1;
    fs::write(&invalid, &bytes)?;
    let rejected = root.path().join("nonzero-stage");
    assert!(stage_encrypted_image(&invalid, &rejected, &key).is_err());
    assert!(rejected.join("manifests").read_dir()?.next().is_none());
    Ok(())
}
