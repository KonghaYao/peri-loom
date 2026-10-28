use super::*;

fn legacy_store(directory: &tempfile::TempDir) -> Result<LocalObjectStore> {
    LocalObjectStore::open_layout(&directory.path().join("objects"), false)
}

#[test]
fn missing_observation_does_not_cache_absence() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = legacy_store(&directory)?;
    assert!(store.get("object")?.is_none());
    let etag = store.put("object", b"created later", PutCondition::Absent)?;
    let object = store.get("object")?.expect("new object must be visible");
    assert_eq!(object.bytes, b"created later");
    assert_eq!(object.etag, etag);
    Ok(())
}

#[test]
fn nonregular_object_is_rejected_instead_of_missing() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = legacy_store(&directory)?;
    fs::create_dir(store.path("directory"))?;
    let error = store.get("directory").unwrap_err();
    assert_eq!(error.to_string(), "nonregular store file rejected");
    Ok(())
}

#[cfg(unix)]
#[test]
fn existing_and_dangling_symlinks_are_rejected_instead_of_missing() -> Result<()> {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir()?;
    let store = legacy_store(&directory)?;
    let target = directory.path().join("target");
    fs::write(&target, b"not an object")?;
    for (key, target) in [
        ("existing-link", target),
        ("dangling-link", directory.path().join("absent")),
    ] {
        symlink(target, store.path(key))?;
        let error = store.get(key).unwrap_err();
        assert_eq!(error.to_string(), "nonregular store file rejected");
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn metadata_error_is_not_converted_to_absence() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = legacy_store(&directory)?;
    fs::rename(&store.root, directory.path().join("moved-objects"))?;
    fs::write(&store.root, b"parent is no longer a directory")?;
    let error = store.get("object").unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::NotADirectory
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn permission_error_is_not_converted_to_absence() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    // A privileged process can bypass directory permissions. Keep that case
    // explicit instead of reporting a permission check that did not occur.
    if unsafe { libc::geteuid() } == 0 {
        eprintln!("permission coverage skipped: effective uid is root");
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let store = legacy_store(&directory)?;
    let original = fs::metadata(&store.root)?.permissions();
    fs::set_permissions(&store.root, fs::Permissions::from_mode(0o0))?;
    let result = store.get("object");
    fs::set_permissions(&store.root, original)?;
    let error = result.unwrap_err();
    assert_eq!(
        error.downcast_ref::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    Ok(())
}
