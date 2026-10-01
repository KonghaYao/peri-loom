//! 停机整实例导出/导入。源目录必须可取得排他实例锁。
use super::instance::{sync_dir, Instance};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

const FORMAT_VERSION: u32 = 1;
const MANIFEST: &str = "instance-export.manifest.json";
const BUF_SIZE: usize = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
struct FileRecord {
    path: String,
    size_bytes: u64,
    sha256: String,
}
#[derive(Debug, Serialize, Deserialize)]
struct ExportManifest {
    format_version: u32,
    instance_id: Uuid,
    instance_format_version: u32,
    producer_version: String,
    database_ids: Vec<String>,
    files: Vec<FileRecord>,
}

/// 离线导出。`source` 不得被服务进程使用；输出是可移动的完整目录。
pub async fn export_instance(source: &Path, target: &Path) -> Result<()> {
    if !source.join("instance.json").is_file() {
        bail!("源目录不是已初始化的实例");
    }
    let locked = Instance::open(source)?;
    let metadata = catalog::SqliteCatalog::connect(locked.root.join("catalog/metadata.db")).await?;
    if metadata.has_unfinished_jobs().await? {
        bail!("实例存在未完成作业或操作；请先启动 simple 服务恢复作业，正常停止后重试导出");
    }
    metadata.close().await?; // SQLite 元数据 WAL 回填后再复制。
    let source = locked.root.clone();
    let target = target.to_path_buf();
    tokio::task::spawn_blocking(move || export_locked(&source, &target)).await??;
    Ok(())
}

fn export_locked(source: &Path, target: &Path) -> Result<()> {
    ensure_new_target(target)?;
    let stage = stage_path(target)?;
    fs::create_dir(&stage)?;
    private_directory(&stage)?;
    let result = (|| {
        let identity: serde_json::Value =
            serde_json::from_slice(&fs::read(source.join("instance.json"))?)?;
        let id = identity
            .get("instance_id")
            .and_then(|v| v.as_str())
            .context("instance_id 缺失")?
            .parse()?;
        let version = identity
            .get("format_version")
            .and_then(|v| v.as_u64())
            .context("实例格式版本缺失")?;
        let mut files = Vec::new();
        for part in [
            "instance.json",
            "catalog",
            "databases",
            "objects",
            "secrets",
        ] {
            collect(source, Path::new(part), &stage, &mut files)?;
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        if !files.iter().any(|f| f.path == "catalog/metadata.db") {
            bail!("元数据库缺失");
        }
        let database_ids = database_ids(&files)?;
        let manifest = ExportManifest {
            format_version: FORMAT_VERSION,
            instance_id: id,
            instance_format_version: version.try_into()?,
            producer_version: env!("CARGO_PKG_VERSION").into(),
            database_ids,
            files,
        };
        let mut output = private_file(&stage.join(MANIFEST))?;
        serde_json::to_writer_pretty(&mut output, &manifest)?;
        output.write_all(b"\n")?;
        output.sync_all()?;
        sync_tree(&stage)?;
        fs::rename(&stage, target)?;
        sync_dir(target.parent().context("输出目录没有父目录")?)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&stage);
    }
    result
}

/// 校验整个导出，再原子发布为新实例目录；保留原实例身份和 JWT 密钥。
pub async fn import_instance(source: &Path, target: &Path) -> Result<()> {
    let source = fs::canonicalize(source)?;
    let target = target.to_path_buf();
    tokio::task::spawn_blocking(move || import_verified(&source, &target)).await??;
    Ok(())
}

fn import_verified(source: &Path, target: &Path) -> Result<()> {
    ensure_new_target(target)?;
    let manifest: ExportManifest = serde_json::from_slice(&fs::read(source.join(MANIFEST))?)?;
    if manifest.format_version != FORMAT_VERSION
        || manifest.instance_format_version != 1
        || manifest.producer_version != env!("CARGO_PKG_VERSION")
    {
        bail!("导出格式或二进制版本不兼容");
    }
    let declared: BTreeSet<_> = manifest.files.iter().map(|f| f.path.as_str()).collect();
    if declared.len() != manifest.files.len()
        || !declared.contains("instance.json")
        || !declared.contains("catalog/metadata.db")
    {
        bail!("导出文件清单不完整或重复");
    }
    let mut found = Vec::new();
    for part in [
        "instance.json",
        "catalog",
        "databases",
        "objects",
        "secrets",
    ] {
        list_files(source, Path::new(part), &mut found)?;
    }
    let actual: BTreeSet<_> = found.iter().map(String::as_str).collect();
    if declared != actual {
        bail!("导出文件清单与目录内容不一致");
    }
    if database_ids(&manifest.files)? != manifest.database_ids {
        bail!("数据库 ID 清单不一致");
    }
    let identity: serde_json::Value =
        serde_json::from_slice(&fs::read(source.join("instance.json"))?)?;
    if identity.get("instance_id").and_then(|v| v.as_str())
        != Some(&manifest.instance_id.to_string()[..])
        || identity.get("format_version").and_then(|v| v.as_u64())
            != Some(u64::from(manifest.instance_format_version))
    {
        bail!("实例身份或格式版本不一致");
    }
    let stage = stage_path(target)?;
    fs::create_dir(&stage)?;
    private_directory(&stage)?;
    let result = (|| {
        for item in &manifest.files {
            let relative = valid_path(&item.path)?;
            let source_file = source.join(relative);
            let dest = stage.join(relative);
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            let (size, hash) = copy_hash(&source_file, &dest)?;
            if size != item.size_bytes || hash != item.sha256 {
                bail!("导出文件校验失败：{}", item.path);
            }
        }
        sync_tree(&stage)?;
        fs::rename(&stage, target)?;
        sync_dir(target.parent().context("目标目录没有父目录")?)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&stage);
    }
    result
}

fn ensure_new_target(target: &Path) -> Result<()> {
    if target.exists() {
        bail!("目标目录已存在；拒绝覆盖");
    }
    let parent = target.parent().context("目标目录没有父目录")?;
    if !parent.is_dir() {
        bail!("目标父目录不存在");
    }
    Ok(())
}
fn stage_path(target: &Path) -> Result<PathBuf> {
    let parent = target.parent().context("目标目录没有父目录")?;
    Ok(parent.join(format!(".peri-loom-export-{}", Uuid::new_v4())))
}
fn valid_path(path: &str) -> Result<&Path> {
    objectstore::snapshot::validate_relative_path(path)?;
    let first = path.split('/').next().unwrap_or("");
    if ![
        "instance.json",
        "catalog",
        "databases",
        "objects",
        "secrets",
    ]
    .contains(&first)
        || first == "instance.json" && path != "instance.json"
    {
        bail!("导出路径越界：{path}");
    }
    Ok(Path::new(path))
}
fn collect(root: &Path, relative: &Path, stage: &Path, files: &mut Vec<FileRecord>) -> Result<()> {
    let src = root.join(relative);
    let meta = fs::symlink_metadata(&src)
        .with_context(|| format!("源文件缺失：{}", relative.display()))?;
    if meta.file_type().is_symlink() {
        bail!("源目录含符号链接：{}", relative.display());
    }
    if meta.is_dir() {
        fs::create_dir_all(stage.join(relative))?;
        for entry in fs::read_dir(&src)? {
            let entry = entry?;
            let storage_sidecar =
                relative.starts_with("catalog") || relative.starts_with("databases");
            if storage_sidecar && entry.file_name().to_string_lossy().ends_with("-shm") {
                continue;
            }
            collect(root, &relative.join(entry.file_name()), stage, files)?;
        }
    } else if meta.is_file() {
        let text = relative.to_str().context("非 UTF-8 文件名")?.to_owned();
        valid_path(&text)?;
        let dst = stage.join(relative);
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        let (size, sha256) = copy_hash(&src, &dst)?;
        files.push(FileRecord {
            path: text,
            size_bytes: size,
            sha256,
        });
    } else {
        bail!("不支持的源文件类型：{}", relative.display());
    }
    Ok(())
}
fn list_files(root: &Path, relative: &Path, files: &mut Vec<String>) -> Result<()> {
    let path = root.join(relative);
    let meta = fs::symlink_metadata(&path)?;
    if meta.file_type().is_symlink() {
        bail!("导出目录含符号链接");
    }
    if meta.is_dir() {
        for item in fs::read_dir(path)? {
            let item = item?;
            list_files(root, &relative.join(item.file_name()), files)?;
        }
    } else if meta.is_file() {
        let text = relative.to_str().context("非 UTF-8 文件名")?;
        valid_path(text)?;
        files.push(text.into());
    } else {
        bail!("不支持的导出文件类型");
    }
    Ok(())
}
fn database_ids(files: &[FileRecord]) -> Result<Vec<String>> {
    let mut ids = BTreeSet::new();
    for item in files {
        if let Some(rest) = item.path.strip_prefix("databases/") {
            let id = rest.split('/').next().unwrap_or("");
            Uuid::parse_str(id).with_context(|| format!("非法数据库 ID：{id}"))?;
            ids.insert(id.to_owned());
        }
    }
    for id in &ids {
        if !files
            .iter()
            .any(|f| f.path == format!("databases/{id}/main.db"))
        {
            bail!("数据库 {id} 缺少主文件");
        }
    }
    Ok(ids.into_iter().collect())
}

fn private_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}
fn private_file(path: &Path) -> Result<File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

fn copy_hash(src: &Path, dest: &Path) -> Result<(u64, String)> {
    let mut input = File::open(src)?;
    let mut output = private_file(dest)?;
    let mut hash = Sha256::new();
    let mut size = 0u64;
    let mut buf = [0u8; BUF_SIZE];
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        output.write_all(&buf[..n])?;
        hash.update(&buf[..n]);
        size += n as u64;
    }
    output.sync_all()?;
    Ok((size, hex::encode(hash.finalize())))
}
fn sync_tree(dir: &Path) -> Result<()> {
    for item in fs::read_dir(dir)? {
        let item = item?;
        if item.file_type()?.is_dir() {
            sync_tree(&item.path())?;
        }
    }
    sync_dir(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn offline_roundtrip_and_corruption_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let instance = Instance::open(&source).unwrap();
        let secret = instance.jwt_secret().unwrap();
        let catalog = catalog::SqliteCatalog::connect(source.join("catalog/metadata.db"))
            .await
            .unwrap();
        catalog.close().await.unwrap();
        fs::write(source.join("objects/item"), b"snapshot bytes").unwrap();
        drop(instance);
        let export = dir.path().join("export");
        export_instance(&source, &export).await.unwrap();
        let restored = dir.path().join("restored");
        import_instance(&export, &restored).await.unwrap();
        assert_eq!(fs::read(restored.join("secrets/jwt.key")).unwrap(), secret);
        assert_eq!(
            fs::read(restored.join("objects/item")).unwrap(),
            b"snapshot bytes"
        );
        assert!(import_instance(&export, &restored).await.is_err());
        fs::write(export.join("objects/item"), b"tampered").unwrap();
        assert!(import_instance(&export, &dir.path().join("invalid"))
            .await
            .is_err());
    }
}
