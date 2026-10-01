//! 实例锁必须随宿主存活；文件存在本身不能证明另一个实例正在使用目录。
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

pub struct Instance {
    pub root: PathBuf,
    _lock: File,
}
#[derive(Serialize, Deserialize)]
struct Format {
    format_version: u32,
    mode: String,
    instance_id: uuid::Uuid,
}

impl Instance {
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)?;
        let root = fs::canonicalize(root)?;
        let lock = private_options()
            .create(true)
            .truncate(false)
            .open(root.join("instance.lock"))?;
        lock.try_lock().context("数据目录已被另一个实例占用")?;
        let format_path = root.join("instance.json");
        if format_path.exists() {
            let value: Format = serde_json::from_slice(&fs::read(&format_path)?)?;
            if value.format_version != 1 || value.mode != "simple" {
                bail!("不支持的数据格式或部署模式；不能原地切换模式或降级");
            }
        } else {
            if fs::read_dir(&root)?.any(|entry| {
                entry
                    .map(|e| e.file_name() != "instance.lock")
                    .unwrap_or(true)
            }) {
                bail!("非空数据目录缺少 instance.json，拒绝覆盖未知数据");
            }
            atomic_write(
                &root,
                "instance.json",
                &serde_json::to_vec_pretty(&Format {
                    format_version: 1,
                    mode: "simple".into(),
                    instance_id: uuid::Uuid::new_v4(),
                })?,
            )?;
        }
        for name in ["secrets", "catalog", "databases", "objects", "tmp"] {
            let path = root.join(name);
            if fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink()) {
                bail!("数据子目录不能为符号链接：{name}");
            }
            fs::create_dir_all(&path)?;
            #[cfg(unix)]
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
        }
        sync_dir(&root)?;
        Ok(Self { root, _lock: lock })
    }
    pub fn jwt_secret(&self) -> Result<Vec<u8>> {
        let dir = self.root.join("secrets");
        let path = dir.join("jwt.key");
        if path.exists() {
            let data = fs::read(&path)?;
            if data.len() < 32 {
                bail!("持久 JWT 密钥长度不足");
            }
            Ok(data)
        } else {
            let secret: [u8; 32] = rand::random();
            atomic_write(&dir, "jwt.key", &secret)?;
            Ok(secret.to_vec())
        }
    }
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    options
}

pub fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all().context("同步目录失败")
}
pub fn atomic_write(dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let temp = dir.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let mut file = private_options().create_new(true).open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temp, dir.join(name))?;
    sync_dir(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exclusive_lock_and_persistent_secret() {
        let dir = tempfile::tempdir().unwrap();
        let first = Instance::open(dir.path()).unwrap();
        assert!(Instance::open(dir.path()).is_err());
        let key = first.jwt_secret().unwrap();
        drop(first);
        assert_eq!(
            Instance::open(dir.path()).unwrap().jwt_secret().unwrap(),
            key
        );
    }
    #[test]
    fn rejects_unknown_nonempty_data() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("important.db"), "data").unwrap();
        assert!(Instance::open(dir.path()).is_err());
    }
}
