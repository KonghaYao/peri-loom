//! S3-compatible ObjectStore 实现（`aws-sdk-s3`）。
//!
//! 同时支持两种寻址风格（架构 §17.9）：
//! - **AWS S3**：virtual-hosted style，`https://<bucket>.s3.<region>.amazonaws.com/<key>`；
//! - **RustFS / Ceph RGW / 其它自建 S3**：path style，`http://<endpoint>/<bucket>/<key>`，需要 `S3_ENDPOINT`
//!   且 `S3_FORCE_PATH_STYLE=true`（自定义 endpoint 场景下这是默认值）。
//!
//! 配置全部来自环境变量，并支持 `*_FILE` 变体（docker secrets / Kubernetes secret 挂载），
//! 例如 `S3_SECRET_ACCESS_KEY_FILE=/run/secrets/s3_secret_access_key`。
//! 凭证不落盘、不写日志：`S3Config` 的 `Debug` 输出已脱敏。

use std::collections::BTreeMap;
use std::env;
use std::fmt;
use std::path::Path;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use domain::error::Result;
use tokio::io::AsyncWriteExt;

use crate::error::StorageError;
use crate::store::{
    part_size_for, read_part, ObjectMeta, ObjectStore, MAX_MULTIPART_PARTS, PART_SIZE_BYTES,
};

/// 自定义 endpoint（RustFS / Ceph RGW 等自建 S3）；不设则使用 AWS S3。
pub const ENV_ENDPOINT: &str = "S3_ENDPOINT";
/// 区域；默认 [`DEFAULT_REGION`]。
pub const ENV_REGION: &str = "S3_REGION";
/// bucket 名（必填）。
pub const ENV_BUCKET: &str = "S3_BUCKET";
/// Access Key ID（与 secret 成对出现；都不设则回退到 AWS 默认凭证链）。
pub const ENV_ACCESS_KEY_ID: &str = "S3_ACCESS_KEY_ID";
/// Secret Access Key。
pub const ENV_SECRET_ACCESS_KEY: &str = "S3_SECRET_ACCESS_KEY";
/// 是否强制 path style 寻址。
pub const ENV_FORCE_PATH_STYLE: &str = "S3_FORCE_PATH_STYLE";

/// 未指定 region 时的默认值（AWS SDK 在无 region 时也会拒绝发请求，必须给一个具体值）。
pub const DEFAULT_REGION: &str = "us-east-1";

/// 显式凭证的来源标记（S3 错误信息里会带上，便于区分凭证来源）。
const CREDENTIALS_PROVIDER_NAME: &str = "peri-loom-objectstore-env";

/// 对象存储配置。
#[derive(Clone, PartialEq, Eq)]
pub struct S3Config {
    /// 自定义 endpoint（`http(s)://host:port`）；`None` 表示 AWS S3。
    pub endpoint: Option<String>,
    /// 区域。
    pub region: String,
    /// bucket 名。
    pub bucket: String,
    /// Access Key ID。
    pub access_key_id: Option<String>,
    /// Secret Access Key。
    pub secret_access_key: Option<String>,
    /// 强制 path style（RustFS / Ceph RGW 通常需要）。
    pub force_path_style: bool,
}

impl fmt::Debug for S3Config {
    /// 手写 Debug：凭证属于敏感信息，绝不能随日志 / panic 信息泄漏。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "<redacted>"),
            )
            .field("force_path_style", &self.force_path_style)
            .finish()
    }
}

impl S3Config {
    /// 从进程环境变量读取配置。
    pub fn from_env() -> Result<Self> {
        Self::from_env_map(&env::vars().collect())
    }

    /// 从给定的「环境变量表」读取配置。
    ///
    /// 与 [`S3Config::from_env`] 分离是为了可测试：测试只需构造一张表，
    /// 不必修改进程级环境（`set_var` 在并发测试下不安全）。
    /// `*_FILE` 变体确实会读文件系统。
    pub fn from_env_map(env: &BTreeMap<String, String>) -> Result<Self> {
        let bucket = read_var(env, ENV_BUCKET)?
            .ok_or_else(|| StorageError::InvalidArgument(format!("{ENV_BUCKET} is required")))?;
        let region = read_var(env, ENV_REGION)?.unwrap_or_else(|| DEFAULT_REGION.to_string());
        let endpoint = read_var(env, ENV_ENDPOINT)?;
        // RustFS / Ceph 这类自定义 endpoint 几乎都需要 path style；
        // 未显式配置时按 endpoint 是否存在推断，避免部署时漏配。
        let force_path_style = match read_var(env, ENV_FORCE_PATH_STYLE)? {
            Some(raw) => parse_bool(ENV_FORCE_PATH_STYLE, &raw)?,
            None => endpoint.is_some(),
        };
        let access_key_id = read_var(env, ENV_ACCESS_KEY_ID)?;
        let secret_access_key = read_var(env, ENV_SECRET_ACCESS_KEY)?;
        if access_key_id.is_some() != secret_access_key.is_some() {
            return Err(StorageError::InvalidArgument(format!(
                "{ENV_ACCESS_KEY_ID} and {ENV_SECRET_ACCESS_KEY} must be set together"
            ))
            .into());
        }
        Ok(Self {
            endpoint,
            region,
            bucket,
            access_key_id,
            secret_access_key,
            force_path_style,
        })
    }
}

/// 读取环境变量；先取直接值，再取 `<name>_FILE` 指向的文件内容（docker secrets）。
///
/// 空值视为未设置。注意：文件内容**只用于凭证**，任何错误信息都不得包含它。
fn read_var(env: &BTreeMap<String, String>, name: &str) -> Result<Option<String>> {
    if let Some(value) = env.get(name) {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Ok(Some(trimmed.to_string()));
        }
    }
    let file_var = format!("{name}_FILE");
    let Some(path) = env.get(&file_var).map(|value| value.trim()) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(path).map_err(|err| StorageError::io(path, err))?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Err(StorageError::InvalidArgument(format!(
            "{file_var} points to an empty file: {path}"
        ))
        .into());
    }
    Ok(Some(trimmed.to_string()))
}

fn parse_bool(name: &str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => Err(StorageError::InvalidArgument(format!(
            "{name} must be a boolean (1/0, true/false), got {other:?}"
        ))
        .into()),
    }
}

/// 基于 `aws-sdk-s3` 的 S3-compatible ObjectStore。
#[derive(Debug)]
pub struct S3ObjectStore {
    client: Client,
    bucket: String,
    force_path_style: bool,
}

impl S3ObjectStore {
    /// 从环境变量构造。
    pub async fn from_env() -> Result<Self> {
        Self::new(S3Config::from_env()?).await
    }

    /// 按给定配置构造（会加载 AWS 默认配置链；显式给了凭证就不会访问 IMDS）。
    ///
    /// `aws_sdk_s3::Client` 内部持有连接池，构造一次后应复用（不要每条请求新建）。
    pub async fn new(config: S3Config) -> Result<Self> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()));
        if let Some(endpoint) = config.endpoint.as_deref() {
            loader = loader.endpoint_url(endpoint);
        }
        if let (Some(access_key_id), Some(secret_access_key)) = (
            config.access_key_id.as_ref(),
            config.secret_access_key.as_ref(),
        ) {
            loader = loader.credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                CREDENTIALS_PROVIDER_NAME,
            ));
        }
        let sdk_config = loader.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(config.force_path_style)
            .build();
        Ok(Self {
            client: Client::from_conf(s3_config),
            bucket: config.bucket,
            force_path_style: config.force_path_style,
        })
    }

    /// 确保目标 bucket 存在（不存在则创建）。
    ///
    /// 为什么由平台自己做而不是靠部署时的初始化容器：
    /// 只要 bucket 只能靠外部工具创建，任何「忘了跑初始化」或「换了对象存储实现」的
    /// 部署都会在第一次快照时才暴露问题（而且错误信息往往与根因相距很远）。
    /// 这里做成幂等操作，由服务启动时就调用一次，部署侧只需要给出 endpoint 与凭据。
    ///
    /// 已存在（含不是自己创建的）时不报错，也不做任何修改。
    pub async fn ensure_bucket(&self) -> Result<()> {
        use aws_sdk_s3::error::SdkError;
        use aws_sdk_s3::operation::head_bucket::HeadBucketError;

        match self.client.head_bucket().bucket(&self.bucket).send().await {
            Ok(_) => return Ok(()),
            Err(SdkError::ServiceError(err)) => {
                // 404 表示不存在，需要创建；403 表示存在但当前凭据无权访问 —— 后者
                // 不能当成「不存在」去创建（会掩盖权限配置错误）。
                //
                // 用 code() 而不是直接匹配 Unhandled：aws-sdk 的未建模错误变体
                // 明确标注「直接匹配不向前兼容」，新版本新增变体会让这里的 match 失效。
                let error = err.err();
                let not_found = matches!(error, HeadBucketError::NotFound(_))
                    || error.code() == Some("NotFound")
                    || error.code() == Some("NoSuchBucket");
                if !not_found {
                    return Err(StorageError::Unavailable(format!(
                        "head_bucket({}) 失败: {error:?}",
                        self.bucket
                    ))
                    .into());
                }
            }
            Err(err) => {
                return Err(StorageError::Unavailable(format!(
                    "head_bucket({}) 传输失败: {err}",
                    self.bucket
                ))
                .into());
            }
        }

        match self
            .client
            .create_bucket()
            .bucket(&self.bucket)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            // 并发创建时可能已被别人建好：再 head 一次确认存在即可
            Err(err) => match self.client.head_bucket().bucket(&self.bucket).send().await {
                Ok(_) => Ok(()),
                Err(_) => Err(StorageError::Unavailable(format!(
                    "创建 bucket {} 失败: {err}",
                    self.bucket
                ))
                .into()),
            },
        }
    }

    /// 目标 bucket。
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// 是否使用 path style 寻址。
    pub fn force_path_style(&self) -> bool {
        self.force_path_style
    }

    /// 上传单个分段；返回该段的 ETag。
    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        number: i32,
        data: Bytes,
    ) -> Result<String> {
        let out = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(number)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|err| map_s3_error("upload_part", key, &err))?;
        // 没有 ETag 就无法 complete，必须显式失败而不是发一个残缺的 complete 请求。
        out.e_tag()
            .map(str::to_string)
            .ok_or_else(|| StorageError::Internal(format!("upload_part {key} 未返回 ETag")))
            .map_err(Into::into)
    }

    /// 大文件走 S3 multipart 上传，返回上传字节数。
    ///
    /// 分段顺序上传：任意时刻最多只有一个分段驻留内存，代价是单连接吞吐。
    /// 失败必须 abort，否则 bucket 上会残留不可见的未完成分片（并按存储计费）。
    async fn put_file_multipart(&self, key: &str, path: &Path, len: u64) -> Result<u64> {
        let part_size = part_size_for(len);
        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| map_s3_error("create_multipart_upload", key, &err))?;
        let upload_id = create
            .upload_id()
            .ok_or_else(|| {
                StorageError::Internal(format!("create_multipart_upload {key} 未返回 upload_id"))
            })?
            .to_string();

        match self
            .upload_all_parts(key, path, &upload_id, part_size)
            .await
        {
            Ok((parts, uploaded)) => {
                if uploaded != len {
                    let _ = self.abort_multipart(key, &upload_id).await;
                    return Err(StorageError::Internal(format!(
                        "{key} 上传期间源文件发生变化: 期望 {len} 字节, 实际读取 {uploaded} 字节"
                    ))
                    .into());
                }
                let completed = CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build();
                self.client
                    .complete_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(completed)
                    .send()
                    .await
                    .map_err(|err| map_s3_error("complete_multipart_upload", key, &err))?;
                Ok(uploaded)
            }
            Err(err) => {
                let _ = self.abort_multipart(key, &upload_id).await;
                Err(err)
            }
        }
    }

    /// 逐个分段上传，返回 (已完成分段, 已上传字节数)。
    async fn upload_all_parts(
        &self,
        key: &str,
        path: &Path,
        upload_id: &str,
        part_size: u64,
    ) -> Result<(Vec<CompletedPart>, u64)> {
        let buf_len = usize::try_from(part_size)
            .map_err(|_| StorageError::Internal(format!("段大小 {part_size} 超出平台限制")))?;
        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|err| StorageError::io(path, err))?;
        let mut buf = vec![0u8; buf_len];
        let mut parts = Vec::new();
        let mut uploaded = 0u64;
        while let Some(chunk) = read_part(path, &mut file, &mut buf).await? {
            // 段号从 1 开始，且不得超过 S3 的 10000 段上限
            // （part_size_for 已保证常规情况下不会触顶，这里兜住源文件变大的情况）。
            let index = parts.len() + 1;
            if index as u64 > MAX_MULTIPART_PARTS {
                return Err(StorageError::Internal(format!(
                    "{key} 分段数超过 {MAX_MULTIPART_PARTS} 上限"
                ))
                .into());
            }
            let number = i32::try_from(index).map_err(|_| {
                StorageError::Internal(format!("{key} 段号 {index} 超出 S3 可表示范围"))
            })?;
            uploaded += chunk.len() as u64;
            let e_tag = self.upload_part(key, upload_id, number, chunk).await?;
            parts.push(
                CompletedPart::builder()
                    .part_number(number)
                    .e_tag(e_tag)
                    .build(),
            );
        }
        Ok((parts, uploaded))
    }

    /// 放弃未完成的分片上传；失败只记日志，不覆盖原始错误。
    async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
        let result = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(err) => {
                let described = describe_failure(&classify_sdk_error(&err));
                tracing::warn!(key, error = %described, "abort_multipart_upload 失败");
                Err(map_s3_error("abort_multipart_upload", key, &err).into())
            }
        }
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|err| map_s3_error("put_object", key, &err))?;
        Ok(())
    }

    async fn put_file(&self, key: &str, path: &Path) -> Result<u64> {
        let len = tokio::fs::metadata(path)
            .await
            .map_err(|err| StorageError::io(path, err))?
            .len();
        if len <= PART_SIZE_BYTES {
            // 小文件（含空文件）走单次 PUT：少两次往返，也避免 0 段 multipart。
            let body = ByteStream::from_path(path).await.map_err(|err| {
                StorageError::Unavailable(format!("failed to read {}: {err}", path.display()))
            })?;
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body)
                .send()
                .await
                .map_err(|err| map_s3_error("put_object", key, &err))?;
            return Ok(len);
        }
        self.put_file_multipart(key, path, len).await
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| map_s3_error("get_object", key, &err))?;
        let collected = out.body.collect().await.map_err(|err| {
            StorageError::Unavailable(format!("failed to read body of {key}: {err}"))
        })?;
        Ok(collected.into_bytes())
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> Result<u64> {
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| map_s3_error("get_object", key, &err))?;
        let expected = out.content_length().map(|len| len as u64);
        let file = tokio::fs::File::create(path)
            .await
            .map_err(|err| StorageError::io(path, err))?;
        let mut writer = tokio::io::BufWriter::new(file);
        let mut body = out.body;
        let mut written = 0u64;
        while let Some(chunk) = body.try_next().await.map_err(|err| {
            StorageError::Unavailable(format!("download of {key} failed mid-stream: {err}"))
        })? {
            writer
                .write_all(&chunk)
                .await
                .map_err(|err| StorageError::io(path, err))?;
            written += chunk.len() as u64;
        }
        writer
            .flush()
            .await
            .map_err(|err| StorageError::io(path, err))?;
        // 响应提前结束 = 对象被截断。宁可报存储故障，也不能把残缺数据交给上层
        // （快照恢复的输入一旦被静默截断，后果是数据丢失而不是报错）。
        if let Some(expected) = expected {
            if written != expected {
                return Err(StorageError::Unavailable(format!(
                    "object {key} truncated: expected {expected} bytes, got {written}"
                ))
                .into());
            }
        }
        Ok(written)
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => Ok(Some(ObjectMeta {
                key: key.to_string(),
                size: out.content_length().unwrap_or(0).max(0) as u64,
                etag: out.e_tag().map(clean_etag),
                last_modified: out.last_modified().and_then(to_utc),
            })),
            // HEAD 是存在性探测：不存在是正常结果而不是错误。
            Err(err) => match classify_sdk_error(&err) {
                SdkFailure::NotFound => Ok(None),
                _ => Err(map_s3_error("head_object", key, &err).into()),
            },
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            // S3 DELETE 幂等：对象不存在同样返回成功。
            Ok(_) => Ok(()),
            Err(err) => match classify_sdk_error(&err) {
                SdkFailure::NotFound => Ok(()),
                _ => Err(map_s3_error("delete_object", key, &err).into()),
            },
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let mut objects = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = token.as_deref() {
                request = request.continuation_token(token);
            }
            let page = request
                .send()
                .await
                .map_err(|err| map_s3_error("list_objects_v2", prefix, &err))?;
            for object in page.contents() {
                let Some(key) = object.key() else {
                    continue;
                };
                objects.push(ObjectMeta {
                    key: key.to_string(),
                    size: object.size().unwrap_or(0).max(0) as u64,
                    etag: object.e_tag().map(clean_etag),
                    last_modified: object.last_modified().and_then(to_utc),
                });
            }
            if !page.is_truncated().unwrap_or(false) {
                break;
            }
            match page.next_continuation_token() {
                Some(next) => token = Some(next.to_string()),
                // 服务端声称还有数据却没给 token：直接结束，避免死循环。
                None => break,
            }
        }
        Ok(objects)
    }
}

/// S3 失败分类：决定映射到哪个 [`StorageError`]。
#[derive(Debug, Clone, PartialEq, Eq)]
enum SdkFailure {
    /// 对象 / bucket 不存在。
    NotFound,
    /// 凭证或权限被拒绝。
    Denied,
    /// 其余（网络、超时、5xx、未建模错误）。
    Other {
        status: Option<u16>,
        code: Option<String>,
    },
}

/// 按 HTTP 状态 + 服务端错误码分类。
///
/// 优先看 HTTP status（S3 在部分场景返回空 body，此时没有错误码），再看 body 里的
/// `<Code>`。两条路径都覆盖，才能既识别 AWS S3 也识别 RustFS / Ceph 的差异。
fn classify_sdk_error<E>(err: &SdkError<E>) -> SdkFailure
where
    E: ProvideErrorMetadata,
{
    let status = err.raw_response().map(|raw| u16::from(raw.status()));
    let code = err.as_service_error().and_then(|inner| inner.code());
    if status == Some(404)
        || matches!(
            code,
            Some("NoSuchKey" | "NotFound" | "NoSuchBucket" | "NoSuchUpload")
        )
    {
        return SdkFailure::NotFound;
    }
    if matches!(status, Some(401 | 403))
        || matches!(
            code,
            Some(
                "AccessDenied"
                    | "InvalidAccessKeyId"
                    | "SignatureDoesNotMatch"
                    | "AllAccessDisabled"
                    | "InvalidSecurity"
                    | "ExpiredToken"
            )
        )
    {
        return SdkFailure::Denied;
    }
    SdkFailure::Other {
        status,
        code: code.map(str::to_string),
    }
}

/// 失败原因的简短描述（只含状态码 / 错误码）。
///
/// **刻意不携带服务端返回的 message**：S3 的错误文本可能回显 Access Key ID，
/// 而错误体会经 HTTP / 日志扩散出去；需要细节时按 `code` 去查。
fn describe_failure(failure: &SdkFailure) -> String {
    match failure {
        SdkFailure::NotFound => "not found".to_string(),
        SdkFailure::Denied => "access denied".to_string(),
        SdkFailure::Other { status, code } => {
            let status = status.map_or_else(|| "-".to_string(), |value| value.to_string());
            let code = code.as_deref().unwrap_or("-");
            format!("status={status} code={code}")
        }
    }
}

/// 通用映射：对象不存在 -> `SNAPSHOT_UNAVAILABLE`；权限 -> `PERMISSION_DENIED`；
/// 其余 -> `STORAGE_UNAVAILABLE`。
fn map_s3_error<E>(operation: &str, key: &str, err: &SdkError<E>) -> StorageError
where
    E: ProvideErrorMetadata,
{
    let failure = classify_sdk_error(err);
    let reason = describe_failure(&failure);
    tracing::debug!(operation, key, error = %reason, "S3 请求失败");
    match failure {
        SdkFailure::NotFound => StorageError::NotFound {
            key: key.to_string(),
        },
        SdkFailure::Denied => {
            StorageError::PermissionDenied(format!("{operation} {key}: {reason}"))
        }
        SdkFailure::Other { .. } => {
            StorageError::Unavailable(format!("{operation} {key}: {reason}"))
        }
    }
}

/// 去掉 S3 ETag 两侧的引号。
fn clean_etag(raw: &str) -> String {
    raw.trim_matches('"').to_string()
}

/// smithy `DateTime` -> chrono UTC；超出 chrono 表示范围返回 `None`。
fn to_utc(value: &aws_sdk_s3::primitives::DateTime) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp(value.secs(), value.subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::error::ErrorCode;
    use std::io::Write;

    fn base_env() -> BTreeMap<String, String> {
        let mut env = BTreeMap::new();
        env.insert(ENV_BUCKET.to_string(), "peri-loom-snapshots".to_string());
        env
    }

    #[test]
    fn config_from_env_map_uses_defaults() {
        let config = S3Config::from_env_map(&base_env()).unwrap();
        assert_eq!(config.bucket, "peri-loom-snapshots");
        assert_eq!(config.region, DEFAULT_REGION);
        assert_eq!(config.endpoint, None);
        assert!(!config.force_path_style, "AWS S3 默认 virtual-host style");
        assert_eq!(config.access_key_id, None);
    }

    #[test]
    fn config_from_env_map_reads_minio_style() {
        let mut env = base_env();
        env.insert(ENV_ENDPOINT.to_string(), "http://minio:9000".to_string());
        env.insert(ENV_REGION.to_string(), "us-east-1".to_string());
        env.insert(ENV_ACCESS_KEY_ID.to_string(), "minioadmin".to_string());
        env.insert(ENV_SECRET_ACCESS_KEY.to_string(), "minioadmin".to_string());
        let config = S3Config::from_env_map(&env).unwrap();
        assert_eq!(config.endpoint.as_deref(), Some("http://minio:9000"));
        assert!(config.force_path_style, "自定义 endpoint 默认 path style");
        assert_eq!(config.access_key_id.as_deref(), Some("minioadmin"));
    }

    #[test]
    fn config_reads_secret_file_variants() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("secret");
        // docker secret 文件通常带尾随换行。
        std::fs::File::create(&secret_path)
            .unwrap()
            .write_all(b"s3cr3t-from-file\n")
            .unwrap();

        let mut env = base_env();
        env.insert(
            format!("{ENV_SECRET_ACCESS_KEY}_FILE"),
            secret_path.display().to_string(),
        );
        env.insert(ENV_ACCESS_KEY_ID.to_string(), "ak".to_string());

        let config = S3Config::from_env_map(&env).unwrap();
        assert_eq!(
            config.secret_access_key.as_deref(),
            Some("s3cr3t-from-file")
        );
        // Debug 必须脱敏（凭证不得进日志）。
        let debug = format!("{config:?}");
        assert!(!debug.contains("s3cr3t-from-file"));
        assert!(!debug.contains("\"ak\""));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn config_prefers_direct_value_over_file() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("secret");
        std::fs::write(&secret_path, "from-file").unwrap();
        let mut env = base_env();
        env.insert(ENV_SECRET_ACCESS_KEY.to_string(), "from-env".to_string());
        env.insert(
            format!("{ENV_SECRET_ACCESS_KEY}_FILE"),
            secret_path.display().to_string(),
        );
        env.insert(ENV_ACCESS_KEY_ID.to_string(), "ak".to_string());
        let config = S3Config::from_env_map(&env).unwrap();
        assert_eq!(config.secret_access_key.as_deref(), Some("from-env"));
    }

    #[test]
    fn config_rejects_invalid_input() {
        // 缺 bucket
        let err = S3Config::from_env_map(&BTreeMap::new()).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);

        // 凭证只给一半
        let mut env = base_env();
        env.insert(ENV_ACCESS_KEY_ID.to_string(), "ak".to_string());
        let err = S3Config::from_env_map(&env).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);

        // 布尔值非法
        let mut env = base_env();
        env.insert(ENV_FORCE_PATH_STYLE.to_string(), "maybe".to_string());
        let err = S3Config::from_env_map(&env).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);

        // 文件不存在
        let mut env = base_env();
        env.insert(
            format!("{ENV_SECRET_ACCESS_KEY}_FILE"),
            "/nonexistent/peri-loom/secret".to_string(),
        );
        env.insert(ENV_ACCESS_KEY_ID.to_string(), "ak".to_string());
        let err = S3Config::from_env_map(&env).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn config_parses_boolean_variants() {
        for raw in ["1", "true", "TRUE", "Yes", "on"] {
            assert!(parse_bool("X", raw).unwrap(), "{raw} 应为 true");
        }
        for raw in ["0", "false", "FALSE", "No", "off"] {
            assert!(!parse_bool("X", raw).unwrap(), "{raw} 应为 false");
        }
        // 显式关闭 path style（AWS S3 + 自定义 endpoint 的场景）。
        let mut env = base_env();
        env.insert(ENV_ENDPOINT.to_string(), "http://s3.local".to_string());
        env.insert(ENV_FORCE_PATH_STYLE.to_string(), "0".to_string());
        assert!(!S3Config::from_env_map(&env).unwrap().force_path_style);
    }

    #[tokio::test]
    async fn client_builds_without_network() {
        let config = S3Config {
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            region: DEFAULT_REGION.to_string(),
            bucket: "b".to_string(),
            access_key_id: Some("ak".to_string()),
            secret_access_key: Some("sk".to_string()),
            force_path_style: true,
        };
        let store = S3ObjectStore::new(config).await.unwrap();
        assert_eq!(store.bucket(), "b");
        assert!(store.force_path_style());
    }

    /// 连接被拒绝（无监听端口）必须映射为 `STORAGE_UNAVAILABLE`（可重试），
    /// 而不是对象不存在。
    #[tokio::test]
    async fn unreachable_endpoint_is_storage_unavailable() {
        let config = S3Config {
            // 端口 1 上不会有服务监听，connect 立即被拒绝。
            endpoint: Some("http://127.0.0.1:1".to_string()),
            region: DEFAULT_REGION.to_string(),
            bucket: "b".to_string(),
            access_key_id: Some("ak".to_string()),
            secret_access_key: Some("sk".to_string()),
            force_path_style: true,
        };
        let store = S3ObjectStore::new(config).await.unwrap();

        let err = store.get("snapshots/db/snap/1.zst").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::StorageUnavailable);
        assert!(err.retryable);

        // head 也必须报故障（不能把「查不到」当成「不存在」）。
        let err = store.head("snapshots/db/snap/1.zst").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::StorageUnavailable);

        let err = store.delete("snapshots/db/snap/1.zst").await.unwrap_err();
        assert_eq!(err.code, ErrorCode::StorageUnavailable);
    }

    #[test]
    fn describe_failure_never_leaks_service_message() {
        let failure = SdkFailure::Other {
            status: Some(500),
            code: Some("InternalError".to_string()),
        };
        assert_eq!(describe_failure(&failure), "status=500 code=InternalError");
        assert_eq!(describe_failure(&SdkFailure::NotFound), "not found");
        assert_eq!(describe_failure(&SdkFailure::Denied), "access denied");
    }

    #[test]
    fn etag_quotes_are_stripped() {
        assert_eq!(
            clean_etag("\"9b2cf535f27731c974343645a3985328\""),
            "9b2cf535f27731c974343645a3985328"
        );
        // multipart ETag 形如 "<digest>-<parts>"
        assert_eq!(clean_etag("\"abc-2\""), "abc-2");
    }
}
