use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use peri_loom_v1::object_store::{LocalObjectStore, ObjectStore, PreconditionFailed, PutCondition};

pub enum Backend {
    Local(Arc<LocalObjectStore>),
    Forward(String),
}

#[derive(Clone, Copy)]
pub enum Target {
    Blob,
    Root,
    AppendWal,
    Grant,
    Put,
    Close,
}

struct Armed {
    target: Target,
    ordinal: u64,
    observed: u64,
    entered: mpsc::Sender<Held>,
}

pub struct Held {
    pub path: String,
    pub ordinal: u64,
    pub response: Vec<u8>,
    resume: Option<mpsc::Sender<bool>>,
}

impl Held {
    pub fn release(mut self, deliver: bool) -> Result<()> {
        self.resume.take().unwrap().send(deliver)?;
        Ok(())
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        if let Some(resume) = self.resume.take() {
            let _ = resume.send(false);
        }
    }
}

#[derive(Default)]
struct Statistics {
    generation: u64,
    operations: BTreeMap<String, (u64, u64, u64)>,
}

impl Statistics {
    fn begin(&mut self, category: &str) -> u64 {
        self.operations.entry(category.into()).or_default().0 += 1;
        self.generation
    }

    fn finish(&mut self, category: &str, generation: u64, elapsed_micros: u64) {
        // A reset defines a new request-start window. A background request that
        // began earlier must neither panic on a cleared entry nor charge its
        // completion to a new request of the same category.
        if generation == self.generation {
            let counts = self
                .operations
                .get_mut(category)
                .expect("started HTTP request missing");
            counts.1 += 1;
            counts.2 += elapsed_micros;
        }
    }

    fn reset(&mut self) {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("HTTP statistics generation exhausted");
        self.operations.clear();
    }
}

type HttpStatistics = Arc<Mutex<Statistics>>;

pub struct Edge {
    pub endpoint: String,
    armed: Arc<Mutex<Option<Armed>>>,
    delay_ms: Arc<AtomicU64>,
    statistics: HttpStatistics,
    shutdown: Option<mpsc::Sender<()>>,
    task: Option<thread::JoinHandle<Result<()>>>,
}

struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    bytes: Vec<u8>,
}

struct Response {
    status: u16,
    headers: BTreeMap<String, String>,
    bytes: Vec<u8>,
}

impl Edge {
    pub fn start(backend: Backend) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let armed = Arc::new(Mutex::new(None::<Armed>));
        let capture = armed.clone();
        let backend = Arc::new(backend);
        let delay_ms = Arc::new(AtomicU64::new(0));
        let latency = delay_ms.clone();
        let statistics = Arc::new(Mutex::new(Statistics::default()));
        let counters = statistics.clone();
        let (shutdown, stopped) = mpsc::channel();
        let task = thread::spawn(move || {
            let client = reqwest::blocking::Client::builder()
                .no_proxy()
                .retry(reqwest::retry::never())
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(20))
                .build()?;
            let mut tasks = Vec::new();
            for stream in listener.incoming() {
                let mut stream = stream?;
                if stopped.try_recv().is_ok() {
                    break;
                }
                let backend = backend.clone();
                let capture = capture.clone();
                let client = client.clone();
                let latency = latency.clone();
                let counters = counters.clone();
                tasks.push(thread::spawn(move || -> Result<()> {
                    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
                    let trace_id = SEQUENCE.fetch_add(1, Ordering::Relaxed);
                    let request = read(&mut stream)?;
                    let started = Instant::now();
                    let trace = std::env::var("PERI_P4_HTTP_TRACE").as_deref() == Ok("1");
                    let operation = serde_json::from_slice::<serde_json::Value>(&request.bytes).ok().filter(|body| body["command"]["command"].is_string()).map(|body| format!("{}/{}", body["command"]["command"].as_str().unwrap_or("backend"), body["command"]["operation"]["operation"].as_str().unwrap_or("none"))).unwrap_or_else(|| "backend".into());
                    if trace {
                        eprintln!("P4_HTTP_BEGIN trace_id={trace_id} method={} path={} operation={operation} bytes={}", request.method, request.path, request.bytes.len());
                    }
                    let category = if operation.starts_with("session/") {
                            let body: serde_json::Value = serde_json::from_slice(&request.bytes)?;
                            let key = body["command"]["operation"]["key"].as_str().unwrap_or("");
                            format!("{operation}/{}", if key.contains("/wal/") { "wal" } else { "other" })
                        } else if operation == "backend" {
                            format!("{}/{}", request.method, if request.path.contains("/authority/root") { "root" } else if request.path.contains("/authority/blobs/") { "blob" } else { "other" })
                        } else { operation.clone() };
                    let generation = counters.lock().unwrap().begin(&category);
                    let selected = {
                        let mut armed = capture.lock().unwrap();
                        let matches = armed.as_ref().is_some_and(|armed| match armed.target {
                            Target::Root => request.method == "PUT" && request.path.contains("/authority/root"),
                            Target::AppendWal => operation == "session/append_wal",
                            Target::Blob => {
                                request.method == "PUT"
                                    && request.path.contains("/authority/blobs/")
                            }
                            Target::Grant => {
                                serde_json::from_slice::<serde_json::Value>(&request.bytes)
                                    .ok()
                                    .is_some_and(|body| body["command"]["command"] == "grant")
                            }
                            Target::Put => {
                                serde_json::from_slice::<serde_json::Value>(&request.bytes)
                                    .ok()
                                    .is_some_and(|body| {
                                        body["command"]["operation"]["operation"] == "put"
                                    })
                            }
                            Target::Close => {
                                serde_json::from_slice::<serde_json::Value>(&request.bytes)
                                    .ok()
                                    .is_some_and(|body| {
                                        matches!(
                                            body["command"]["command"].as_str(),
                                            Some("close_session" | "retire_session")
                                        )
                                    })
                            }
                        });
                        let selected = matches && {
                            let selected = armed.as_mut().unwrap();
                            selected.observed += 1;
                            selected.observed == selected.ordinal
                        };
                        if selected {
                            armed.take()
                        } else {
                            None
                        }
                    };
                    let delay = latency.load(Ordering::Relaxed);
                    if delay != 0 { thread::sleep(Duration::from_millis(delay)); }
                    let response = match &*backend {
                        Backend::Local(store) => local(store, &request)?,
                        Backend::Forward(endpoint) => forward(&client, endpoint, &request)?,
                    };
                    counters.lock().unwrap().finish(&category, generation, started.elapsed().as_micros() as u64);
                    if trace {
                        eprintln!("P4_HTTP_BACKEND_TERMINAL trace_id={trace_id} method={} path={} operation={operation} status={} elapsed_us={}", request.method, request.path, response.status, started.elapsed().as_micros());
                    }
                    if let Some(selected) = selected {
                        let (resume, resumed) = mpsc::channel();
                        selected.entered.send(Held {
                            path: request.path,
                            ordinal: selected.observed,
                            response: response.bytes.clone(),
                            resume: Some(resume),
                        })?;
                        if !resumed.recv_timeout(Duration::from_secs(20))? {
                            if trace {
                                eprintln!("P4_HTTP_RESPONSE_DROPPED trace_id={trace_id} operation={operation} elapsed_us={}", started.elapsed().as_micros());
                            }
                            return Ok(());
                        }
                    }
                    let result = write(&mut stream, response);
                    if trace {
                        eprintln!("P4_HTTP_RESPONSE_WRITE trace_id={trace_id} operation={operation} elapsed_us={} write_ok={}", started.elapsed().as_micros(), result.is_ok());
                    }
                    result
                }));
            }
            for task in tasks {
                task.join()
                    .map_err(|_| anyhow::anyhow!("HTTP edge panicked"))??;
            }
            Ok(())
        });
        Ok(Self {
            endpoint,
            armed,
            delay_ms,
            statistics,
            shutdown: Some(shutdown),
            task: Some(task),
        })
    }

    pub fn arm(&self, target: Target) -> mpsc::Receiver<Held> {
        self.arm_nth(target, 1)
    }

    pub fn arm_nth(&self, target: Target, ordinal: u64) -> mpsc::Receiver<Held> {
        assert!(ordinal > 0);
        let (entered, receiver) = mpsc::channel();
        let mut armed = self.armed.lock().unwrap();
        assert!(armed.is_none());
        *armed = Some(Armed {
            target,
            ordinal,
            observed: 0,
            entered,
        });
        receiver
    }

    pub fn disarm(&self) {
        self.armed.lock().unwrap().take();
    }

    pub fn delay(&self, millis: u64) {
        self.delay_ms.store(millis, Ordering::Relaxed);
    }

    pub fn reset_statistics(&self) {
        self.statistics.lock().unwrap().reset();
    }

    pub fn statistics(&self) -> serde_json::Value {
        serde_json::json!(self.statistics.lock().unwrap().operations.iter().map(|(operation, counts)| (operation.clone(), serde_json::json!({"started": counts.0, "backend_terminal": counts.1, "elapsed_us_sum": counts.2}))).collect::<BTreeMap<_, _>>())
    }

    pub fn stop(&mut self) -> Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(())?;
            let address = self.endpoint.strip_prefix("http://").unwrap();
            let _ = TcpStream::connect(address)?;
            self.task
                .take()
                .unwrap()
                .join()
                .map_err(|_| anyhow::anyhow!("HTTP edge panicked"))??;
        }
        Ok(())
    }
}

#[test]
fn reset_statistics_does_not_adopt_an_earlier_request_completion() {
    let mut statistics = Statistics::default();
    let old = statistics.begin("PUT/blob");
    statistics.reset();
    statistics.finish("PUT/blob", old, 1);
    assert!(statistics.operations.is_empty());
    let current = statistics.begin("PUT/blob");
    statistics.finish("PUT/blob", old, 2);
    assert_eq!(statistics.operations["PUT/blob"], (1, 0, 0));
    statistics.finish("PUT/blob", current, 3);
    assert_eq!(statistics.operations["PUT/blob"], (1, 1, 3));
}

impl Drop for Edge {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn read(stream: &mut TcpStream) -> Result<Request> {
    stream.set_read_timeout(Some(Duration::from_secs(20)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let fields = line.split_whitespace().collect::<Vec<_>>();
    ensure!(fields.len() == 3, "invalid edge HTTP request");
    let method = fields[0].to_owned();
    let path = fields[1].to_owned();
    let mut headers = BTreeMap::new();
    loop {
        line.clear();
        reader.read_line(&mut line)?;
        if line == "\r\n" {
            break;
        }
        ensure!(!line.is_empty(), "incomplete edge headers");
        let (name, value) = line.split_once(':').context("invalid edge header")?;
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }
    let length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    ensure!(length <= 32 * 1024 * 1024, "edge request too large");
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes)?;
    Ok(Request {
        method,
        path,
        headers,
        bytes,
    })
}

fn forward(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    request: &Request,
) -> Result<Response> {
    let mut builder = client
        .request(
            request.method.parse()?,
            format!("{endpoint}{}", request.path),
        )
        .body(request.bytes.clone());
    for (name, value) in &request.headers {
        if name != "connection" && name != "content-length" {
            builder = builder.header(name, value);
        }
    }
    let response = builder.send()?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .filter(|(name, _)| {
            !matches!(
                name.as_str(),
                "connection" | "content-length" | "transfer-encoding"
            )
        })
        .map(|(name, value)| Ok((name.to_string(), value.to_str()?.to_owned())))
        .collect::<Result<BTreeMap<_, _>>>()?;
    Ok(Response {
        status,
        headers,
        bytes: response.bytes()?.to_vec(),
    })
}

fn local(store: &LocalObjectStore, request: &Request) -> Result<Response> {
    let url = url::Url::parse(&format!("http://localhost{}", request.path))?;
    let key = url
        .path()
        .trim_start_matches('/')
        .split_once('/')
        .map(|(_, key)| key)
        .unwrap_or("");
    let mut headers = BTreeMap::new();
    let outcome: Result<(u16, Vec<u8>)> = (|| match request.method.as_str() {
        "GET" if key.is_empty() => {
            let prefix = url
                .query_pairs()
                .find(|(name, _)| name == "prefix")
                .map(|(_, value)| value.into_owned())
                .unwrap_or_default();
            let entries = store
                .list(&prefix)?
                .into_iter()
                .map(|key| format!("<Contents><Key>{key}</Key></Contents>"))
                .collect::<String>();
            Ok((200, format!("<ListBucketResult><IsTruncated>false</IsTruncated>{entries}</ListBucketResult>").into_bytes()))
        }
        "GET" => match store.get(key)? {
            Some(object) => {
                headers.insert("etag".into(), format!("\"{}\"", object.etag));
                Ok((200, object.bytes))
            }
            None => Ok((404, b"<Error><Code>NoSuchKey</Code></Error>".to_vec())),
        },
        "PUT" => {
            let condition = if request.headers.contains_key("if-none-match") {
                PutCondition::Absent
            } else if let Some(etag) = request.headers.get("if-match") {
                PutCondition::Match(etag.trim_matches('"').to_owned())
            } else {
                PutCondition::Any
            };
            let etag = store.put(key, &request.bytes, condition)?;
            headers.insert("etag".into(), format!("\"{etag}\""));
            Ok((200, Vec::new()))
        }
        "DELETE" => {
            store.delete(
                key,
                request
                    .headers
                    .get("if-match")
                    .map(|etag| etag.trim_matches('"')),
            )?;
            Ok((204, Vec::new()))
        }
        _ => anyhow::bail!("unsupported local edge method"),
    })();
    match outcome {
        Ok((status, bytes)) => Ok(Response {
            status,
            headers,
            bytes,
        }),
        Err(error) if error.is::<PreconditionFailed>() => Ok(Response {
            status: 412,
            headers,
            bytes: b"<Error><Code>PreconditionFailed</Code></Error>".to_vec(),
        }),
        Err(error) => Err(error),
    }
}

fn write(stream: &mut TcpStream, response: Response) -> Result<()> {
    stream.set_nodelay(true)?;
    let mut output = BufWriter::new(stream);
    write!(
        output,
        "HTTP/1.1 {} OK\r\nConnection: close\r\nContent-Length: {}\r\n",
        response.status,
        response.bytes.len()
    )?;
    for (name, value) in response.headers {
        write!(output, "{name}: {value}\r\n")?;
    }
    output.write_all(b"\r\n")?;
    output.write_all(&response.bytes)?;
    output.flush()?;
    Ok(())
}
