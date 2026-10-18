use super::{Counts, Event, MAX_FILE_BYTES};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn private_file(path: PathBuf) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

pub(super) fn consume(directory: PathBuf, receiver: Receiver<Event>, counts: Arc<Counts>) {
    let pid = std::process::id();
    let files = private_file(directory.join(format!("rust-slow-{pid}.jsonl"))).and_then(|events| {
        private_file(directory.join(format!("rust-slow-{pid}.status.json")))
            .map(|status| (events, status))
    });
    let Ok((output, mut status)) = files else {
        counts.write_errors.fetch_add(1, Ordering::Relaxed);
        eprintln!("Rust 慢事件诊断文件创建失败；诊断不可用，业务不受影响");
        return;
    };
    let mut writer = EventWriter {
        output,
        written: 0,
        capped: false,
        limit: MAX_FILE_BYTES,
    };
    let mut last_status = Instant::now();
    writer.status(&mut status, &counts);
    loop {
        let disconnected = match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(event) => {
                writer.event(&event, &counts);
                false
            }
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => true,
        };
        if disconnected || last_status.elapsed() >= Duration::from_secs(1) {
            writer.status(&mut status, &counts);
            last_status = Instant::now();
        }
        if disconnected {
            return;
        }
    }
}

struct EventWriter<W> {
    output: W,
    written: u64,
    capped: bool,
    limit: u64,
}

impl<W: Write> EventWriter<W> {
    fn event(&mut self, event: &Event, counts: &Counts) {
        if self.capped {
            counts.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let result = (|| -> std::io::Result<bool> {
            let mut line = serde_json::to_vec(event)?;
            line.push(b'\n');
            if self.written + line.len() as u64 > self.limit {
                return Ok(false);
            }
            self.output.write_all(&line)?;
            self.written += line.len() as u64;
            Ok(true)
        })();
        if !matches!(result, Ok(true)) {
            self.capped = true;
            counts.dropped.fetch_add(1, Ordering::Relaxed);
            if result.is_err() {
                counts.write_errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("Rust 慢事件写入失败；后续事件丢弃，业务不受影响");
            }
        }
    }

    fn status(&self, status: &mut File, counts: &Counts) {
        let value = serde_json::json!({"schema":"peri-slow-status-v1", "evidence_class":"non_harness", "pid":std::process::id(),
            "ts_unix_ms":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok().map(|time| time.as_millis()),
            "dropped_events":counts.dropped.load(Ordering::Relaxed), "write_errors":counts.write_errors.load(Ordering::Relaxed),
            "written_bytes":self.written, "capped_or_failed":self.capped, "complete":false});
        let result = (|| -> std::io::Result<()> {
            status.seek(SeekFrom::Start(0))?;
            let bytes = serde_json::to_vec(&value)?;
            status.write_all(&bytes)?;
            status.set_len(bytes.len() as u64)
        })();
        if result.is_err() && counts.write_errors.fetch_add(1, Ordering::Relaxed) == 0 {
            eprintln!("Rust 慢事件状态写入失败；丢弃计数可能不完整");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_file_and_failed_writer_count_lost_events() {
        let event = crate::slow_diagnostics::tests::event();
        let counts = Counts::default();
        let mut writer = EventWriter {
            output: Vec::new(),
            written: 0,
            capped: false,
            limit: 1,
        };
        writer.event(&event, &counts);
        writer.event(&event, &counts);
        assert!(writer.output.is_empty());
        assert_eq!(counts.dropped.load(Ordering::Relaxed), 2);
        let mut writer = EventWriter {
            output: std::io::Cursor::new([0u8; 1]),
            written: 0,
            capped: false,
            limit: MAX_FILE_BYTES,
        };
        writer.event(&event, &counts);
        assert_eq!(counts.write_errors.load(Ordering::Relaxed), 1);
        assert_eq!(counts.dropped.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn consumer_writes_private_json_and_final_drop_snapshot_without_sleep() {
        let directory = tempfile::tempdir().unwrap();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        sender
            .try_send(crate::slow_diagnostics::tests::event())
            .unwrap();
        drop(sender);
        let counts = Arc::new(Counts::default());
        counts.dropped.store(7, Ordering::Relaxed);
        consume(directory.path().to_owned(), receiver, counts);
        let prefix = directory
            .path()
            .join(format!("rust-slow-{}", std::process::id()));
        let event: serde_json::Value =
            serde_json::from_slice(&std::fs::read(prefix.with_extension("jsonl")).unwrap())
                .unwrap();
        assert_eq!(event["event"], "native_execute");
        let status: serde_json::Value =
            serde_json::from_slice(&std::fs::read(prefix.with_extension("status.json")).unwrap())
                .unwrap();
        assert_eq!(status["dropped_events"], 7);
        assert_eq!(status["complete"], false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(prefix.with_extension("jsonl"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }
}
