use super::{config, mock, read_request, reply, verify_signature};
use anyhow::Result;
use peri_loom_v1::object_store::{ObjectStore, PreconditionFailed, PutCondition, S3ObjectStore};
use std::{io::Write, net::TcpListener, thread, time::Duration};

#[test]
fn s3_missing_bucket_is_not_a_missing_object_and_errors_are_redacted() -> Result<()> {
    let (store, worker) = mock(|listener| {
        for code in ["NoSuchBucket", "NoSuchKey", "never-log-this-secret"] {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            let body = format!("<Error><Code>{code}</Code><Message>session-token never-log-this-secret</Message></Error>");
            reply(&mut stream, 404, &[], body.as_bytes());
        }
    });
    let error = store.get("db/HEAD").unwrap_err();
    assert!(error.to_string().contains("NoSuchBucket"));
    assert!(store.get("db/HEAD")?.is_none());
    let error = store.get("db/HEAD").unwrap_err();
    assert!(!format!("{error:#?}").contains("never-log-this-secret"));
    assert!(!format!("{error:#?}").contains("session-token"));
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_conflicts_are_typed_without_masking_other_409_errors() -> Result<()> {
    let (store, worker) = mock(|listener| {
        for code in ["ConditionalRequestConflict", "BucketAlreadyExists"] {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            reply(
                &mut stream,
                409,
                &[],
                format!("<Error><Code>{code}</Code></Error>").as_bytes(),
            );
        }
    });
    assert!(store
        .put("db/HEAD", b"value", PutCondition::Absent)
        .unwrap_err()
        .is::<PreconditionFailed>());
    assert!(!store
        .put("db/HEAD", b"value", PutCondition::Absent)
        .unwrap_err()
        .is::<PreconditionFailed>());
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_lost_conditional_put_ack_preserves_conflict_for_receipt_readback() -> Result<()> {
    let (store, worker) = mock(|listener| {
        for operation in 0..3 {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            verify_signature(&request);
            if operation < 2 {
                assert_eq!(request.method, "PUT");
                assert_eq!(request.headers["if-match"], "\"old\"");
                assert_eq!(request.body, b"generation-2-receipt");
            }
            match operation {
                0 => drop(stream),
                1 => reply(
                    &mut stream,
                    412,
                    &[],
                    b"<Error><Code>PreconditionFailed</Code></Error>",
                ),
                _ => {
                    assert_eq!(request.method, "GET");
                    reply(
                        &mut stream,
                        200,
                        &[("ETag", "\"new\"")],
                        b"generation-2-receipt",
                    );
                }
            }
        }
    });
    let error = store
        .put(
            "db/HEAD",
            b"generation-2-receipt",
            PutCondition::Match("\"old\"".into()),
        )
        .unwrap_err();
    assert!(error.is::<PreconditionFailed>());
    let receipt = store.get("db/HEAD")?.unwrap();
    assert_eq!(receipt.bytes, b"generation-2-receipt");
    assert_eq!(receipt.etag, "\"new\"");
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_retry_budget_is_bounded_and_requests_remain_signed() -> Result<()> {
    let (store, worker) = mock(|listener| {
        for status in [429, 503] {
            let (mut stream, _) = listener.accept().unwrap();
            verify_signature(&read_request(&mut stream));
            reply(
                &mut stream,
                status,
                &[],
                b"<Error><Message>never-log-this-secret</Message></Error>",
            );
        }
    });
    let error = store.get("db/HEAD").unwrap_err();
    assert!(error.to_string().contains("503"));
    assert!(!error.to_string().contains("never-log-this-secret"));
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_retries_truncated_response_bodies() -> Result<()> {
    let (store, worker) = mock(|listener| {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nETag: \"tag\"\r\nConnection: close\r\n\r\nshort").unwrap();
        drop(stream);
        let (mut stream, _) = listener.accept().unwrap();
        verify_signature(&read_request(&mut stream));
        reply(&mut stream, 200, &[("ETag", "\"tag\"")], b"complete");
    });
    assert_eq!(store.get("db/HEAD")?.unwrap().bytes, b"complete");
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_times_out_while_reading_response_body() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut settings = config(format!("http://{}", listener.local_addr()?));
    settings.timeout = Duration::from_millis(40);
    settings.retries = 0;
    let worker = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nETag: \"tag\"\r\n\r\n")
            .unwrap();
        thread::sleep(Duration::from_millis(150));
    });
    let error = S3ObjectStore::new(settings)?.get("db/HEAD").unwrap_err();
    assert!(error.to_string().contains("timeout"));
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_list_rejects_namespace_escape_and_pagination_cycles() -> Result<()> {
    let (store, worker) = mock(|listener| {
        for page in 0..3 {
            let (mut stream, _) = listener.accept().unwrap();
            read_request(&mut stream);
            let document = if page == 0 {
                "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>other/db/HEAD</Key></Contents></ListBucketResult>"
            } else {
                "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>repeated</NextContinuationToken></ListBucketResult>"
            };
            reply(&mut stream, 200, &[], document.as_bytes());
        }
    });
    assert!(store
        .list("tenant/db/")
        .unwrap_err()
        .to_string()
        .contains("outside requested prefix"));
    assert!(store
        .list("tenant/db/")
        .unwrap_err()
        .to_string()
        .contains("continuation token"));
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_server_checksum_is_verified() -> Result<()> {
    let (store, worker) = mock(|listener| {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        reply(
            &mut stream,
            200,
            &[("ETag", "\"tag\""), ("x-amz-checksum-sha256", "invalid")],
            b"payload",
        );
    });
    assert!(store
        .get("db/HEAD")
        .unwrap_err()
        .to_string()
        .contains("server checksum mismatch"));
    worker.join().unwrap();
    Ok(())
}

#[test]
fn s3_invalid_configuration_and_conditional_headers_fail_before_network() -> Result<()> {
    for endpoint in [
        "http://user:secret@localhost:19000",
        "file:///tmp/objects",
        "http://localhost:19000/base",
        "http://localhost:19000/?token=secret",
    ] {
        assert!(S3ObjectStore::new(config(endpoint.into())).is_err());
    }
    let store = S3ObjectStore::new(config("http://127.0.0.1:1".into()))?;
    for etag in ["", "*", "\"one\",\"two\"", "tag\r\ninjected: true"] {
        assert!(store
            .put("db/HEAD", b"value", PutCondition::Match(etag.into()))
            .unwrap_err()
            .to_string()
            .contains("invalid conditional ETag"));
        assert!(store
            .delete("db/HEAD", Some(etag))
            .unwrap_err()
            .to_string()
            .contains("invalid conditional ETag"));
    }
    Ok(())
}
