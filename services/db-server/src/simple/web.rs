use anyhow::{Context, Result};
use axum::{
    body::Body,
    extract::Request,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Router,
};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
include!(concat!(env!("OUT_DIR"), "/assets.rs"));

pub fn router(router: Router) -> Router {
    router.fallback(asset)
}
async fn asset(request: Request) -> Response {
    let path = request.uri().path();
    if ["/api", "/data", "/db"]
        .iter()
        .any(|prefix| path == *prefix || path.starts_with(&format!("{prefix}/")))
    {
        return crate::error::ApiError::not_found("接口不存在").into_response();
    }
    if request.method() != axum::http::Method::GET && request.method() != axum::http::Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let path = path.trim_start_matches('/');
    let asset = ASSETS.iter().find(|(name, _)| *name == path).or_else(|| {
        if path.starts_with("assets/") {
            None
        } else {
            ASSETS.iter().find(|(name, _)| *name == "index.html")
        }
    });
    let Some((name, bytes)) = asset else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let content_type = if name.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if name.ends_with(".js") {
        "text/javascript; charset=utf-8"
    } else if name.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if name.ends_with(".svg") {
        "image/svg+xml"
    } else if name.ends_with(".png") {
        "image/png"
    } else if name.ends_with(".ico") {
        "image/x-icon"
    } else if name.ends_with(".woff2") {
        "font/woff2"
    } else {
        "application/octet-stream"
    };
    let body = if request.method() == axum::http::Method::HEAD {
        Body::empty()
    } else {
        Body::from(*bytes)
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(
            header::CACHE_CONTROL,
            if *name == "index.html" {
                "no-cache"
            } else {
                "public, max-age=31536000, immutable"
            },
        )
        .header("x-content-type-options", "nosniff")
        .header("x-frame-options", "DENY")
        .header("referrer-policy", "same-origin")
        .body(body)
        .unwrap()
}

pub async fn serve_tls(
    listener: tokio::net::TcpListener,
    router: Router,
    config: &super::config::SimpleConfig,
    state: crate::state::AppState,
    services: Arc<super::SimpleServices>,
) -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cert_data = std::fs::read(config.tls_cert.as_ref().unwrap())?;
    let key_data = std::fs::read(config.tls_key.as_ref().unwrap())?;
    let certs =
        rustls_pemfile::certs(&mut cert_data.as_slice()).collect::<std::io::Result<Vec<_>>>()?;
    let key = rustls_pemfile::private_key(&mut key_data.as_slice())?.context("TLS 私钥缺失")?;
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
    let mut connections = tokio::task::JoinSet::new();
    let shutdown = crate::app::wait_for_shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = accepted?; let acceptor = acceptor.clone(); let router = router.clone();
                connections.spawn(async move {
                    let Ok(Ok(tls)) = tokio::time::timeout(Duration::from_secs(10), acceptor.accept(stream)).await else { return; };
                    let io = hyper_util::rt::TokioIo::new(tls);
                    let service = hyper_util::service::TowerToHyperService::new(router);
                    let _ = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new()).serve_connection_with_upgrades(io, service).await;
                });
            }
        }
    }
    services.stopping.store(true, Ordering::Release);
    state.readiness.mark_local_stopping();
    if tokio::time::timeout(Duration::from_secs(35), async {
        while connections.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        connections.abort_all();
    }
    Ok(())
}
