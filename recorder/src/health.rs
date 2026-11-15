//! Liveness bookkeeping and the `/healthz` endpoint.

use crate::now_ms;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Default)]
pub struct Health {
    /// stream key -> last write (ms).
    last_write: Mutex<BTreeMap<String, i64>>,
    /// stream key -> longest silence tolerated (ms) before the check fails.
    expected: Mutex<BTreeMap<String, i64>>,
    pub lines: AtomicU64,
    pub bytes: AtomicU64,
    pub uploaded: AtomicU64,
    pub upload_failures: AtomicU64,
    started_ms: AtomicU64,
}

impl Health {
    pub fn new() -> Self {
        let h = Health::default();
        h.started_ms.store(now_ms() as u64, Ordering::Relaxed);
        h
    }

    pub fn wrote(&self, key: &str, ms: i64, bytes: usize) {
        self.lines.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        let mut last = self.last_write.lock().unwrap();
        match last.get_mut(key) {
            Some(v) => *v = ms,
            None => {
                last.insert(key.to_string(), ms);
            }
        }
    }

    /// Declares that `key` must be written at least every `max_silence_ms`.
    pub fn expect(&self, key: &str, max_silence_ms: i64) {
        self.expected.lock().unwrap().insert(key.to_string(), max_silence_ms);
    }

    /// `(healthy, report)` at time `now`. A stream that never wrote is measured from start.
    pub fn check(&self, now: i64) -> (bool, serde_json::Value) {
        let last = self.last_write.lock().unwrap();
        let expected = self.expected.lock().unwrap();
        let started = self.started_ms.load(Ordering::Relaxed) as i64;
        let mut ok = true;
        let mut streams = serde_json::Map::new();
        for (key, max) in expected.iter() {
            let seen = last.get(key).copied();
            let silent = now - seen.unwrap_or(started);
            let healthy = silent <= *max;
            ok &= healthy;
            streams.insert(
                key.clone(),
                json!({ "last_write_ms": seen, "silent_ms": silent, "max_silence_ms": max, "ok": healthy }),
            );
        }
        let report = json!({
            "ok": ok,
            "now_ms": now,
            "lines": self.lines.load(Ordering::Relaxed),
            "bytes": self.bytes.load(Ordering::Relaxed),
            "uploaded_segments": self.uploaded.load(Ordering::Relaxed),
            "upload_failures": self.upload_failures.load(Ordering::Relaxed),
            "streams": streams,
        });
        (ok, report)
    }
}

/// Minimal HTTP server: any request gets the health report, 200 or 503.
pub async fn serve(health: std::sync::Arc<Health>, port: u16) -> anyhow::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "health endpoint listening");
    loop {
        let (mut socket, _) = listener.accept().await?;
        let health = health.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let (ok, report) = health.check(now_ms());
            let body = report.to_string();
            let status = if ok { "200 OK" } else { "503 Service Unavailable" };
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fails_when_an_expected_stream_is_silent() {
        let h = Health::new();
        h.expect("polymarket/book_events", 60_000);
        let now = now_ms();
        h.wrote("polymarket/book_events", now, 10);
        assert!(h.check(now + 30_000).0);
        let (ok, report) = h.check(now + 61_000);
        assert!(!ok);
        assert_eq!(report["streams"]["polymarket/book_events"]["ok"], false);
        // Streams nobody declared never fail the check.
        h.wrote("polymarket/universe", now, 10);
        assert!(h.check(now + 30_000).0);
    }
}
