//! Ships closed segments to S3: hash, PUT, verify with HEAD, write a manifest, mark, prune.
//!
//! Keys: `{prefix}/venue={v}/stream={s}/dt=YYYY-MM-DD/hour=HH/{host}-pN.ndjson.zst` and a
//! sibling `{host}-pN.manifest.json`. A segment is only deleted locally after a verified
//! upload and `local_retain_hours`. Without S3 configured, segments simply stay on disk.

use crate::config::S3Config;
use crate::health::Health;
use crate::now_ms;
use crate::sink::{Segment, SEGMENT_SUFFIX, UPLOADED_SUFFIX};
use anyhow::{anyhow, bail, Context, Result};
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;

const SIGN_TTL: Duration = Duration::from_secs(900);
const IMDS: &str = "http://169.254.169.254";

pub struct Uploader {
    bucket: Bucket,
    prefix: String,
    host_id: String,
    http: reqwest::Client,
    creds: Mutex<Option<(Credentials, i64)>>,
    health: Arc<Health>,
}

pub fn object_key(prefix: &str, host_id: &str, seg: &Segment, suffix: &str) -> String {
    format!(
        "{}/venue={}/stream={}/dt={}/hour={}/{}-p{}{}",
        prefix.trim_matches('/'),
        seg.venue,
        seg.stream,
        seg.dt,
        seg.hour,
        host_id,
        seg.part,
        suffix
    )
}

/// Streaming SHA-256 and size of a file.
pub fn sha256_file(path: &Path) -> Result<(String, u64)> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1 << 16];
    let mut size = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((hex::encode(hasher.finalize()), size))
}

impl Uploader {
    pub fn new(cfg: &S3Config, host_id: &str, health: Arc<Health>) -> Result<Self> {
        let (endpoint, style) = match &cfg.endpoint {
            Some(url) => (url.clone(), UrlStyle::Path),
            None => (format!("https://s3.{}.amazonaws.com", cfg.region), UrlStyle::VirtualHost),
        };
        let bucket = Bucket::new(
            endpoint.parse().map_err(|e| anyhow!("S3 endpoint {endpoint:?}: {e}"))?,
            style,
            cfg.bucket.clone(),
            cfg.region.clone(),
        )
        .map_err(|e| anyhow!("S3 bucket config: {e}"))?;
        Ok(Uploader {
            bucket,
            prefix: cfg.prefix.clone(),
            host_id: host_id.to_string(),
            http: reqwest::Client::builder().timeout(Duration::from_secs(600)).build()?,
            creds: Mutex::new(None),
            health,
        })
    }

    /// Static keys from the environment, else the instance role via IMDSv2 (cached 30 min).
    async fn credentials(&self) -> Result<Credentials> {
        let mut cached = self.creds.lock().await;
        if let Some((creds, until)) = cached.as_ref() {
            if now_ms() < *until {
                return Ok(creds.clone());
            }
        }
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let creds = if let (Some(key), Some(secret)) = (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")) {
            match env("AWS_SESSION_TOKEN") {
                Some(token) => Credentials::new_with_token(key, secret, token),
                None => Credentials::new(key, secret),
            }
        } else {
            let token = self
                .http
                .put(format!("{IMDS}/latest/api/token"))
                .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
                .timeout(Duration::from_secs(3))
                .send()
                .await
                .context("instance metadata token (no AWS_ACCESS_KEY_ID set and not on EC2?)")?
                .error_for_status()?
                .text()
                .await?;
            let base = format!("{IMDS}/latest/meta-data/iam/security-credentials/");
            let get = |url: String| {
                self.http.get(url).header("X-aws-ec2-metadata-token", token.clone()).timeout(Duration::from_secs(3)).send()
            };
            let role = get(base.clone()).await?.error_for_status()?.text().await?;
            let role = role.lines().next().unwrap_or("").trim().to_string();
            if role.is_empty() {
                bail!("the instance has no IAM role attached");
            }
            let body = get(format!("{base}{role}")).await?.error_for_status()?.text().await?;
            let doc: serde_json::Value = serde_json::from_str(&body).context("instance role credentials")?;
            let field = |k: &str| doc[k].as_str().map(str::to_string).ok_or_else(|| anyhow!("role credentials lack {k}"));
            Credentials::new_with_token(field("AccessKeyId")?, field("SecretAccessKey")?, field("Token")?)
        };
        *cached = Some((creds.clone(), now_ms() + 30 * 60_000));
        Ok(creds)
    }

    async fn put(&self, key: &str, body: reqwest::Body, len: u64, creds: &Credentials) -> Result<()> {
        let url = self.bucket.put_object(Some(creds), key).sign(SIGN_TTL);
        let response = self.http.put(url).header("content-length", len).body(body).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            bail!("PUT {key}: {status} {}", text.chars().take(300).collect::<String>());
        }
        Ok(())
    }

    /// Uploads one segment and its manifest; returns the object key.
    pub async fn upload(&self, seg: &Segment) -> Result<String> {
        let path = seg.path.clone();
        let (sha256, size) = tokio::task::spawn_blocking(move || sha256_file(&path)).await??;
        let creds = self.credentials().await?;
        let key = object_key(&self.prefix, &self.host_id, seg, SEGMENT_SUFFIX);

        let file = tokio::fs::File::open(&seg.path).await?;
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::with_capacity(file, 1 << 16));
        self.put(&key, body, size, &creds).await?;

        let head = self.http.head(self.bucket.head_object(Some(&creds), &key).sign(SIGN_TTL)).send().await?;
        let stored = head
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        if !head.status().is_success() || stored != Some(size) {
            bail!("verify {key}: status {} size {stored:?}, expected {size}", head.status());
        }

        let manifest = json!({
            "key": key,
            "venue": seg.venue,
            "stream": seg.stream,
            "dt": seg.dt,
            "hour": seg.hour,
            "part": seg.part,
            "host": self.host_id,
            "bytes": size,
            "sha256": sha256,
            "lines": seg.lines,
            "first_recv_ms": seg.first_recv_ms,
            "last_recv_ms": seg.last_recv_ms,
            "uploaded_ms": now_ms(),
            "compression": "zstd",
        })
        .to_string();
        let manifest_key = object_key(&self.prefix, &self.host_id, seg, ".manifest.json");
        let len = manifest.len() as u64;
        self.put(&manifest_key, manifest.into(), len, &creds).await?;
        Ok(key)
    }

    /// Drains the queue forever, retrying each segment with backoff until it is stored.
    pub async fn run(self: Arc<Self>, mut queue: UnboundedReceiver<Segment>) {
        while let Some(seg) = queue.recv().await {
            let mut attempt = 0u32;
            loop {
                if !seg.path.exists() {
                    break;
                }
                match self.upload(&seg).await {
                    Ok(key) => {
                        let marker = marker_path(&seg.path);
                        if let Err(error) = tokio::fs::write(&marker, key.as_bytes()).await {
                            tracing::warn!(%error, marker = %marker.display(), "cannot write upload marker");
                        }
                        self.health.uploaded.fetch_add(1, Ordering::Relaxed);
                        tracing::info!(%key, "segment uploaded");
                        break;
                    }
                    Err(error) => {
                        self.health.upload_failures.fetch_add(1, Ordering::Relaxed);
                        *self.creds.lock().await = None;
                        let wait = Duration::from_secs((5u64 << attempt.min(6)).min(300));
                        tracing::warn!(path = %seg.path.display(), error = format!("{error:#}"), ?wait, "upload failed; retrying");
                        attempt += 1;
                        tokio::time::sleep(wait).await;
                    }
                }
            }
        }
    }
}

pub fn marker_path(segment: &Path) -> PathBuf {
    PathBuf::from(format!("{}{UPLOADED_SUFFIX}", segment.display()))
}

/// Deletes segments (and markers) whose upload marker is older than `retain`. Returns files removed.
pub fn prune_uploaded(dir: &Path, retain: Duration) -> usize {
    let mut removed = 0;
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            removed += prune_uploaded(&path, retain);
            let _ = std::fs::remove_dir(&path); // only succeeds when empty
            continue;
        }
        let name = path.to_string_lossy().to_string();
        let Some(segment) = name.strip_suffix(UPLOADED_SUFFIX) else { continue };
        let old = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age >= retain);
        if old {
            if std::fs::remove_file(segment).is_ok() || !Path::new(segment).exists() {
                let _ = std::fs::remove_file(&path);
                removed += 1;
            }
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    type Store = Arc<StdMutex<HashMap<String, Vec<u8>>>>;

    /// A tiny path-style S3 stand-in: PUT stores the body, HEAD returns its length.
    async fn fake_s3() -> (String, Store) {
        let store: Store = Arc::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = store.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let store = shared.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 8192];
                    let header_end = loop {
                        let n = socket.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let mut first = head.lines().next().unwrap_or("").split_whitespace();
                    let method = first.next().unwrap_or("").to_string();
                    let path = first.next().unwrap_or("").split('?').next().unwrap_or("").replace("%3D", "=");
                    let len = head
                        .lines()
                        .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap_or(0)))
                        .unwrap_or(0);
                    let mut body = buf[header_end..].to_vec();
                    while method == "PUT" && body.len() < len {
                        let n = socket.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        body.extend_from_slice(&chunk[..n]);
                    }
                    let response = match method.as_str() {
                        "PUT" => {
                            store.lock().unwrap().insert(path, body);
                            "HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string()
                        }
                        "HEAD" => match store.lock().unwrap().get(&path) {
                            Some(b) => format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", b.len()),
                            None => "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string(),
                        },
                        _ => "HTTP/1.1 405 Method Not Allowed\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_string(),
                    };
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        (format!("http://{addr}"), store)
    }

    #[tokio::test]
    async fn uploads_verifies_writes_manifest_and_prunes() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        let (endpoint, store) = fake_s3().await;
        let dir = tempfile::tempdir().unwrap();
        let seg_dir = dir.path().join("polymarket/book_events/dt=2026-11-15");
        std::fs::create_dir_all(&seg_dir).unwrap();
        let path = seg_dir.join("10.p0.ndjson.zst");
        let payload = vec![7u8; 200_000];
        std::fs::write(&path, &payload).unwrap();
        let seg = crate::sink::parse_segment_path(dir.path(), &path).unwrap();

        let cfg = S3Config { bucket: "bkt".into(), region: "eu-west-1".into(), prefix: "raw".into(), endpoint: Some(endpoint) };
        let uploader = Uploader::new(&cfg, "rec-a", Arc::new(Health::new())).unwrap();
        let key = uploader.upload(&seg).await.unwrap();
        assert_eq!(key, "raw/venue=polymarket/stream=book_events/dt=2026-11-15/hour=10/rec-a-p0.ndjson.zst");

        let stored = store.lock().unwrap();
        assert_eq!(stored.get(&format!("/bkt/{key}")).unwrap(), &payload);
        let manifest: serde_json::Value = serde_json::from_slice(
            stored.get("/bkt/raw/venue=polymarket/stream=book_events/dt=2026-11-15/hour=10/rec-a-p0.manifest.json").unwrap(),
        )
        .unwrap();
        assert_eq!(manifest["bytes"], 200_000);
        assert_eq!(manifest["sha256"], sha256_file(&path).unwrap().0);
        drop(stored);

        // Not pruned without a marker; pruned once marked and past retention.
        assert_eq!(prune_uploaded(dir.path(), Duration::ZERO), 0);
        std::fs::write(marker_path(&path), b"k").unwrap();
        assert_eq!(prune_uploaded(dir.path(), Duration::from_secs(3600)), 0);
        assert_eq!(prune_uploaded(dir.path(), Duration::ZERO), 1);
        assert!(!path.exists());
    }
}
