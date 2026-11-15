//! Append-only hourly segments: `{dir}/{venue}/{stream}/dt=YYYY-MM-DD/HH.pN.ndjson.zst`.
//!
//! One line per inbound frame: `{"recv_ms":…,"conn":…,"msg":<frame as received>}`, or a
//! control line `{"recv_ms":…,"conn":…,"ctl":"connect",…}`. A segment is closed when its
//! UTC hour ends and handed to the uploader. Every process start opens a new part
//! (`pN`), so a segment left unterminated by a crash is never appended to.

use crate::health::Health;
use crate::utc_date_hour;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;

pub const SEGMENT_SUFFIX: &str = ".ndjson.zst";
pub const UPLOADED_SUFFIX: &str = ".uploaded";
const ZSTD_LEVEL: i32 = 3;

/// A closed segment ready for upload.
#[derive(Debug, Clone, PartialEq)]
pub struct Segment {
    pub venue: String,
    pub stream: String,
    pub dt: String,
    pub hour: String,
    pub part: u32,
    pub path: PathBuf,
    /// `None` for segments recovered from a previous run.
    pub lines: Option<u64>,
    pub first_recv_ms: Option<i64>,
    pub last_recv_ms: Option<i64>,
}

struct Open {
    dt: String,
    hour: String,
    part: u32,
    path: PathBuf,
    enc: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    lines: u64,
    first: i64,
    last: i64,
}

pub struct Sink {
    dir: PathBuf,
    open: Mutex<HashMap<String, Open>>,
    closed: UnboundedSender<Segment>,
    health: Arc<Health>,
}

/// True when `raw` can be embedded in a JSON line without re-serialising it.
fn embeddable(raw: &str) -> bool {
    let t = raw.trim();
    let b = t.as_bytes();
    let framed = matches!((b.first(), b.last()), (Some(b'{'), Some(b'}')) | (Some(b'['), Some(b']')));
    framed && !t.contains(['\n', '\r'])
}

/// `head` is an unterminated JSON object with at least one member.
fn splice(mut head: String, raw: &str) -> String {
    head.push_str(",\"msg\":");
    if embeddable(raw) {
        head.push_str(raw.trim());
    } else {
        head.push_str(&serde_json::to_string(raw).unwrap_or_else(|_| "null".to_string()));
    }
    head.push('}');
    head
}

impl Sink {
    pub fn new(dir: impl Into<PathBuf>, health: Arc<Health>, closed: UnboundedSender<Segment>) -> Self {
        Sink { dir: dir.into(), open: Mutex::new(HashMap::new()), closed, health }
    }

    /// A raw venue frame, spliced in as received when it is a single-line JSON value.
    pub fn frame(&self, venue: &str, stream: &str, recv_ms: i64, conn: u64, raw: &str) {
        let head = format!("{{\"recv_ms\":{recv_ms},\"conn\":{conn}");
        self.write_line(venue, stream, recv_ms, &splice(head, raw));
    }

    /// A raw response with extra fields of our own (`fields` must be a JSON object), for
    /// example a REST book snapshot tagged with the asset and the request time.
    pub fn tagged(&self, venue: &str, stream: &str, recv_ms: i64, fields: &Value, raw: &str) {
        let mut head = fields.to_string();
        head.pop(); // drop the closing brace; `splice` adds "msg" and closes it
        self.write_line(venue, stream, recv_ms, &splice(head, raw));
    }

    /// A control event (`connect`, `disconnect`, `subscribe`, …) so gaps are visible offline.
    pub fn control(&self, venue: &str, stream: &str, recv_ms: i64, conn: u64, ctl: &str, extra: Value) {
        let mut obj = serde_json::Map::new();
        obj.insert("recv_ms".into(), recv_ms.into());
        obj.insert("conn".into(), conn.into());
        obj.insert("ctl".into(), ctl.into());
        if let Value::Object(map) = extra {
            obj.extend(map);
        }
        self.write_line(venue, stream, recv_ms, &Value::Object(obj).to_string());
    }

    /// A record this process built itself (universe, metadata, REST responses).
    pub fn json(&self, venue: &str, stream: &str, recv_ms: i64, record: &Value) {
        self.write_line(venue, stream, recv_ms, &record.to_string());
    }

    fn write_line(&self, venue: &str, stream: &str, ms: i64, line: &str) {
        let key = format!("{venue}/{stream}");
        let (dt, hour) = utc_date_hour(ms);
        let mut open = self.open.lock().unwrap();
        let stale = open.get(&key).is_some_and(|o| o.dt != dt || o.hour != hour);
        if stale {
            if let Some(old) = open.remove(&key) {
                self.finish(venue, stream, old);
            }
        }
        if !open.contains_key(&key) {
            match self.open_segment(venue, stream, &dt, &hour) {
                Ok(seg) => {
                    open.insert(key.clone(), seg);
                }
                Err(error) => {
                    tracing::error!(%key, %error, "cannot open segment; line dropped");
                    return;
                }
            }
        }
        let seg = open.get_mut(&key).expect("segment just inserted");
        let result = seg.enc.write_all(line.as_bytes()).and_then(|_| seg.enc.write_all(b"\n"));
        if let Err(error) = result {
            tracing::error!(%key, %error, "segment write failed");
            return;
        }
        if seg.lines == 0 {
            seg.first = ms;
        }
        seg.lines += 1;
        seg.last = ms;
        self.health.wrote(&key, ms, line.len() + 1);
    }

    fn open_segment(&self, venue: &str, stream: &str, dt: &str, hour: &str) -> std::io::Result<Open> {
        let dir = self.dir.join(venue).join(stream).join(format!("dt={dt}"));
        fs::create_dir_all(&dir)?;
        let mut part = 0u32;
        let path = loop {
            let candidate = dir.join(format!("{hour}.p{part}{SEGMENT_SUFFIX}"));
            if !candidate.exists() {
                break candidate;
            }
            part += 1;
        };
        let file = File::options().create_new(true).write(true).open(&path)?;
        let enc = zstd::stream::write::Encoder::new(BufWriter::new(file), ZSTD_LEVEL)?;
        Ok(Open { dt: dt.to_string(), hour: hour.to_string(), part, path, enc, lines: 0, first: 0, last: 0 })
    }

    fn finish(&self, venue: &str, stream: &str, seg: Open) {
        let Open { dt, hour, part, path, enc, lines, first, last } = seg;
        match enc.finish().and_then(|mut w| w.flush().map(|_| w)) {
            Ok(writer) => {
                if let Ok(file) = writer.into_inner() {
                    let _ = file.sync_all();
                }
            }
            Err(error) => tracing::error!(path = %path.display(), %error, "segment finish failed"),
        }
        let _ = self.closed.send(Segment {
            venue: venue.to_string(),
            stream: stream.to_string(),
            dt,
            hour,
            part,
            path,
            lines: Some(lines),
            first_recv_ms: Some(first),
            last_recv_ms: Some(last),
        });
    }

    /// Closes segments whose hour has ended (quiet streams) and flushes the rest to disk.
    pub fn tick(&self, now_ms: i64) {
        let (dt, hour) = utc_date_hour(now_ms);
        let mut open = self.open.lock().unwrap();
        let stale: Vec<String> =
            open.iter().filter(|(_, o)| o.dt != dt || o.hour != hour).map(|(k, _)| k.clone()).collect();
        for key in stale {
            if let (Some(seg), Some((venue, stream))) = (open.remove(&key), key.split_once('/')) {
                self.finish(venue, stream, seg);
            }
        }
        for seg in open.values_mut() {
            let _ = seg.enc.flush();
        }
    }

    /// Closes everything (graceful shutdown).
    pub fn close_all(&self) {
        let mut open = self.open.lock().unwrap();
        let keys: Vec<String> = open.keys().cloned().collect();
        for key in keys {
            if let (Some(seg), Some((venue, stream))) = (open.remove(&key), key.split_once('/')) {
                self.finish(venue, stream, seg);
            }
        }
    }

    /// Segments left on disk by earlier runs that were never uploaded. Call before writing.
    pub fn recover(&self) -> Vec<Segment> {
        let mut found = Vec::new();
        scan(&self.dir, &self.dir, &mut found);
        found.sort_by(|a, b| (&a.dt, &a.hour, a.part).cmp(&(&b.dt, &b.hour, b.part)));
        for seg in &found {
            let _ = self.closed.send(seg.clone());
        }
        found
    }
}

/// Parses `{root}/{venue}/{stream}/dt=YYYY-MM-DD/HH.pN.ndjson.zst`.
pub fn parse_segment_path(root: &Path, path: &Path) -> Option<Segment> {
    let rel = path.strip_prefix(root).ok()?;
    let parts: Vec<&str> = rel.iter().filter_map(|p| p.to_str()).collect();
    let [venue, stream, dt_dir, file] = parts.as_slice() else {
        return None;
    };
    let dt = dt_dir.strip_prefix("dt=")?;
    let stem = file.strip_suffix(SEGMENT_SUFFIX)?;
    let (hour, part) = stem.split_once(".p")?;
    Some(Segment {
        venue: venue.to_string(),
        stream: stream.to_string(),
        dt: dt.to_string(),
        hour: hour.to_string(),
        part: part.parse().ok()?,
        path: path.to_path_buf(),
        lines: None,
        first_recv_ms: None,
        last_recv_ms: None,
    })
}

fn scan(root: &Path, dir: &Path, out: &mut Vec<Segment>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            scan(root, &path, out);
        } else if path.to_string_lossy().ends_with(SEGMENT_SUFFIX) {
            let marker = PathBuf::from(format!("{}{UPLOADED_SUFFIX}", path.display()));
            if !marker.exists() {
                if let Some(seg) = parse_segment_path(root, &path) {
                    out.push(seg);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_iso_ms;
    use std::io::Read;
    use tokio::sync::mpsc::unbounded_channel;

    fn read_lines(path: &Path) -> Vec<Value> {
        let mut text = String::new();
        zstd::stream::read::Decoder::new(File::open(path).unwrap()).unwrap().read_to_string(&mut text).unwrap();
        text.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
    }

    #[test]
    fn splices_frames_and_rotates_at_the_hour() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, mut rx) = unbounded_channel();
        let sink = Sink::new(dir.path(), Arc::new(Health::new()), tx);
        let t0 = parse_iso_ms("2026-11-15T10:59:59Z").unwrap();
        sink.frame("polymarket", "book_events", t0, 3, r#"{"event_type":"book","bids":[]}"#);
        sink.frame("polymarket", "book_events", t0 + 1, 3, "not json\nat all");
        sink.control("polymarket", "book_events", t0 + 2, 3, "disconnect", serde_json::json!({"code": 1006}));
        assert!(rx.try_recv().is_err(), "nothing closes inside the hour");

        // First write of the next hour closes the previous segment.
        sink.frame("polymarket", "book_events", t0 + 1000, 4, "[1,2]");
        let seg = rx.try_recv().unwrap();
        assert_eq!((seg.dt.as_str(), seg.hour.as_str(), seg.part), ("2026-11-15", "10", 0));
        assert_eq!(seg.lines, Some(3));
        assert_eq!((seg.first_recv_ms, seg.last_recv_ms), (Some(t0), Some(t0 + 2)));
        assert!(seg.path.ends_with("polymarket/book_events/dt=2026-11-15/10.p0.ndjson.zst"));

        let lines = read_lines(&seg.path);
        assert_eq!(lines[0]["msg"]["event_type"], "book");
        assert_eq!(lines[0]["conn"], 3);
        assert_eq!(lines[1]["msg"], "not json\nat all");
        assert_eq!(lines[2]["ctl"], "disconnect");
        assert_eq!(lines[2]["code"], 1006);

        // A quiet stream is closed by the tick, not only by the next write.
        sink.tick(t0 + 3_600_000 + 1000);
        let seg = rx.try_recv().unwrap();
        assert_eq!(seg.hour, "11");
        assert_eq!(read_lines(&seg.path)[0]["msg"], serde_json::json!([1, 2]));
    }

    #[test]
    fn a_restart_opens_a_new_part_and_recovers_unsent_segments() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = parse_iso_ms("2026-11-15T10:10:00Z").unwrap();
        {
            let (tx, _rx) = unbounded_channel();
            let sink = Sink::new(dir.path(), Arc::new(Health::new()), tx);
            sink.frame("kalshi", "book_events", t0, 1, "{}");
            sink.close_all();
        }
        let (tx, mut rx) = unbounded_channel();
        let sink = Sink::new(dir.path(), Arc::new(Health::new()), tx);
        let recovered = sink.recover();
        assert_eq!(recovered.len(), 1);
        assert_eq!((recovered[0].part, recovered[0].lines), (0, None));
        assert_eq!(rx.try_recv().unwrap().path, recovered[0].path);

        // Uploaded segments are not recovered again.
        fs::write(format!("{}{UPLOADED_SUFFIX}", recovered[0].path.display()), b"").unwrap();
        assert!(sink.recover().is_empty());

        sink.frame("kalshi", "book_events", t0 + 5, 2, "{}");
        sink.close_all();
        let seg = rx.try_recv().unwrap();
        assert_eq!(seg.part, 1, "same hour, new part");
    }
}
