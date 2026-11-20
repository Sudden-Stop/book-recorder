//! One recorded WebSocket connection, shared by every venue.
//!
//! Each connection writes every text frame to the sink before anything else, keeps
//! itself alive, reconnects with jittered backoff, and logs `connect`, `disconnect`,
//! `subscribe` and `unsubscribe` control lines so gaps can be reconstructed offline.

use crate::now_ms;
use crate::sink::Sink;
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::Message;

const RECONNECT_BASE_MS: u64 = 500;
const RECONNECT_MAX_MS: u64 = 10_000;
/// How often a connection checks itself for a stall.
const WATCHDOG: Duration = Duration::from_secs(1);

/// What a venue wants done after seeing one of its frames.
#[derive(Debug, Default, PartialEq)]
pub struct Reaction {
    /// Do not record this frame (application-level pongs).
    pub skip: bool,
    /// Messages to send back on the same connection.
    pub send: Vec<String>,
    /// Drop the connection and start over, with the reason logged as a control line.
    pub reconnect: Option<String>,
    /// A control line to record next to the frame (for example a sequence gap).
    pub note: Option<(&'static str, serde_json::Value)>,
}

/// Venue-specific behaviour of a connection.
pub trait Protocol: Send + Sync + 'static {
    fn venue(&self) -> &'static str;
    fn stream(&self) -> &'static str;
    fn url(&self) -> String;
    /// Extra handshake headers (signed auth), built fresh for every attempt.
    fn headers(&self) -> Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }
    /// Messages sent right after the socket opens, for the full current id set.
    fn on_open(&self, ids: &[String]) -> Vec<String>;
    fn subscribe(&self, ids: &[String]) -> Vec<String>;
    fn unsubscribe(&self, ids: &[String]) -> Vec<String>;
    /// Application-level text ping, if the venue uses one; otherwise a WebSocket Ping frame.
    fn app_ping(&self) -> Option<&'static str> {
        None
    }
    fn ping_interval(&self) -> Duration {
        Duration::from_secs(10)
    }
    /// Silence (no frame of any kind, pongs included) after which the socket is taken as
    /// half-open and replaced. Checked once a second.
    fn stall_after(&self) -> Duration {
        self.ping_interval() * 3
    }
    /// On a stall, open and subscribe the replacement before closing the stalled socket.
    /// Only for streams that tolerate two connections briefly delivering the same frames
    /// (the pipeline keeps one copy).
    fn overlap_on_stall(&self) -> bool {
        false
    }
    /// Called for every text frame before it is recorded. `conn` identifies the connection.
    fn inspect(&self, _conn: u64, _raw: &str) -> Reaction {
        Reaction::default()
    }
    /// The connection with this id opened; reset any per-connection state.
    fn opened(&self, _conn: u64) {}
}

#[derive(Debug)]
pub enum Cmd {
    Add(Vec<String>),
    Remove(Vec<String>),
    /// Send a raw message (for example a snapshot request).
    Send(String),
}

fn backoff(attempt: u32) -> Duration {
    let base = (RECONNECT_BASE_MS << attempt.min(5)).min(RECONNECT_MAX_MS);
    let jitter = rand::thread_rng().gen_range(0..=base / 4);
    Duration::from_millis(base + jitter)
}

type Socket = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Opens the socket (handshake only).
async fn dial<P: Protocol>(proto: &P) -> Result<Socket> {
    let mut request = proto.url().into_client_request()?;
    for (name, value) in proto.headers()? {
        request.headers_mut().insert(HeaderName::from_bytes(name.as_bytes())?, HeaderValue::from_str(&value)?);
    }
    Ok(tokio_tungstenite::connect_async(request).await?.0)
}

/// Sends the opening subscription for the full id set and logs `connect`. False if a send failed.
async fn open<P: Protocol>(proto: &P, sink: &Sink, socket: &mut Socket, conn: u64, ids: &BTreeSet<String>) -> bool {
    let (venue, stream) = (proto.venue(), proto.stream());
    proto.opened(conn);
    let current: Vec<String> = ids.iter().cloned().collect();
    let mut ok = true;
    for message in proto.on_open(&current) {
        ok &= socket.send(Message::Text(message.into())).await.is_ok();
    }
    sink.control(venue, stream, now_ms(), conn, "connect", json!({ "assets": current }));
    tracing::info!(venue, stream, conn, assets = current.len(), "websocket subscribed");
    ok
}

/// Records a data frame as received, acting on no reaction. True if it was recorded.
fn record<P: Protocol>(proto: &P, sink: &Sink, conn: u64, message: &Message) -> bool {
    let recv_ms = now_ms();
    match message {
        Message::Text(text) if !proto.inspect(conn, text.as_str()).skip => {
            sink.frame(proto.venue(), proto.stream(), recv_ms, conn, text.as_str());
            true
        }
        Message::Binary(bytes) => {
            sink.frame(proto.venue(), proto.stream(), recv_ms, conn, &String::from_utf8_lossy(bytes));
            true
        }
        _ => false,
    }
}

/// Runs one connection until `shutdown` flips. `ids` is the initial subscription set.
pub async fn run_connection<P: Protocol>(
    proto: Arc<P>,
    sink: Arc<Sink>,
    conn_ids: Arc<AtomicU64>,
    mut cmds: UnboundedReceiver<Cmd>,
    initial: Vec<String>,
    mut shutdown: watch::Receiver<bool>,
) {
    let (venue, stream) = (proto.venue(), proto.stream());
    let mut ids: BTreeSet<String> = initial.into_iter().collect();
    let mut attempt = 0u32;
    // A replacement already opened and subscribed while the stalled connection was still up.
    let mut handover: Option<(u64, Socket, bool)> = None;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let (conn, mut socket, mut failed) = match handover.take() {
            Some(next) => next,
            None => {
                let conn = conn_ids.fetch_add(1, Ordering::Relaxed) + 1;
                match dial(&*proto).await {
                    Ok(mut socket) => {
                        let ok = open(&*proto, &sink, &mut socket, conn, &ids).await;
                        (conn, socket, !ok)
                    }
                    Err(error) => {
                        let wait = backoff(attempt);
                        attempt += 1;
                        sink.control(venue, stream, now_ms(), conn, "connect_error", json!({ "error": format!("{error:#}") }));
                        tracing::warn!(venue, stream, conn, error = format!("{error:#}"), ?wait, "connect failed");
                        tokio::select! {
                            _ = tokio::time::sleep(wait) => continue,
                            _ = shutdown.changed() => return,
                        }
                    }
                }
            }
        };

        let interval = proto.ping_interval();
        let mut ping = tokio::time::interval(interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let stall_after = proto.stall_after();
        let mut watchdog = tokio::time::interval(WATCHDOG);
        watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_frame = tokio::time::Instant::now();
        let mut reason = serde_json::Value::Null;
        let mut code: Option<u16> = None;
        let mut frames = 0u64;
        let mut stalled = false;

        while !failed {
            tokio::select! {
                message = socket.next() => {
                    let recv_ms = now_ms();
                    last_frame = tokio::time::Instant::now();
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            let raw = text.as_str();
                            let reaction = proto.inspect(conn, raw);
                            if !reaction.skip {
                                sink.frame(venue, stream, recv_ms, conn, raw);
                                frames += 1;
                            }
                            if let Some((ctl, extra)) = reaction.note {
                                sink.control(venue, stream, recv_ms, conn, ctl, extra);
                            }
                            for out in reaction.send {
                                failed |= socket.send(Message::Text(out.into())).await.is_err();
                            }
                            if let Some(why) = reaction.reconnect {
                                reason = json!(why);
                                break;
                            }
                        }
                        Some(Ok(Message::Binary(bytes))) => {
                            sink.frame(venue, stream, recv_ms, conn, &String::from_utf8_lossy(&bytes));
                            frames += 1;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            if let Some(frame) = frame {
                                code = Some(frame.code.into());
                                reason = json!(frame.reason.to_string());
                            }
                            break;
                        }
                        Some(Ok(_)) => {} // Ping / Pong: liveness only
                        Some(Err(error)) => {
                            reason = json!(error.to_string());
                            break;
                        }
                        None => break,
                    }
                }
                cmd = cmds.recv() => {
                    let Some(cmd) = cmd else { return };
                    let (ctl, changed, messages) = match cmd {
                        Cmd::Add(list) => {
                            let new: Vec<String> = list.into_iter().filter(|id| ids.insert(id.clone())).collect();
                            let messages = if new.is_empty() { Vec::new() } else { proto.subscribe(&new) };
                            ("subscribe", new, messages)
                        }
                        Cmd::Remove(list) => {
                            let gone: Vec<String> = list.into_iter().filter(|id| ids.remove(id)).collect();
                            let messages = if gone.is_empty() { Vec::new() } else { proto.unsubscribe(&gone) };
                            ("unsubscribe", gone, messages)
                        }
                        Cmd::Send(message) => ("", Vec::new(), vec![message]),
                    };
                    for message in messages {
                        failed |= socket.send(Message::Text(message.into())).await.is_err();
                    }
                    if !changed.is_empty() {
                        sink.control(venue, stream, now_ms(), conn, ctl, json!({ "assets": changed }));
                    }
                }
                _ = watchdog.tick() => {
                    // Nothing at all (not even a pong) for too long: assume a half-open socket.
                    if last_frame.elapsed() > stall_after {
                        reason = json!("stalled");
                        stalled = true;
                        break;
                    }
                }
                _ = ping.tick() => {
                    let message = match proto.app_ping() {
                        Some(text) => Message::Text(text.into()),
                        None => Message::Ping(Vec::new().into()),
                    };
                    failed |= socket.send(message).await.is_err();
                }
                _ = shutdown.changed() => {
                    let _ = socket.close(None).await;
                    sink.control(venue, stream, now_ms(), conn, "disconnect", json!({ "code": null, "reason": "shutdown" }));
                    return;
                }
            }
        }

        if stalled && proto.overlap_on_stall() {
            // Make before break: open and subscribe the replacement while still reading the
            // stalled socket. When the venue pauses, every socket goes quiet at once; if the
            // stalled one was only quiet, what it delivers meanwhile is still recorded, and
            // there is never a moment without a subscribed socket.
            let next = conn_ids.fetch_add(1, Ordering::Relaxed) + 1;
            let dialing = dial(&*proto);
            tokio::pin!(dialing);
            let mut old_open = true;
            let dialed = loop {
                tokio::select! {
                    result = &mut dialing => break Some(result),
                    message = socket.next(), if old_open => match message {
                        Some(Ok(message)) => frames += u64::from(record(&*proto, &sink, conn, &message)),
                        _ => old_open = false,
                    },
                    _ = shutdown.changed() => break None,
                }
            };
            match dialed {
                Some(Ok(mut new_socket)) => {
                    let ok = open(&*proto, &sink, &mut new_socket, next, &ids).await;
                    handover = Some((next, new_socket, !ok));
                }
                Some(Err(error)) => {
                    sink.control(venue, stream, now_ms(), next, "connect_error", json!({ "error": format!("{error:#}") }));
                    tracing::warn!(venue, stream, conn = next, error = format!("{error:#}"), "replacement connect failed");
                }
                None => {}
            }
        }

        let _ = socket.close(None).await;
        sink.control(venue, stream, now_ms(), conn, "disconnect", json!({ "code": code, "reason": reason, "frames": frames }));
        tracing::warn!(venue, stream, conn, ?code, %reason, frames, "websocket closed");
        if handover.is_some() {
            // The replacement is subscribed to the current set; pending commands reach it
            // from the queue as usual.
            attempt = 0;
            continue;
        }
        // Commands that arrived while disconnected still update the id set.
        while let Ok(cmd) = cmds.try_recv() {
            match cmd {
                Cmd::Add(list) => ids.extend(list),
                Cmd::Remove(list) => list.iter().for_each(|id| {
                    ids.remove(id);
                }),
                Cmd::Send(_) => {}
            }
        }
        // A stall after a good handshake is the venue going quiet, not refusing us: retry at
        // the base delay instead of backing off, or a paused venue resumes while we are away.
        attempt = if frames > 0 || stalled { 0 } else { attempt + 1 };
        tokio::select! {
            _ = tokio::time::sleep(backoff(attempt)) => {}
            _ = shutdown.changed() => return,
        }
    }
}

/// Splits ids across connections of at most `size`, keeping every id on the shard it
/// already has so a universe refresh never moves a live subscription.
#[derive(Debug, Default)]
pub struct Assigner {
    size: usize,
    shards: Vec<BTreeSet<String>>,
}

/// Per-shard changes produced by [`Assigner::sync`].
#[derive(Debug, Default, PartialEq)]
pub struct ShardDiff {
    pub add: Vec<String>,
    pub remove: Vec<String>,
}

impl Assigner {
    pub fn new(size: usize) -> Self {
        Assigner { size: size.max(1), shards: Vec::new() }
    }

    pub fn shards(&self) -> &[BTreeSet<String>] {
        &self.shards
    }

    /// Moves to the target set. Returns one diff per shard (new shards are appended).
    pub fn sync(&mut self, target: &[String]) -> Vec<ShardDiff> {
        let wanted: BTreeSet<&String> = target.iter().collect();
        let mut diffs: Vec<ShardDiff> = self.shards.iter().map(|_| ShardDiff::default()).collect();
        let mut placed: BTreeSet<String> = BTreeSet::new();
        for (i, shard) in self.shards.iter_mut().enumerate() {
            let gone: Vec<String> = shard.iter().filter(|id| !wanted.contains(id)).cloned().collect();
            for id in gone {
                shard.remove(&id);
                diffs[i].remove.push(id);
            }
            placed.extend(shard.iter().cloned());
        }
        for id in target {
            if !placed.insert(id.clone()) {
                continue;
            }
            let slot = match self.shards.iter().position(|s| s.len() < self.size) {
                Some(i) => i,
                None => {
                    self.shards.push(BTreeSet::new());
                    diffs.push(ShardDiff::default());
                    self.shards.len() - 1
                }
            };
            self.shards[slot].insert(id.clone());
            diffs[slot].add.push(id.clone());
        }
        diffs
    }
}

/// A set of sharded connections for one protocol.
pub struct ShardSet<P: Protocol> {
    proto: Arc<P>,
    sink: Arc<Sink>,
    conn_ids: Arc<AtomicU64>,
    shutdown: watch::Receiver<bool>,
    assigner: Assigner,
    senders: Vec<UnboundedSender<Cmd>>,
}

impl<P: Protocol> ShardSet<P> {
    pub fn new(proto: Arc<P>, sink: Arc<Sink>, conn_ids: Arc<AtomicU64>, size: usize, shutdown: watch::Receiver<bool>) -> Self {
        ShardSet { proto, sink, conn_ids, shutdown, assigner: Assigner::new(size), senders: Vec::new() }
    }

    /// Applies a new target id set, spawning connections as needed.
    pub fn sync(&mut self, target: &[String]) {
        for (i, diff) in self.assigner.sync(target).into_iter().enumerate() {
            if i >= self.senders.len() {
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                self.senders.push(tx);
                tokio::spawn(run_connection(
                    self.proto.clone(),
                    self.sink.clone(),
                    self.conn_ids.clone(),
                    rx,
                    diff.add,
                    self.shutdown.clone(),
                ));
                continue;
            }
            if !diff.remove.is_empty() {
                let _ = self.senders[i].send(Cmd::Remove(diff.remove));
            }
            if !diff.add.is_empty() {
                let _ = self.senders[i].send(Cmd::Add(diff.add));
            }
        }
    }

    pub fn send_all(&self, message: &str) {
        for tx in &self.senders {
            let _ = tx.send(Cmd::Send(message.to_string()));
        }
    }

    pub fn connections(&self) -> usize {
        self.senders.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(range: std::ops::Range<u32>) -> Vec<String> {
        range.map(|i| format!("a{i:03}")).collect()
    }

    #[test]
    fn assigner_keeps_ids_on_their_shard() {
        let mut a = Assigner::new(3);
        let first = a.sync(&ids(0..7));
        assert_eq!(first.iter().map(|d| d.add.len()).collect::<Vec<_>>(), vec![3, 3, 1]);
        assert!(first.iter().all(|d| d.remove.is_empty()));

        // Drop two ids from shard 0 and add three: the holes are reused, nothing else moves.
        let mut target = ids(2..7);
        target.extend(ids(10..13));
        let diffs = a.sync(&target);
        assert_eq!(diffs[0].remove, vec!["a000", "a001"]);
        assert_eq!(diffs[0].add, vec!["a010", "a011"]);
        assert!(diffs[1].add.is_empty() && diffs[1].remove.is_empty());
        assert_eq!(diffs[2].add, vec!["a012"]);
        assert!(a.shards().iter().all(|s| s.len() <= 3));
        let total: usize = a.shards().iter().map(|s| s.len()).sum();
        assert_eq!(total, 8);

        // Same target again: no changes.
        assert!(a.sync(&target).iter().all(|d| d.add.is_empty() && d.remove.is_empty()));
    }
}
