//! A connection against a local WebSocket server: subscribe on open, record frames, skip
//! pongs, apply a mid-stream subscription change, and reconnect after the server hangs up.

use book_recorder::health::Health;
use book_recorder::polymarket::MarketProto;
use book_recorder::sink::{Segment, Sink};
use book_recorder::ws::{run_connection, Cmd};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::io::Read;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

fn lines(seg: &Segment) -> Vec<Value> {
    let mut text = String::new();
    zstd::stream::read::Decoder::new(std::fs::File::open(&seg.path).unwrap()).unwrap().read_to_string(&mut text).unwrap();
    text.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

#[tokio::test]
async fn records_frames_and_reconnects_after_a_drop() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel::<String>();

    // Connection 1: two frames and a pong, then wait for the client's subscribe and hang up.
    // Connection 2: one frame, then stay open.
    tokio::spawn(async move {
        for round in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let first = ws.next().await.unwrap().unwrap().into_text().unwrap().to_string();
            seen_tx.send(first).unwrap();
            if round == 0 {
                ws.send(Message::Text(r#"[{"event_type":"book","asset_id":"1"}]"#.into())).await.unwrap();
                ws.send(Message::Text("PONG".into())).await.unwrap();
                ws.send(Message::Text(r#"{"event_type":"price_change"}"#.into())).await.unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    if let Message::Text(text) = message {
                        if text.as_str().contains("operation") {
                            seen_tx.send(text.to_string()).unwrap();
                            break;
                        }
                    }
                }
                drop(ws); // abrupt close, no close frame
            } else {
                ws.send(Message::Text(r#"{"event_type":"last_trade_price"}"#.into())).await.unwrap();
                while ws.next().await.is_some() {}
            }
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let (segments_tx, mut segments_rx) = mpsc::unbounded_channel();
    let sink = Arc::new(Sink::new(dir.path(), Arc::new(Health::new()), segments_tx));
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = watch::channel(false);
    let task = tokio::spawn(run_connection(
        Arc::new(MarketProto { url }),
        sink.clone(),
        Arc::new(AtomicU64::new(0)),
        cmd_rx,
        vec!["1".to_string()],
        stop_rx,
    ));

    let wait = Duration::from_secs(10);
    let first = tokio::time::timeout(wait, seen_rx.recv()).await.unwrap().unwrap();
    assert_eq!(first, r#"{"assets_ids":["1"],"type":"market"}"#);
    cmd_tx.send(Cmd::Add(vec!["2".to_string()])).unwrap();
    let change = tokio::time::timeout(wait, seen_rx.recv()).await.unwrap().unwrap();
    assert_eq!(change, r#"{"assets_ids":["2"],"operation":"subscribe"}"#);
    // After the drop the client comes back and subscribes to the full, updated set.
    let again = tokio::time::timeout(wait, seen_rx.recv()).await.unwrap().unwrap();
    assert_eq!(again, r#"{"assets_ids":["1","2"],"type":"market"}"#);

    tokio::time::sleep(Duration::from_millis(300)).await;
    stop_tx.send(true).unwrap();
    tokio::time::timeout(wait, task).await.unwrap().unwrap();
    sink.close_all();

    let segment = segments_rx.try_recv().unwrap();
    assert_eq!((segment.venue.as_str(), segment.stream.as_str()), ("polymarket", "book_events"));
    let recorded = lines(&segment);
    let summary: Vec<String> = recorded
        .iter()
        .map(|l| match l.get("ctl") {
            Some(ctl) => format!("{}:{}", l["conn"], ctl.as_str().unwrap()),
            None => format!("{}:frame", l["conn"]),
        })
        .collect();
    // The `subscribe` control line is written when the command is handled, which races with
    // the server's first two frames: check where it may be, then the rest in strict order.
    let at = summary.iter().position(|s| s == "1:subscribe").expect("the added asset is logged");
    assert!((1..=3).contains(&at), "subscribe is logged while connection 1 is open: {summary:?}");
    assert_eq!(recorded[at]["assets"], serde_json::json!(["2"]));
    let mut rest = recorded.clone();
    rest.remove(at);
    let mut order = summary.clone();
    order.remove(at);
    assert_eq!(
        order,
        ["1:connect", "1:frame", "1:frame", "1:disconnect", "2:connect", "2:frame", "2:disconnect"],
        "the pong is not recorded and every gap is bracketed by control lines"
    );
    assert_eq!(rest[1]["msg"][0]["event_type"], "book");
    assert_eq!(rest[4]["assets"], serde_json::json!(["1", "2"]));
    assert_eq!(rest[6]["reason"], "shutdown");
    assert_eq!(segment.lines, Some(8));
}

/// A protocol whose server never answers pings, with a short stall timeout.
struct Silent {
    url: String,
}

impl book_recorder::ws::Protocol for Silent {
    fn venue(&self) -> &'static str {
        "test"
    }
    fn stream(&self) -> &'static str {
        "silent"
    }
    fn url(&self) -> String {
        self.url.clone()
    }
    fn on_open(&self, _ids: &[String]) -> Vec<String> {
        vec!["hello".to_string()]
    }
    fn subscribe(&self, _ids: &[String]) -> Vec<String> {
        Vec::new()
    }
    fn unsubscribe(&self, _ids: &[String]) -> Vec<String> {
        Vec::new()
    }
    fn app_ping(&self) -> Option<&'static str> {
        Some("ping")
    }
    fn ping_interval(&self) -> Duration {
        Duration::from_secs(60)
    }
    fn stall_after(&self) -> Duration {
        Duration::from_millis(1_500)
    }
}

#[tokio::test]
async fn a_silent_socket_is_replaced_after_stall_after() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (accepted_tx, mut accepted_rx) = mpsc::unbounded_channel::<tokio::time::Instant>();
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            accepted_tx.send(tokio::time::Instant::now()).unwrap();
            ws.send(Message::Text(r#"{"type":"trades"}"#.into())).await.unwrap();
            held.push(ws); // open but silent from now on: a half-open socket as seen by the client
        }
    });

    let dir = tempfile::tempdir().unwrap();
    let (segments_tx, _segments_rx) = mpsc::unbounded_channel();
    let sink = Arc::new(Sink::new(dir.path(), Arc::new(Health::new()), segments_tx));
    let (_cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = watch::channel(false);
    let task = tokio::spawn(run_connection(Arc::new(Silent { url }), sink, Arc::new(AtomicU64::new(0)), cmd_rx, Vec::new(), stop_rx));

    let first = tokio::time::timeout(Duration::from_secs(5), accepted_rx.recv()).await.unwrap().unwrap();
    let second = tokio::time::timeout(Duration::from_secs(6), accepted_rx.recv()).await.unwrap().unwrap();
    let took = second - first;
    // Declared stalled 1.5-2.5 s after the last frame (1 s watchdog), then a sub-second backoff.
    assert!(took >= Duration::from_millis(1_500) && took < Duration::from_millis(4_000), "reconnected after {took:?}");
    stop_tx.send(true).unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
}
