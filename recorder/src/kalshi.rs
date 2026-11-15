//! Kalshi: signed REST and WebSocket access, market universe, order book and trade channels.
//!
//! Checked against docs.kalshi.com on 2026-11-15:
//! - every request carries `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-TIMESTAMP` (ms) and
//!   `KALSHI-ACCESS-SIGNATURE` = base64(RSA-PSS-SHA256(timestamp + METHOD + path)), salt
//!   length = digest length, path without query string;
//! - one connection holds one subscription per channel; a repeated `subscribe` to
//!   `orderbook_delta` adds markets to the existing `sid`;
//! - `update_subscription` with `delete_markets` / `get_snapshot` targets one `sid`;
//! - the server sends `orderbook_snapshot` first, then `orderbook_delta`, each with a
//!   per-subscription `seq`.

use crate::config::KalshiConfig;
use crate::select::Candidate;
use crate::ws::{Protocol, Reaction};
use crate::{now_ms, pairs, parse_iso_ms};
use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::BlindedSigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

pub const VENUE: &str = "kalshi";
const PAGE: usize = 1000;

/// Signs requests with the account's RSA key.
pub struct Signer {
    key_id: String,
    key: RsaPrivateKey,
}

impl Signer {
    pub fn new(key_id: &str, pem: &str) -> Result<Self> {
        let key = RsaPrivateKey::from_pkcs8_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
            .map_err(|e| anyhow!("Kalshi private key is not an RSA PEM (PKCS#8 or PKCS#1): {e}"))?;
        Ok(Signer { key_id: key_id.to_string(), key })
    }

    /// The message Kalshi verifies: timestamp, method and path without the query string.
    pub fn message(timestamp_ms: i64, method: &str, path: &str) -> String {
        format!("{timestamp_ms}{method}{}", path.split('?').next().unwrap_or(path))
    }

    pub fn sign(&self, timestamp_ms: i64, method: &str, path: &str) -> String {
        let signing = BlindedSigningKey::<Sha256>::new(self.key.clone());
        let signature = signing.sign_with_rng(&mut rand::thread_rng(), Self::message(timestamp_ms, method, path).as_bytes());
        base64::engine::general_purpose::STANDARD.encode(signature.to_bytes())
    }

    pub fn headers(&self, method: &str, path: &str) -> Vec<(String, String)> {
        let ts = now_ms();
        vec![
            ("KALSHI-ACCESS-KEY".to_string(), self.key_id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP".to_string(), ts.to_string()),
            ("KALSHI-ACCESS-SIGNATURE".to_string(), self.sign(ts, method, path)),
        ]
    }
}

/// The path part of a URL (`wss://host/trade-api/ws/v2` -> `/trade-api/ws/v2`).
pub fn url_path(url: &str) -> &str {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    rest.find('/').map(|i| &rest[i..]).unwrap_or("/")
}

#[derive(Default)]
struct ConnState {
    conn: u64,
    /// channel -> subscription id, learned from `subscribed` replies.
    sids: HashMap<String, u64>,
    last_seq: HashMap<u64, u64>,
}

/// The single market-data connection: order book deltas for tracked markets, plus every
/// public trade and every market lifecycle event on the exchange.
pub struct KalshiProto {
    pub url: String,
    signer: Signer,
    next_id: AtomicU64,
    state: Mutex<ConnState>,
}

#[derive(Deserialize)]
struct Head {
    #[serde(rename = "type")]
    kind: Option<String>,
    sid: Option<u64>,
    seq: Option<u64>,
    msg: Option<HeadMsg>,
}

#[derive(Deserialize)]
struct HeadMsg {
    channel: Option<String>,
    sid: Option<u64>,
}

impl KalshiProto {
    pub fn new(url: &str, signer: Signer) -> Self {
        KalshiProto { url: url.to_string(), signer, next_id: AtomicU64::new(1), state: Mutex::default() }
    }

    fn id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn orderbook_sid(&self) -> Option<u64> {
        self.state.lock().unwrap().sids.get("orderbook_delta").copied()
    }

    fn update(&self, action: &str, tickers: &[String]) -> Option<String> {
        let sid = self.orderbook_sid()?;
        Some(
            json!({ "id": self.id(), "cmd": "update_subscription",
                    "params": { "sids": [sid], "market_tickers": tickers, "action": action } })
            .to_string(),
        )
    }

    /// Asks for fresh `orderbook_snapshot`s without changing the subscription.
    pub fn snapshot_request(&self, tickers: &[String]) -> Option<String> {
        self.update("get_snapshot", tickers)
    }
}

impl Protocol for KalshiProto {
    fn venue(&self) -> &'static str {
        VENUE
    }
    fn stream(&self) -> &'static str {
        "book_events"
    }
    fn url(&self) -> String {
        self.url.clone()
    }
    fn headers(&self) -> Result<Vec<(String, String)>> {
        Ok(self.signer.headers("GET", url_path(&self.url)))
    }
    fn opened(&self, conn: u64) {
        *self.state.lock().unwrap() = ConnState { conn, ..Default::default() };
    }
    fn on_open(&self, ids: &[String]) -> Vec<String> {
        let mut out = Vec::new();
        if !ids.is_empty() {
            out.extend(self.subscribe(ids));
        }
        out.push(json!({ "id": self.id(), "cmd": "subscribe", "params": { "channels": ["trade"] } }).to_string());
        out.push(json!({ "id": self.id(), "cmd": "subscribe", "params": { "channels": ["market_lifecycle_v2"] } }).to_string());
        out
    }
    fn subscribe(&self, ids: &[String]) -> Vec<String> {
        vec![json!({ "id": self.id(), "cmd": "subscribe",
                     "params": { "channels": ["orderbook_delta"], "market_tickers": ids } })
        .to_string()]
    }
    fn unsubscribe(&self, ids: &[String]) -> Vec<String> {
        self.update("delete_markets", ids).into_iter().collect()
    }
    fn inspect(&self, conn: u64, raw: &str) -> Reaction {
        let Ok(head) = serde_json::from_str::<Head>(raw) else {
            return Reaction::default();
        };
        let mut state = self.state.lock().unwrap();
        if state.conn != conn {
            return Reaction::default();
        }
        if head.kind.as_deref() == Some("subscribed") {
            if let Some(HeadMsg { channel: Some(channel), sid: Some(sid) }) = head.msg {
                state.sids.insert(channel, sid);
            }
            return Reaction::default();
        }
        let mut reaction = Reaction::default();
        if let (Some(sid), Some(seq)) = (head.sid, head.seq) {
            if let Some(last) = state.last_seq.insert(sid, seq) {
                if seq != last + 1 {
                    // Recorded, not acted on: the periodic snapshot request bounds the damage
                    // and the pipeline marks the interval as a gap.
                    reaction.note = Some(("seq_gap", json!({ "sid": sid, "expected": last + 1, "got": seq })));
                }
            }
        }
        reaction
    }
}

fn dollars(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

/// `KXNBAGAME-26NOV18UTALAL` -> `KXNBAGAME`.
pub fn series_of(event_ticker: &str) -> &str {
    event_ticker.split('-').next().unwrap_or(event_ticker)
}

fn volume_24h(market: &Value) -> f64 {
    dollars(&market["volume_24h_fp"]).or_else(|| dollars(&market["volume_24h"])).unwrap_or(0.0)
}

/// Whether a listed market can ever be selected: it trades at least `min_volume` contracts
/// a day (the sample floor, which the volume ranking also clears), or it belongs to a
/// head-to-head game that may be paired with Polymarket whatever its volume.
pub fn worth_keeping(market: &Value, min_volume: f64) -> bool {
    volume_24h(market) >= min_volume || market["event_ticker"].as_str().is_some_and(|e| pairs::kalshi_game(e).is_some())
}

/// True once the exchange has determined the market: `result` is `yes`, `no`, …
pub fn resolved(market: &Value) -> bool {
    market["result"].as_str().is_some_and(|r| !r.is_empty())
}

/// A Kalshi market record as a selection candidate.
pub fn candidate(market: &Value, paired_events: &HashSet<String>) -> Option<Candidate> {
    let ticker = market["ticker"].as_str().filter(|s| !s.is_empty())?;
    let price = match (dollars(&market["yes_bid_dollars"]), dollars(&market["yes_ask_dollars"])) {
        (Some(bid), Some(ask)) if ask > 0.0 && ask >= bid && ask <= 1.0 => Some((bid + ask) / 2.0),
        _ => dollars(&market["last_price_dollars"]).filter(|p| *p > 0.0),
    };
    let end = ["expected_expiration_time", "close_time", "expiration_time"]
        .iter()
        .find_map(|k| market[*k].as_str().and_then(parse_iso_ms));
    Some(Candidate {
        id: ticker.to_string(),
        volume_24h: volume_24h(market),
        price,
        end_ms: end,
        fee_free: false,
        paired: market["event_ticker"].as_str().is_some_and(|e| paired_events.contains(e)),
    })
}

pub struct Rest {
    http: reqwest::Client,
    base: String,
    signer: Signer,
}

impl Rest {
    pub fn new(http: reqwest::Client, cfg: &KalshiConfig, signer: Signer) -> Self {
        Rest { http, base: cfg.rest_url.trim_end_matches('/').to_string(), signer }
    }

    async fn get(&self, path_and_query: &str) -> Result<Value> {
        let url = format!("{}{path_and_query}", self.base);
        let mut request = self.http.get(&url).timeout(Duration::from_secs(30));
        for (name, value) in self.signer.headers("GET", url_path(&url)) {
            request = request.header(name, value);
        }
        let response = request.send().await.with_context(|| format!("GET {url}"))?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            bail!("GET {url}: {status} {}", text.chars().take(200).collect::<String>());
        }
        serde_json::from_str(&text).with_context(|| format!("GET {url}: not JSON"))
    }

    /// Open single-event markets worth considering, paging by cursor, plus how many the
    /// exchange listed in total.
    ///
    /// The full listing is about 190,000 markets and 425 MB of JSON (2026-11-15), most of
    /// them with no volume, so each page is trimmed as it arrives (see [`worth_keeping`])
    /// and the listing never sits in memory whole.
    ///
    /// `keep` are tickers kept whatever their volume: markets already tracked or in the
    /// followed sample, which must stay visible for as long as the exchange lists them.
    pub async fn open_markets(&self, max_pages: usize, min_volume: f64, keep: &HashSet<String>) -> Result<(Vec<Value>, usize)> {
        let mut markets = Vec::new();
        let mut seen = HashSet::new();
        let mut cursor = String::new();
        let mut listed = 0usize;
        for _ in 0..max_pages {
            let mut path = format!("/markets?status=open&mve_filter=exclude&limit={PAGE}");
            if !cursor.is_empty() {
                path.push_str(&format!("&cursor={cursor}"));
            }
            let mut body = self.get(&path).await?;
            let (rows, next) = parse_page(&mut body);
            listed += rows.len();
            let wanted = |m: &Value| worth_keeping(m, min_volume) || m["ticker"].as_str().is_some_and(|t| keep.contains(t));
            for market in rows.into_iter().filter(wanted) {
                if let Some(ticker) = market["ticker"].as_str() {
                    if seen.insert(ticker.to_string()) {
                        markets.push(market);
                    }
                }
            }
            if next.is_empty() {
                break;
            }
            cursor = next;
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
        Ok((markets, listed))
    }

    /// Current records of specific markets, whatever their status (for final outcomes).
    pub async fn markets_by_ticker(&self, tickers: &[String]) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        for chunk in tickers.chunks(50) {
            let mut body = self.get(&format!("/markets?tickers={}", chunk.join(","))).await?;
            out.extend(parse_page(&mut body).0);
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
        Ok(out)
    }

    /// Series record: carries `fee_type` and `fee_multiplier`.
    pub async fn series(&self, ticker: &str) -> Result<Value> {
        self.get(&format!("/series/{ticker}")).await
    }
}

/// Splits a `/markets` response into its markets and the next cursor ("" when done).
pub fn parse_page(body: &mut Value) -> (Vec<Value>, String) {
    let rows = match body["markets"].take() {
        Value::Array(rows) => rows,
        _ => Vec::new(),
    };
    (rows, body["cursor"].as_str().unwrap_or("").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pss::{Signature, VerifyingKey};
    use rsa::signature::Verifier;

    fn signer() -> Signer {
        Signer { key_id: "key-1".into(), key: RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap() }
    }

    #[test]
    fn signature_verifies_as_rsa_pss_sha256_over_timestamp_method_path() {
        let s = signer();
        assert_eq!(Signer::message(1703123456789, "GET", "/trade-api/v2/markets?limit=5"), "1703123456789GET/trade-api/v2/markets");
        let encoded = s.sign(1703123456789, "GET", "/trade-api/ws/v2");
        let bytes = base64::engine::general_purpose::STANDARD.decode(encoded).unwrap();
        let verifier = VerifyingKey::<Sha256>::new(s.key.to_public_key());
        let signature = Signature::try_from(bytes.as_slice()).unwrap();
        verifier.verify(b"1703123456789GET/trade-api/ws/v2", &signature).unwrap();
        assert!(verifier.verify(b"1703123456789GET/other", &signature).is_err());

        let headers = s.headers("GET", "/trade-api/ws/v2");
        assert_eq!(headers[0], ("KALSHI-ACCESS-KEY".to_string(), "key-1".to_string()));
        assert_eq!(url_path("wss://external-api-ws.kalshi.com/trade-api/ws/v2"), "/trade-api/ws/v2");
    }

    #[test]
    fn tracks_subscription_ids_and_reports_sequence_gaps() {
        let p = KalshiProto::new("wss://x/trade-api/ws/v2", signer());
        p.opened(7);
        let ids = vec!["KXNBAGAME-26NOV18UTALAL-UTA".to_string()];
        let open = p.on_open(&ids);
        assert_eq!(open.len(), 3);
        let first: Value = serde_json::from_str(&open[0]).unwrap();
        assert_eq!(first["cmd"], "subscribe");
        assert_eq!(first["params"]["channels"], json!(["orderbook_delta"]));
        assert_eq!(first["params"]["market_tickers"], json!(ids));

        assert!(p.snapshot_request(&ids).is_none(), "no sid before the server confirms");
        p.inspect(7, r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":4}}"#);
        let request: Value = serde_json::from_str(&p.snapshot_request(&ids).unwrap()).unwrap();
        assert_eq!(request["params"], json!({ "sids": [4], "market_tickers": ids, "action": "get_snapshot" }));
        let gone: Value = serde_json::from_str(&p.unsubscribe(&ids)[0]).unwrap();
        assert_eq!(gone["params"]["action"], "delete_markets");

        assert_eq!(p.inspect(7, r#"{"type":"orderbook_snapshot","sid":4,"seq":1,"msg":{}}"#), Reaction::default());
        assert_eq!(p.inspect(7, r#"{"type":"orderbook_delta","sid":4,"seq":2,"msg":{}}"#), Reaction::default());
        let gap = p.inspect(7, r#"{"type":"orderbook_delta","sid":4,"seq":5,"msg":{}}"#);
        assert_eq!(gap.note, Some(("seq_gap", json!({ "sid": 4, "expected": 3, "got": 5 }))));

        // A new connection starts clean.
        p.opened(8);
        assert!(p.snapshot_request(&ids).is_none());
    }

    #[test]
    fn candidate_reads_fixed_point_fields() {
        let market = json!({
            "ticker": "KXNBAGAME-26NOV18UTALAL-UTA", "event_ticker": "KXNBAGAME-26NOV18UTALAL",
            "yes_bid_dollars": "0.4100", "yes_ask_dollars": "0.4300", "volume_24h_fp": "2.99",
            "expected_expiration_time": "2026-11-19T06:00:00Z", "close_time": "2026-12-02T03:00:00Z"
        });
        let paired: HashSet<String> = ["KXNBAGAME-26NOV18UTALAL".to_string()].into();
        let c = candidate(&market, &paired).unwrap();
        assert!((c.price.unwrap() - 0.42).abs() < 1e-9);
        assert_eq!(c.volume_24h, 2.99);
        assert_eq!(c.end_ms, parse_iso_ms("2026-11-19T06:00:00Z"));
        assert!(c.paired);
        assert_eq!(series_of("KXNBAGAME-26NOV18UTALAL"), "KXNBAGAME");

        let mut body = json!({ "markets": [market], "cursor": "abc" });
        let (rows, cursor) = parse_page(&mut body);
        assert_eq!((rows.len(), cursor.as_str()), (1, "abc"));
    }

    #[test]
    fn the_listing_keeps_only_traded_markets_and_games() {
        let traded = json!({ "ticker": "KXFED-26DEC-H25", "event_ticker": "KXFED-26DEC", "volume_24h_fp": "50.00" });
        let quiet = json!({ "ticker": "KXFED-27DEC-H25", "event_ticker": "KXFED-27DEC", "volume_24h_fp": "49.99" });
        let untraded = json!({ "ticker": "KXRAIN-26NOV16-T1", "event_ticker": "KXRAIN-26NOV16" });
        let game = json!({ "ticker": "KXNBAGAME-26NOV18UTALAL-UTA", "event_ticker": "KXNBAGAME-26NOV18UTALAL", "volume_24h_fp": "0.00" });
        assert!(worth_keeping(&traded, 50.0));
        assert!(!worth_keeping(&quiet, 50.0));
        assert!(!worth_keeping(&untraded, 50.0));
        assert!(worth_keeping(&game, 50.0), "a game is kept for pairing even with no volume");

        assert!(!resolved(&traded));
        assert!(!resolved(&json!({ "ticker": "X", "result": "" })));
        assert!(resolved(&json!({ "ticker": "X", "result": "yes" })));
    }
}
