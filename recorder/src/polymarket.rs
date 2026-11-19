//! Polymarket: market universe (Gamma), order book socket, trade feed, REST book snapshots.

use crate::config::PolymarketConfig;
use crate::select::Candidate;
use crate::sink::Sink;
use crate::ws::{Protocol, Reaction};
use crate::{now_ms, parse_iso_ms, pairs};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

pub const VENUE: &str = "polymarket";
const PAGE: usize = 100; // Gamma returns at most 100 markets per request

/// CLOB market channel: order book snapshots, price changes, last trade prices, tick changes.
pub struct MarketProto {
    pub url: String,
}

impl Protocol for MarketProto {
    fn venue(&self) -> &'static str {
        VENUE
    }
    fn stream(&self) -> &'static str {
        "book_events"
    }
    fn url(&self) -> String {
        self.url.clone()
    }
    fn on_open(&self, ids: &[String]) -> Vec<String> {
        vec![json!({ "assets_ids": ids, "type": "market" }).to_string()]
    }
    fn subscribe(&self, ids: &[String]) -> Vec<String> {
        vec![json!({ "assets_ids": ids, "operation": "subscribe" }).to_string()]
    }
    fn unsubscribe(&self, ids: &[String]) -> Vec<String> {
        vec![json!({ "assets_ids": ids, "operation": "unsubscribe" }).to_string()]
    }
    fn app_ping(&self) -> Option<&'static str> {
        Some("PING")
    }
    fn inspect(&self, _conn: u64, raw: &str) -> Reaction {
        Reaction { skip: raw == "PONG" || raw == "PING", ..Default::default() }
    }
}

/// Real-time data socket, `activity/trades`: every trade on the venue with the trader wallet
/// and transaction hash. Recorded raw; the publish step decides what happens to wallets.
pub struct ActivityProto {
    pub url: String,
}

impl Protocol for ActivityProto {
    fn venue(&self) -> &'static str {
        VENUE
    }
    fn stream(&self) -> &'static str {
        "activity"
    }
    fn url(&self) -> String {
        self.url.clone()
    }
    fn on_open(&self, _ids: &[String]) -> Vec<String> {
        vec![json!({ "action": "subscribe", "subscriptions": [{ "topic": "activity", "type": "trades" }] }).to_string()]
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
        Duration::from_secs(2)
    }
    /// A live socket carries trades every block (~2 s) and a pong every 2 s; the longest
    /// healthy gap seen in a day of data was 5.2 s. Six seconds of nothing is a dead socket.
    fn stall_after(&self) -> Duration {
        Duration::from_secs(6)
    }
    fn inspect(&self, _conn: u64, raw: &str) -> Reaction {
        let t = raw.trim();
        Reaction { skip: t.is_empty() || t.eq_ignore_ascii_case("pong") || t.eq_ignore_ascii_case("ping"), ..Default::default() }
    }
}

/// Gamma encodes `clobTokenIds` / `outcomes` as JSON strings holding arrays.
fn string_array(value: &Value) -> Vec<String> {
    match value {
        Value::Array(items) => items.iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
        Value::String(text) => serde_json::from_str::<Vec<String>>(text).unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

pub fn condition_id(market: &Value) -> Option<&str> {
    market["conditionId"].as_str().filter(|s| !s.is_empty())
}

/// Still trading: not closed and not deactivated.
pub fn is_open(market: &Value) -> bool {
    market["closed"] != Value::Bool(true) && market["active"] != Value::Bool(false)
}

/// True once the market is closed with its outcome known: UMA reports it resolved, or the
/// outcome prices have settled to 0 and 1.
pub fn resolved(market: &Value) -> bool {
    if market["closed"] != Value::Bool(true) {
        return false;
    }
    let prices: Vec<f64> = string_array(&market["outcomePrices"]).iter().filter_map(|p| p.parse().ok()).collect();
    let settled = !prices.is_empty() && prices.iter().all(|p| *p == 0.0 || *p == 1.0) && prices.iter().sum::<f64>() == 1.0;
    settled || market["umaResolutionStatus"] == "resolved"
}

/// `(token id, outcome)` for every token of a market.
pub fn tokens(market: &Value) -> Vec<(String, String)> {
    let ids = string_array(&market["clobTokenIds"]);
    let outcomes = string_array(&market["outcomes"]);
    ids.into_iter()
        .enumerate()
        .filter(|(_, id)| !id.is_empty())
        .map(|(i, id)| (id, outcomes.get(i).cloned().unwrap_or_default()))
        .collect()
}

/// A Gamma market record as a selection candidate. `None` when it cannot be recorded.
pub fn candidate(market: &Value, paired_games: &HashSet<pairs::GameKey>) -> Option<Candidate> {
    let id = condition_id(market)?;
    if market["enableOrderBook"] == Value::Bool(false) || tokens(market).is_empty() {
        return None;
    }
    let price = match (number(&market["bestBid"]), number(&market["bestAsk"])) {
        (Some(bid), Some(ask)) if ask > 0.0 && ask >= bid => Some((bid + ask) / 2.0),
        _ => number(&market["lastTradePrice"]).filter(|p| *p > 0.0),
    };
    let paired = market["slug"].as_str().and_then(pairs::polymarket_game).is_some_and(|k| paired_games.contains(&k));
    Some(Candidate {
        id: id.to_string(),
        volume_24h: number(&market["volume24hr"]).unwrap_or(0.0),
        price,
        end_ms: market["endDate"].as_str().and_then(parse_iso_ms),
        fee_free: market["feesEnabled"] == Value::Bool(false),
        paired,
    })
}

async fn get_json(http: &reqwest::Client, url: &str) -> Result<Value> {
    let response = http.get(url).timeout(Duration::from_secs(20)).send().await.with_context(|| format!("GET {url}"))?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        bail!("GET {url}: {status}");
    }
    serde_json::from_str(&text).with_context(|| format!("GET {url}: not JSON"))
}

/// Parses one Gamma page; returns the markets and whether another page may follow.
pub fn parse_page(body: Value) -> (Vec<Value>, bool) {
    let Value::Array(rows) = body else {
        return (Vec::new(), false);
    };
    let more = rows.len() >= PAGE;
    (rows.into_iter().filter(|m| condition_id(m).is_some()).collect(), more)
}

/// All open markets by 24 h volume, paging until Gamma runs out or `max_pages` is reached.
///
/// Gamma refuses offsets above 2000 with a 422 ("offset too large"). The list is ordered
/// by volume, so a page that fails after the first one only cuts the low-volume tail:
/// keep what was fetched instead of losing the whole refresh.
pub async fn fetch_active(http: &reqwest::Client, cfg: &PolymarketConfig) -> Result<Vec<Value>> {
    let mut markets = Vec::new();
    let mut seen = HashSet::new();
    for page in 0..cfg.max_pages {
        let url = format!(
            "{}/markets?closed=false&active=true&order=volume24hr&ascending=false&limit={PAGE}&offset={}",
            cfg.gamma_url,
            page * PAGE
        );
        let body = match get_json(http, &url).await {
            Ok(body) => body,
            Err(error) if !markets.is_empty() => {
                tracing::warn!(page, fetched = markets.len(), error = format!("{error:#}"), "gamma page failed; using the pages already fetched");
                break;
            }
            Err(error) => return Err(error),
        };
        let (rows, more) = parse_page(body);
        for market in rows {
            if seen.insert(condition_id(&market).unwrap_or_default().to_string()) {
                markets.push(market);
            }
        }
        if !more {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(markets)
}

/// The latest record of specific markets, open or closed. Used for markets that left
/// tracking (outcome and close time) and for followed markets that fell out of the
/// volume-ordered listing but may still be open.
pub async fn fetch_final(http: &reqwest::Client, cfg: &PolymarketConfig, ids: &[String]) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    for chunk in ids.chunks(20) {
        let query: Vec<String> = chunk.iter().map(|id| format!("condition_ids={id}")).collect();
        for closed in ["true", "false"] {
            let url = format!("{}/markets?{}&closed={closed}&limit={PAGE}", cfg.gamma_url, query.join("&"));
            if let Value::Array(rows) = get_json(http, &url).await? {
                out.extend(rows);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(out)
}

/// Polls the CLOB REST book of every tracked token once per `period`, as an independent
/// reference for replay checks: `{"recv_ms","req_ms","asset","status","msg":<book>}`.
pub async fn snapshot_loop(
    http: reqwest::Client,
    cfg: PolymarketConfig,
    sink: Arc<Sink>,
    tokens: watch::Receiver<Arc<Vec<String>>>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let list = tokens.borrow().clone();
        let gap = if list.is_empty() {
            Duration::from_secs(5)
        } else {
            Duration::from_millis((cfg.snapshot_period_ms / list.len() as u64).max(150))
        };
        if list.is_empty() {
            tokio::select! {
                _ = tokio::time::sleep(gap) => continue,
                _ = shutdown.changed() => return,
            }
        }
        for asset in list.iter() {
            let req_ms = now_ms();
            let url = format!("{}/book?token_id={asset}", cfg.clob_url);
            match http.get(&url).timeout(Duration::from_secs(10)).send().await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let body = response.text().await.unwrap_or_default();
                    let recv_ms = now_ms();
                    let fields = json!({ "recv_ms": recv_ms, "req_ms": req_ms, "asset": asset, "status": status });
                    sink.tagged(VENUE, "book_snapshots", recv_ms, &fields, &body);
                }
                Err(error) => tracing::debug!(%asset, %error, "book snapshot failed"),
            }
            tokio::select! {
                _ = tokio::time::sleep(gap) => {}
                _ = shutdown.changed() => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market(i: usize) -> Value {
        json!({
            "conditionId": format!("0x{i:04}"),
            "slug": "nba-uta-lal-2026-11-18",
            "clobTokenIds": "[\"111\",\"222\"]",
            "outcomes": "[\"Jazz\",\"Lakers\"]",
            "volume24hr": 1234.5,
            "bestBid": 0.41, "bestAsk": 0.43,
            "endDate": "2026-11-19T03:00:00Z",
            "feesEnabled": true
        })
    }

    #[test]
    fn a_full_page_means_more_pages() {
        let (rows, more) = parse_page(Value::Array((0..PAGE).map(market).collect()));
        assert_eq!((rows.len(), more), (100, true));
        let (rows, more) = parse_page(Value::Array((0..37).map(market).collect()));
        assert_eq!((rows.len(), more), (37, false));
        assert_eq!(parse_page(json!({"error": "x"})), (Vec::new(), false));
    }

    /// Serves full pages up to offset 100 and Gamma's 422 beyond it.
    async fn fake_gamma() -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buf[..n]).to_string();
                    let offset: usize =
                        head.split("offset=").nth(1).and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next()).and_then(|s| s.parse().ok()).unwrap_or(0);
                    let (status, body) = if offset <= 100 {
                        ("200 OK", Value::Array((offset..offset + PAGE).map(market).collect()).to_string())
                    } else {
                        ("422 Unprocessable Entity", r#"{"type":"validation error","error":"offset too large"}"#.to_string())
                    };
                    let response = format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        format!("http://{addr}")
    }

    fn config(gamma_url: String, max_pages: usize) -> PolymarketConfig {
        PolymarketConfig {
            enabled: true,
            gamma_url,
            clob_url: String::new(),
            market_ws_url: String::new(),
            live_ws_url: String::new(),
            activity_enabled: false,
            activity_connections: 1,
            selection: crate::config::SelectionConfig { top_n: 1, fee_free_max: 0, fee_free_min_volume: 0.0, sample_n: 0, sample_min_volume: 0.0 },
            paired_max: 0,
            max_pages,
            shard_size: 1,
            snapshot_period_ms: 1,
        }
    }

    #[tokio::test]
    async fn a_refused_deep_page_keeps_the_pages_already_fetched() {
        let url = fake_gamma().await;
        let http = reqwest::Client::new();
        let markets = fetch_active(&http, &config(url.clone(), 10)).await.unwrap();
        assert_eq!(markets.len(), 200, "two good pages survive the 422 on the third");

        // A failure on the very first page is still an error: there is nothing to fall back on.
        let dead = config("http://127.0.0.1:1".to_string(), 10);
        assert!(fetch_active(&http, &dead).await.is_err());
    }

    #[test]
    fn candidate_reads_gamma_fields() {
        let m = market(1);
        assert_eq!(tokens(&m), vec![("111".to_string(), "Jazz".to_string()), ("222".to_string(), "Lakers".to_string())]);
        let c = candidate(&m, &HashSet::new()).unwrap();
        assert_eq!(c.id, "0x0001");
        assert!((c.price.unwrap() - 0.42).abs() < 1e-9);
        assert!(!c.fee_free && !c.paired);
        assert_eq!(c.end_ms, parse_iso_ms("2026-11-19T03:00:00Z"));

        let games: HashSet<_> = [pairs::polymarket_game("nba-uta-lal-2026-11-18").unwrap()].into();
        assert!(candidate(&m, &games).unwrap().paired);

        let mut free = market(2);
        free["feesEnabled"] = json!(false);
        free["volume24hr"] = json!("88.5");
        let c = candidate(&free, &HashSet::new()).unwrap();
        assert!(c.fee_free);
        assert_eq!(c.volume_24h, 88.5);

        // Open, closed without an outcome yet, and resolved.
        assert!(is_open(&m) && !resolved(&m));
        let mut closed = market(4);
        closed["closed"] = json!(true);
        closed["outcomePrices"] = json!("[\"0.62\",\"0.38\"]");
        assert!(!is_open(&closed) && !resolved(&closed), "closed but still at trading prices");
        closed["outcomePrices"] = json!("[\"1\",\"0\"]");
        assert!(resolved(&closed));
        closed["outcomePrices"] = json!("[\"0.5\",\"0.5\"]");
        closed["umaResolutionStatus"] = json!("resolved");
        assert!(resolved(&closed), "a split resolution counts once UMA reports it");

        let mut no_book = market(3);
        no_book["enableOrderBook"] = json!(false);
        assert!(candidate(&no_book, &HashSet::new()).is_none());
    }

    #[test]
    fn market_socket_messages_match_the_venue_protocol() {
        let p = MarketProto { url: "wss://x".into() };
        let ids = vec!["1".to_string(), "2".to_string()];
        assert_eq!(p.on_open(&ids), vec![r#"{"assets_ids":["1","2"],"type":"market"}"#]);
        assert_eq!(p.subscribe(&ids), vec![r#"{"assets_ids":["1","2"],"operation":"subscribe"}"#]);
        assert!(p.inspect(1, "PONG").skip);
        assert!(!p.inspect(1, "[]").skip);
        assert!(ActivityProto { url: "wss://y".into() }.inspect(1, " pong ").skip);
    }
}
