use anyhow::Result;
use book_recorder::config::{Config, KalshiConfig, PolymarketConfig};
use book_recorder::health::{self, Health};
use book_recorder::select::{select, Tracked};
use book_recorder::sink::Sink;
use book_recorder::uploader::{prune_uploaded, Uploader};
use book_recorder::ws::ShardSet;
use book_recorder::{kalshi, now_ms, pairs, polymarket, utc_date_hour};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

const CROSS: &str = "cross";

/// What each venue last saw of head-to-head games, for matching events across venues.
#[derive(Default)]
struct Shared {
    /// game -> `{condition_id, slug, tokens}`
    polymarket_games: Mutex<HashMap<pairs::GameKey, Value>>,
    /// event ticker -> market tickers
    kalshi_events: Mutex<BTreeMap<String, Vec<String>>>,
}

fn write_pairs(shared: &Shared, sink: &Sink) {
    let games = shared.polymarket_games.lock().unwrap();
    let events = shared.kalshi_events.lock().unwrap();
    let matched = pairs::matches(games.keys(), events.keys());
    let rows: Vec<Value> = matched
        .iter()
        .map(|(key, event)| {
            json!({
                "league": key.league, "date": key.date, "teams": [key.teams.0, key.teams.1],
                "polymarket": games[key],
                "kalshi": { "event_ticker": event, "market_tickers": events[event] },
            })
        })
        .collect();
    let now = now_ms();
    sink.json(CROSS, "pairs", now, &json!({ "ts_ms": now, "rule": "league+date+team_codes", "confidence": "exact", "pairs": rows }));
}

async fn polymarket_task(
    cfg: PolymarketConfig,
    refresh_ms: u64,
    grace_ms: i64,
    http: reqwest::Client,
    sink: Arc<Sink>,
    conn_ids: Arc<AtomicU64>,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
) {
    let venue = polymarket::VENUE;
    let proto = Arc::new(polymarket::MarketProto { url: cfg.market_ws_url.clone() });
    let mut shards = ShardSet::new(proto, sink.clone(), conn_ids.clone(), cfg.shard_size, shutdown.clone());
    let (tokens_tx, tokens_rx) = watch::channel(Arc::new(Vec::<String>::new()));
    tokio::spawn(polymarket::snapshot_loop(http.clone(), cfg.clone(), sink.clone(), tokens_rx, shutdown.clone()));
    if cfg.activity_enabled {
        let activity = Arc::new(polymarket::ActivityProto { url: cfg.live_ws_url.clone() });
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(book_recorder::ws::run_connection(activity, sink.clone(), conn_ids.clone(), rx, Vec::new(), shutdown.clone()));
        // Keep the command sender alive for the life of the task.
        std::mem::forget(_tx);
    }

    let mut tracked = Tracked::new(grace_ms);
    let mut known: HashMap<String, Value> = HashMap::new();
    let mut last_daily = String::new();
    loop {
        let wait = match polymarket::fetch_active(&http, &cfg).await {
            Ok(markets) => {
                let now = now_ms();
                let date = utc_date_hour(now).0;
                let mut games = HashMap::new();
                for market in &markets {
                    if let (Some(key), Some(id)) = (market["slug"].as_str().and_then(pairs::polymarket_game), polymarket::condition_id(market)) {
                        let tokens: Vec<Value> =
                            polymarket::tokens(market).into_iter().map(|(t, o)| json!({ "token_id": t, "outcome": o })).collect();
                        games.insert(key, json!({ "condition_id": id, "slug": market["slug"], "tokens": tokens }));
                    }
                }
                let paired: HashSet<pairs::GameKey> = {
                    let events = shared.kalshi_events.lock().unwrap();
                    pairs::matches(games.keys(), events.keys()).into_iter().map(|(k, _)| k).collect()
                };
                *shared.polymarket_games.lock().unwrap() = games;

                let candidates: Vec<_> = markets.iter().filter_map(|m| polymarket::candidate(m, &paired)).collect();
                let picked = select(&candidates, &cfg.selection, cfg.paired_max, &date, now);
                let ids: Vec<String> = picked.iter().map(|s| s.id.clone()).collect();
                let diff = tracked.update(&ids, now);
                for market in markets {
                    if let Some(id) = polymarket::condition_id(&market).map(str::to_string) {
                        if tracked.contains(&id) {
                            known.insert(id, market);
                        }
                    }
                }

                let selected: Vec<Value> = picked
                    .iter()
                    .filter_map(|s| {
                        let m = known.get(&s.id)?;
                        let tokens: Vec<Value> =
                            polymarket::tokens(m).into_iter().map(|(t, o)| json!({ "token_id": t, "outcome": o })).collect();
                        Some(json!({ "condition_id": s.id, "slug": m["slug"], "reason": s.reason, "rank": s.rank,
                                     "volume24hr": m["volume24hr"], "tokens": tokens }))
                    })
                    .collect();
                sink.json(venue, "universe", now, &json!({
                    "ts_ms": now, "pool": candidates.len(), "selected": selected,
                    "in_grace": diff.in_grace, "added": diff.added, "removed": diff.removed,
                }));

                let daily = last_daily != date;
                for id in tracked.ids() {
                    let reason = if diff.added.contains(&id) { "added" } else if daily { "daily" } else { continue };
                    if let Some(market) = known.get(&id) {
                        sink.json(venue, "markets", now, &json!({ "ts_ms": now, "reason": reason, "market": market }));
                    }
                }
                last_daily = date;
                if !diff.removed.is_empty() {
                    match polymarket::fetch_final(&http, &cfg, &diff.removed).await {
                        Ok(rows) => rows.iter().for_each(|market| {
                            sink.json(venue, "markets", now_ms(), &json!({ "ts_ms": now_ms(), "reason": "final", "market": market }))
                        }),
                        Err(error) => tracing::warn!(error = format!("{error:#}"), "polymarket final records failed"),
                    }
                    diff.removed.iter().for_each(|id| {
                        known.remove(id);
                    });
                }

                let tokens: Vec<String> = tracked
                    .ids()
                    .iter()
                    .filter_map(|id| known.get(id))
                    .flat_map(|m| polymarket::tokens(m).into_iter().map(|(t, _)| t))
                    .collect();
                shards.sync(&tokens);
                let _ = tokens_tx.send(Arc::new(tokens.clone()));
                write_pairs(&shared, &sink);
                tracing::info!(venue, pool = candidates.len(), markets = tracked.ids().len(), tokens = tokens.len(),
                               connections = shards.connections(), added = diff.added.len(), removed = diff.removed.len(), "universe refreshed");
                refresh_ms
            }
            Err(error) => {
                tracing::warn!(venue, error = format!("{error:#}"), "universe refresh failed");
                if tracked.ids().is_empty() { 30_000 } else { refresh_ms }
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(wait)) => {}
            _ = shutdown.changed() => return,
        }
    }
}

async fn kalshi_task(
    cfg: KalshiConfig,
    refresh_ms: u64,
    grace_ms: i64,
    http: reqwest::Client,
    sink: Arc<Sink>,
    conn_ids: Arc<AtomicU64>,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let venue = kalshi::VENUE;
    let (key_id, pem) = (cfg.key_id.clone().unwrap_or_default(), cfg.private_key_pem.clone().unwrap_or_default());
    let rest = kalshi::Rest::new(http, &cfg, kalshi::Signer::new(&key_id, &pem)?);
    let proto = Arc::new(kalshi::KalshiProto::new(&cfg.ws_url, kalshi::Signer::new(&key_id, &pem)?));
    // One connection: Kalshi allows a single order book subscription per connection.
    let mut conn = ShardSet::new(proto.clone(), sink.clone(), conn_ids, usize::MAX, shutdown.clone());

    let mut tracked = Tracked::new(grace_ms);
    let mut known: HashMap<String, Value> = HashMap::new();
    let mut series_seen: HashSet<String> = HashSet::new();
    let mut last_daily = String::new();
    let mut next_refresh = tokio::time::Instant::now();
    let mut snapshots = tokio::time::interval(Duration::from_millis(cfg.snapshot_period_ms));
    snapshots.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(next_refresh) => {}
            _ = snapshots.tick() => {
                for chunk in tracked.ids().chunks(200) {
                    if let Some(request) = proto.snapshot_request(chunk) {
                        conn.send_all(&request);
                    }
                }
                continue;
            }
            _ = shutdown.changed() => return Ok(()),
        }
        let wait = match rest.open_markets(cfg.max_pages).await {
            Ok(markets) => {
                let now = now_ms();
                let date = utc_date_hour(now).0;
                let mut events: BTreeMap<String, Vec<String>> = BTreeMap::new();
                for market in &markets {
                    if let (Some(event), Some(ticker)) = (market["event_ticker"].as_str(), market["ticker"].as_str()) {
                        if pairs::kalshi_game(event).is_some() {
                            events.entry(event.to_string()).or_default().push(ticker.to_string());
                        }
                    }
                }
                let paired: HashSet<String> = {
                    let games = shared.polymarket_games.lock().unwrap();
                    pairs::matches(games.keys(), events.keys()).into_iter().map(|(_, e)| e).collect()
                };
                *shared.kalshi_events.lock().unwrap() = events;

                let candidates: Vec<_> = markets.iter().filter_map(|m| kalshi::candidate(m, &paired)).collect();
                let picked = select(&candidates, &cfg.selection, cfg.paired_max, &date, now);
                let ids: Vec<String> = picked.iter().map(|s| s.id.clone()).collect();
                let diff = tracked.update(&ids, now);
                for market in markets {
                    if let Some(ticker) = market["ticker"].as_str().map(str::to_string) {
                        if tracked.contains(&ticker) {
                            known.insert(ticker, market);
                        }
                    }
                }

                let selected: Vec<Value> = picked
                    .iter()
                    .filter_map(|s| {
                        let m = known.get(&s.id)?;
                        Some(json!({ "ticker": s.id, "event_ticker": m["event_ticker"], "reason": s.reason, "rank": s.rank,
                                     "volume_24h": m["volume_24h_fp"] }))
                    })
                    .collect();
                sink.json(venue, "universe", now, &json!({
                    "ts_ms": now, "pool": candidates.len(), "selected": selected,
                    "in_grace": diff.in_grace, "added": diff.added, "removed": diff.removed,
                }));

                let daily = last_daily != date;
                for id in tracked.ids() {
                    let reason = if diff.added.contains(&id) { "added" } else if daily { "daily" } else { continue };
                    if let Some(market) = known.get(&id) {
                        sink.json(venue, "markets", now, &json!({ "ts_ms": now, "reason": reason, "market": market }));
                    }
                    let series = known.get(&id).and_then(|m| m["event_ticker"].as_str()).map(kalshi::series_of).unwrap_or("").to_string();
                    if !series.is_empty() && series_seen.insert(series.clone()) {
                        match rest.series(&series).await {
                            Ok(record) => sink.json(venue, "series", now_ms(), &json!({ "ts_ms": now_ms(), "series": record })),
                            Err(error) => tracing::debug!(%series, error = format!("{error:#}"), "series record failed"),
                        }
                    }
                }
                if daily {
                    series_seen.clear(); // fee settings can change: refetch once a day
                }
                last_daily = date;
                if !diff.removed.is_empty() {
                    match rest.markets_by_ticker(&diff.removed).await {
                        Ok(rows) => rows.iter().for_each(|market| {
                            sink.json(venue, "markets", now_ms(), &json!({ "ts_ms": now_ms(), "reason": "final", "market": market }))
                        }),
                        Err(error) => tracing::warn!(error = format!("{error:#}"), "kalshi final records failed"),
                    }
                    diff.removed.iter().for_each(|id| {
                        known.remove(id);
                    });
                }

                conn.sync(&tracked.ids());
                write_pairs(&shared, &sink);
                tracing::info!(venue, pool = candidates.len(), markets = tracked.ids().len(), paired = paired.len(),
                               added = diff.added.len(), removed = diff.removed.len(), "universe refreshed");
                refresh_ms
            }
            Err(error) => {
                tracing::warn!(venue, error = format!("{error:#}"), "universe refresh failed");
                if tracked.ids().is_empty() { 60_000 } else { refresh_ms }
            }
        };
        next_refresh = tokio::time::Instant::now() + Duration::from_millis(wait);
    }
}

async fn wait_for_stop() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let level = std::env::var("BR_LOG_LEVEL").ok().and_then(|l| l.parse().ok()).unwrap_or(tracing::Level::INFO);
    tracing_subscriber::fmt().json().with_max_level(level).init();
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cfg = Config::from_env()?;
    let health = Arc::new(Health::new());
    let (segments_tx, mut segments_rx) = tokio::sync::mpsc::unbounded_channel();
    let sink = Arc::new(Sink::new(&cfg.data_dir, health.clone(), segments_tx));
    let recovered = sink.recover().len();
    let (shutdown_tx, shutdown) = watch::channel(false);
    tracing::info!(data_dir = %cfg.data_dir.display(), host = %cfg.host_id, s3 = cfg.s3.as_ref().map(|s| s.bucket.as_str()),
                   polymarket = cfg.polymarket.enabled, kalshi = cfg.kalshi.enabled, recovered, "book-recorder starting");

    match &cfg.s3 {
        Some(s3) => {
            let uploader = Arc::new(Uploader::new(s3, &cfg.host_id, health.clone())?);
            tokio::spawn(uploader.run(segments_rx));
        }
        None => {
            tracing::warn!("BR_S3_BUCKET is not set: segments stay on local disk");
            tokio::spawn(async move { while segments_rx.recv().await.is_some() {} });
        }
    }
    tokio::spawn(health::serve(health.clone(), cfg.health_port));

    {
        let (sink, dir, retain, prune) =
            (sink.clone(), cfg.data_dir.clone(), Duration::from_secs(cfg.local_retain_hours * 3600), cfg.s3.is_some());
        tokio::spawn(async move {
            let mut ticks = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                sink.tick(now_ms());
                ticks += 1;
                if prune && ticks % 120 == 0 {
                    let dir = dir.clone();
                    let removed = tokio::task::spawn_blocking(move || prune_uploaded(&dir, retain)).await.unwrap_or(0);
                    if removed > 0 {
                        tracing::info!(removed, "pruned uploaded segments");
                    }
                }
            }
        });
    }

    let http = reqwest::Client::builder().user_agent("book-recorder/0.1").gzip(true).build()?;
    let conn_ids = Arc::new(AtomicU64::new(0));
    let shared = Arc::new(Shared::default());
    if cfg.polymarket.enabled {
        health.expect("polymarket/book_events", 120_000);
        if cfg.polymarket.activity_enabled {
            health.expect("polymarket/activity", 120_000);
        }
        tokio::spawn(polymarket_task(cfg.polymarket.clone(), cfg.universe_refresh_ms, cfg.universe_grace_ms, http.clone(),
                                     sink.clone(), conn_ids.clone(), shared.clone(), shutdown.clone()));
    }
    if cfg.kalshi.enabled {
        health.expect("kalshi/book_events", 300_000);
        let task = kalshi_task(cfg.kalshi.clone(), cfg.universe_refresh_ms, cfg.universe_grace_ms, http.clone(),
                               sink.clone(), conn_ids.clone(), shared.clone(), shutdown.clone());
        tokio::spawn(async move {
            if let Err(error) = task.await {
                tracing::error!(error = format!("{error:#}"), "kalshi task stopped");
            }
        });
    }

    let mut heartbeat = tokio::time::interval(Duration::from_secs(60));
    heartbeat.tick().await;
    let stop = wait_for_stop();
    tokio::pin!(stop);
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let (ok, report) = health.check(now_ms());
                tracing::info!(ok, report = %report, "heartbeat");
            }
            _ = &mut stop => break,
        }
    }

    tracing::info!("shutting down");
    let _ = shutdown_tx.send(true);
    tokio::time::sleep(Duration::from_millis(750)).await;
    sink.close_all();
    // Give the uploader a moment; anything left is recovered on the next start.
    tokio::time::sleep(Duration::from_secs(if cfg.s3.is_some() { 20 } else { 0 })).await;
    Ok(())
}
