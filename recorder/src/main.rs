use anyhow::Result;
use book_recorder::config::{Config, KalshiConfig, PolymarketConfig};
use book_recorder::health::{self, Health};
use book_recorder::select::{select, Tracked};
use book_recorder::sink::Sink;
use book_recorder::state::VenueState;
use book_recorder::uploader::{prune_uploaded, Uploader};
use book_recorder::ws::ShardSet;
use book_recorder::{kalshi, now_ms, pairs, polymarket, utc_date_hour};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
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
    let matched = pairs::matches(games.keys(), &events);
    // Each pair names the rule that produced it; only `exact` is a confident match.
    let rows: Vec<Value> = matched
        .iter()
        .map(|(key, event, rule)| {
            json!({
                "league": key.league, "date": key.date, "teams": [key.teams.0, key.teams.1],
                "rule": rule, "confidence": if *rule == pairs::EXACT { "exact" } else { "candidate" },
                "polymarket": games[key],
                "kalshi": { "event_ticker": event, "market_tickers": events[event] },
            })
        })
        .collect();
    let now = now_ms();
    sink.json(CROSS, "pairs", now, &json!({ "ts_ms": now, "rule": "league+date+team_codes", "confidence": "per_pair", "pairs": rows }));
}

/// How many awaited outcomes are asked for per refresh.
const OUTCOME_BATCH: usize = 200;

fn kalshi_ticker(market: &Value) -> Option<&str> {
    market["ticker"].as_str()
}

/// Records what the venue says about markets that left tracking (`first`) or whose outcome
/// is still awaited: `final` once the outcome is known; otherwise `removed` the first time,
/// and another check later. Returns how many outcomes were recorded.
fn record_outcomes(
    venue: &str,
    sink: &Sink,
    state: &mut VenueState,
    ids: &[String],
    rows: &[Value],
    id_of: fn(&Value) -> Option<&str>,
    resolved: fn(&Value) -> bool,
    first: bool,
) -> usize {
    let mut recorded = 0;
    for id in ids {
        let now = now_ms();
        let mut mine = rows.iter().filter(|m| id_of(m) == Some(id.as_str()));
        if let Some(market) = mine.clone().find(|m| resolved(m)) {
            sink.json(venue, "markets", now, &json!({ "ts_ms": now, "reason": "final", "market": market }));
            state.settled(id);
            recorded += 1;
        } else if first {
            if let Some(market) = mine.next() {
                sink.json(venue, "markets", now, &json!({ "ts_ms": now, "reason": "removed", "market": market }));
            }
            state.await_outcome(id, now);
        } else if !state.retry(id, now) {
            tracing::warn!(venue, market = %id, "no outcome after 45 days; no longer asking");
        }
    }
    recorded
}

fn save_state(state: &VenueState, data_dir: &Path, venue: &str) {
    if let Err(error) = state.save(data_dir, venue) {
        tracing::warn!(venue, %error, "cannot save the venue state");
    }
}

async fn polymarket_task(
    cfg: PolymarketConfig,
    data_dir: PathBuf,
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
        for i in 0..cfg.activity_connections {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            // Keep the command sender alive for the life of the task.
            std::mem::forget(tx);
            let (activity, sink, conn_ids, shutdown) = (activity.clone(), sink.clone(), conn_ids.clone(), shutdown.clone());
            tokio::spawn(async move {
                // Staggered starts, so the sockets do not open (and age) in lockstep.
                tokio::time::sleep(Duration::from_millis(1_500 * i as u64)).await;
                book_recorder::ws::run_connection(activity, sink, conn_ids, rx, Vec::new(), shutdown).await;
            });
        }
    }

    let mut tracked = Tracked::new(grace_ms);
    let mut known: HashMap<String, Value> = HashMap::new();
    let mut last_daily = String::new();
    let mut state = VenueState::load(&data_dir, venue);
    loop {
        let wait = match polymarket::fetch_active(&http, &cfg).await {
            Ok(mut markets) => {
                let now = now_ms();
                let date = utc_date_hour(now).0;

                // A followed market that fell out of the volume-ordered listing may still be
                // open: ask for it by id so it stays a candidate until it really closes.
                let listed: HashSet<&str> = markets.iter().filter_map(polymarket::condition_id).collect();
                let missing: Vec<String> = state.sample.iter().filter(|id| !listed.contains(id.as_str())).cloned().collect();
                let mut unknown: HashSet<String> = HashSet::new();
                if !missing.is_empty() {
                    match polymarket::fetch_final(&http, &cfg, &missing).await {
                        Ok(rows) => markets.extend(rows.into_iter().filter(polymarket::is_open)),
                        Err(error) => {
                            // Cannot tell whether they closed: keep following them.
                            tracing::warn!(error = format!("{error:#}"), "followed markets lookup failed");
                            unknown = missing.into_iter().collect();
                        }
                    }
                }
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
                    pairs::matches(games.keys(), &events).into_iter().map(|(k, _, _)| k).collect()
                };
                *shared.polymarket_games.lock().unwrap() = games;

                let candidates: Vec<_> = markets.iter().filter_map(|m| polymarket::candidate(m, &paired)).collect();
                let followed: HashSet<String> = state.sample.iter().cloned().collect();
                let picked = select(&candidates, &cfg.selection, cfg.paired_max, &followed, now);
                let ids: Vec<String> = picked.iter().map(|s| s.id.clone()).collect();
                state.sample = next_sample(&state.sample, &candidates, &picked, &unknown);
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
                    "sample_followed": state.sample.len(), "awaiting_outcome": state.pending.len(),
                }));

                let daily = last_daily != date;
                for id in tracked.ids() {
                    let reason = if diff.added.contains(&id) { "added" } else if daily { "daily" } else { continue };
                    if let Some(market) = known.get(&id) {
                        sink.json(venue, "markets", now, &json!({ "ts_ms": now, "reason": reason, "market": market }));
                    }
                }
                last_daily = date;

                // Outcomes: markets that just left tracking, then those still awaited.
                let due = state.due(now, OUTCOME_BATCH);
                for (ids, first) in [(&diff.removed, true), (&due, false)] {
                    if ids.is_empty() {
                        continue;
                    }
                    match polymarket::fetch_final(&http, &cfg, ids).await {
                        Ok(rows) => {
                            record_outcomes(venue, &sink, &mut state, ids, &rows, polymarket::condition_id, polymarket::resolved, first);
                        }
                        Err(error) => {
                            tracing::warn!(error = format!("{error:#}"), "polymarket outcome lookup failed");
                            if first {
                                ids.iter().for_each(|id| state.await_outcome(id, now)); // ask again later
                            }
                        }
                    }
                }
                diff.removed.iter().for_each(|id| {
                    known.remove(id);
                });
                save_state(&state, &data_dir, venue);

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

/// The followed sample after a refresh: markets still listed (or whose status could not be
/// read, `unknown`) plus the ones the rule just sampled. A market taken by another group
/// this time (say it became top volume) is still followed when it stops qualifying there.
fn next_sample(
    current: &BTreeSet<String>,
    candidates: &[book_recorder::select::Candidate],
    picked: &[book_recorder::select::Selected],
    unknown: &HashSet<String>,
) -> BTreeSet<String> {
    let listed: HashSet<&str> = candidates.iter().map(|c| c.id.as_str()).collect();
    let mut next: BTreeSet<String> =
        current.iter().filter(|id| listed.contains(id.as_str()) || unknown.contains(*id)).cloned().collect();
    next.extend(picked.iter().filter(|s| s.reason == "sample").map(|s| s.id.clone()));
    next
}

async fn kalshi_task(
    cfg: KalshiConfig,
    data_dir: PathBuf,
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
    let mut state = VenueState::load(&data_dir, venue);
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
        // Tracked and followed markets stay in the listing whatever their volume.
        let keep: HashSet<String> = tracked.ids().into_iter().chain(state.sample.iter().cloned()).collect();
        let wait = match rest.open_markets(cfg.max_pages, cfg.selection.sample_min_volume, &keep).await {
            Ok((markets, listed)) => {
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
                    pairs::matches(games.keys(), &events).into_iter().map(|(_, e, _)| e).collect()
                };
                *shared.kalshi_events.lock().unwrap() = events;

                let candidates: Vec<_> = markets.iter().filter_map(|m| kalshi::candidate(m, &paired)).collect();
                let followed: HashSet<String> = state.sample.iter().cloned().collect();
                let picked = select(&candidates, &cfg.selection, cfg.paired_max, &followed, now);
                let ids: Vec<String> = picked.iter().map(|s| s.id.clone()).collect();
                state.sample = next_sample(&state.sample, &candidates, &picked, &HashSet::new());
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
                // `listed` = every open market on the exchange; `pool` = those above the
                // volume floor or in a game, which is what the selection rule saw.
                sink.json(venue, "universe", now, &json!({
                    "ts_ms": now, "listed": listed, "pool": candidates.len(), "selected": selected,
                    "in_grace": diff.in_grace, "added": diff.added, "removed": diff.removed,
                    "sample_followed": state.sample.len(), "awaiting_outcome": state.pending.len(),
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

                // Outcomes: markets that just left tracking, then those still awaited.
                let due = state.due(now, OUTCOME_BATCH);
                for (ids, first) in [(&diff.removed, true), (&due, false)] {
                    if ids.is_empty() {
                        continue;
                    }
                    match rest.markets_by_ticker(ids).await {
                        Ok(rows) => {
                            record_outcomes(venue, &sink, &mut state, ids, &rows, kalshi_ticker, kalshi::resolved, first);
                        }
                        Err(error) => {
                            tracing::warn!(error = format!("{error:#}"), "kalshi outcome lookup failed");
                            if first {
                                ids.iter().for_each(|id| state.await_outcome(id, now)); // ask again later
                            }
                        }
                    }
                }
                diff.removed.iter().for_each(|id| {
                    known.remove(id);
                });
                save_state(&state, &data_dir, venue);

                conn.sync(&tracked.ids());
                write_pairs(&shared, &sink);
                tracing::info!(venue, listed, pool = candidates.len(), markets = tracked.ids().len(), paired = paired.len(),
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
    // A panic in any task (a venue loop, a connection, the uploader) would otherwise kill
    // only that task and leave a process that looks alive but records less. Exit instead:
    // the supervisor restarts the recorder and unsent segments are recovered on start.
    std::panic::set_hook(Box::new(|info| {
        tracing::error!(panic = %info, "panic: exiting so the recorder is restarted");
        std::process::exit(1);
    }));

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
        tokio::spawn(polymarket_task(cfg.polymarket.clone(), cfg.data_dir.clone(), cfg.universe_refresh_ms, cfg.universe_grace_ms, http.clone(),
                                     sink.clone(), conn_ids.clone(), shared.clone(), shutdown.clone()));
    }
    if cfg.kalshi.enabled {
        health.expect("kalshi/book_events", 300_000);
        let task = kalshi_task(cfg.kalshi.clone(), cfg.data_dir.clone(), cfg.universe_refresh_ms, cfg.universe_grace_ms, http.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use book_recorder::select::{Candidate, Selected};
    use std::io::Read;

    fn lines(dir: &Path) -> Vec<Value> {
        let mut out = Vec::new();
        let Ok(days) = std::fs::read_dir(dir.join("polymarket/markets")) else { return out };
        for day in days.flatten() {
            for file in std::fs::read_dir(day.path()).unwrap().flatten() {
                let mut text = String::new();
                zstd::stream::read::Decoder::new(std::fs::File::open(file.path()).unwrap()).unwrap().read_to_string(&mut text).unwrap();
                out.extend(text.lines().map(|l| serde_json::from_str::<Value>(l).unwrap()));
            }
        }
        out
    }

    #[test]
    fn outcomes_are_recorded_when_known_and_asked_for_again_when_not() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = Sink::new(dir.path(), Arc::new(Health::new()), tx);
        let mut state = VenueState::default();
        let open = json!({ "conditionId": "0xopen", "closed": false });
        let done = json!({ "conditionId": "0xdone", "closed": true, "outcomePrices": "[\"1\",\"0\"]" });
        let ids = vec!["0xopen".to_string(), "0xdone".to_string(), "0xgone".to_string()];

        // Leaving tracking: the resolved one is final, the others wait.
        let n = record_outcomes("polymarket", &sink, &mut state, &ids, &[open, done], polymarket::condition_id, polymarket::resolved, true);
        assert_eq!(n, 1);
        let waiting: Vec<&String> = state.pending.keys().collect();
        assert_eq!(waiting, vec!["0xgone", "0xopen"]);

        // A later check finds the open one resolved; the other is still unknown.
        let resolved = json!({ "conditionId": "0xopen", "closed": true, "outcomePrices": "[\"0\",\"1\"]" });
        let due = vec!["0xopen".to_string(), "0xgone".to_string()];
        let n = record_outcomes("polymarket", &sink, &mut state, &due, &[resolved], polymarket::condition_id, polymarket::resolved, false);
        assert_eq!(n, 1);
        assert_eq!(state.pending.keys().collect::<Vec<_>>(), vec!["0xgone"]);
        assert_eq!(state.pending["0xgone"].attempts, 1);

        sink.close_all();
        let written: Vec<(String, String)> = lines(dir.path())
            .iter()
            .map(|l| (l["reason"].as_str().unwrap().to_string(), l["market"]["conditionId"].as_str().unwrap().to_string()))
            .collect();
        assert_eq!(
            written,
            vec![
                ("removed".to_string(), "0xopen".to_string()),
                ("final".to_string(), "0xdone".to_string()),
                ("final".to_string(), "0xopen".to_string()),
            ]
        );
    }

    #[test]
    fn the_followed_sample_drops_delisted_markets_and_keeps_unknown_ones() {
        let cand = |id: &str| Candidate { id: id.into(), volume_24h: 1.0, price: None, end_ms: None, fee_free: false, paired: false };
        let pick = |id: &str, reason: &'static str| Selected { id: id.into(), reason, rank: 1 };
        let current: BTreeSet<String> = ["still", "top-now", "delisted", "lookup-failed"].iter().map(|s| s.to_string()).collect();
        let candidates = vec![cand("still"), cand("top-now"), cand("fresh"), cand("other")];
        let picked = vec![pick("top-now", "top_volume"), pick("still", "sample"), pick("fresh", "sample"), pick("other", "top_volume")];
        let unknown: HashSet<String> = ["lookup-failed".to_string()].into();
        let next: Vec<String> = next_sample(&current, &candidates, &picked, &unknown).into_iter().collect();
        assert_eq!(next, vec!["fresh", "lookup-failed", "still", "top-now"]);
    }
}
