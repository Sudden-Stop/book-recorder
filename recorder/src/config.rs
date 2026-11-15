//! Environment configuration, validated once at start.

use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    /// Identifies this recorder in S3 keys, so a second recorder can run elsewhere.
    pub host_id: String,
    pub health_port: u16,
    pub universe_refresh_ms: u64,
    pub universe_grace_ms: i64,
    pub polymarket: PolymarketConfig,
    pub kalshi: KalshiConfig,
    pub s3: Option<S3Config>,
    /// Hours a verified-uploaded segment stays on local disk.
    pub local_retain_hours: u64,
}

#[derive(Debug, Clone)]
pub struct SelectionConfig {
    /// Markets taken by 24 h volume.
    pub top_n: usize,
    /// Fee-free markets added on top (Polymarket only), with their volume floor.
    pub fee_free_max: usize,
    pub fee_free_min_volume: f64,
    /// Stratified random sample of the rest, with its volume floor.
    pub sample_n: usize,
    pub sample_min_volume: f64,
}

#[derive(Debug, Clone)]
pub struct PolymarketConfig {
    pub enabled: bool,
    pub gamma_url: String,
    pub clob_url: String,
    pub market_ws_url: String,
    pub live_ws_url: String,
    pub activity_enabled: bool,
    pub selection: SelectionConfig,
    /// Game markets whose event also trades on Kalshi, taken first.
    pub paired_max: usize,
    pub max_pages: usize,
    pub shard_size: usize,
    /// Every token gets one REST book snapshot per this period.
    pub snapshot_period_ms: u64,
}

#[derive(Debug, Clone)]
pub struct KalshiConfig {
    pub enabled: bool,
    pub rest_url: String,
    pub ws_url: String,
    pub key_id: Option<String>,
    /// PEM text of the RSA private key (or a path to it via `BR_KALSHI_PRIVATE_KEY_FILE`).
    pub private_key_pem: Option<String>,
    pub selection: SelectionConfig,
    /// Markets of events that also trade on Polymarket, taken first.
    pub paired_max: usize,
    pub max_pages: usize,
    pub snapshot_period_ms: u64,
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    /// Override for S3-compatible stores and tests; path-style when set.
    pub endpoint: Option<String>,
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn get<T: FromStr>(name: &str, default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match var(name) {
        None => Ok(default),
        Some(v) => v.parse::<T>().map_err(|e| anyhow::anyhow!("{name}={v:?}: {e}")),
    }
}

fn flag(name: &str, default: bool) -> Result<bool> {
    match var(name).as_deref() {
        None => Ok(default),
        Some("1" | "true" | "yes") => Ok(true),
        Some("0" | "false" | "no") => Ok(false),
        Some(other) => bail!("{name}={other:?}: expected true or false"),
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let key_pem = match (var("BR_KALSHI_PRIVATE_KEY"), var("BR_KALSHI_PRIVATE_KEY_FILE")) {
            (Some(pem), _) => Some(pem.replace("\\n", "\n")),
            (None, Some(path)) => Some(
                std::fs::read_to_string(&path).with_context(|| format!("reading {path}"))?,
            ),
            (None, None) => None,
        };
        let key_id = var("BR_KALSHI_KEY_ID");
        let kalshi_enabled = flag("BR_KALSHI_ENABLED", key_id.is_some() && key_pem.is_some())?;
        if kalshi_enabled && (key_id.is_none() || key_pem.is_none()) {
            bail!("BR_KALSHI_ENABLED needs BR_KALSHI_KEY_ID and BR_KALSHI_PRIVATE_KEY (or _FILE)");
        }
        let s3 = match var("BR_S3_BUCKET") {
            None => None,
            Some(bucket) => Some(S3Config {
                bucket,
                region: var("BR_S3_REGION")
                    .or_else(|| var("AWS_REGION"))
                    .context("BR_S3_REGION (or AWS_REGION) is required with BR_S3_BUCKET")?,
                prefix: get("BR_S3_PREFIX", "raw".to_string())?,
                endpoint: var("BR_S3_ENDPOINT"),
            }),
        };
        let cfg = Config {
            data_dir: PathBuf::from(get("BR_DATA_DIR", "./data".to_string())?),
            host_id: get("BR_HOST_ID", "rec-a".to_string())?,
            health_port: get("BR_HEALTH_PORT", 8080)?,
            universe_refresh_ms: get("BR_UNIVERSE_REFRESH_MS", 3_600_000)?,
            universe_grace_ms: get("BR_UNIVERSE_GRACE_MS", 6 * 3_600_000)?,
            local_retain_hours: get("BR_LOCAL_RETAIN_HOURS", 24)?,
            polymarket: PolymarketConfig {
                enabled: flag("BR_POLYMARKET_ENABLED", true)?,
                gamma_url: get("BR_GAMMA_URL", "https://gamma-api.polymarket.com".to_string())?,
                clob_url: get("BR_CLOB_URL", "https://clob.polymarket.com".to_string())?,
                market_ws_url: get(
                    "BR_POLYMARKET_WS_URL",
                    "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
                )?,
                live_ws_url: get("BR_POLYMARKET_LIVE_WS_URL", "wss://ws-live-data.polymarket.com".to_string())?,
                activity_enabled: flag("BR_POLYMARKET_ACTIVITY_ENABLED", true)?,
                selection: SelectionConfig {
                    top_n: get("BR_POLYMARKET_TOP_N", 100)?,
                    fee_free_max: get("BR_POLYMARKET_FEE_FREE_MAX", 120)?,
                    fee_free_min_volume: get("BR_POLYMARKET_FEE_FREE_MIN_VOLUME", 2_000.0)?,
                    sample_n: get("BR_POLYMARKET_SAMPLE_N", 80)?,
                    sample_min_volume: get("BR_POLYMARKET_SAMPLE_MIN_VOLUME", 500.0)?,
                },
                paired_max: get("BR_POLYMARKET_PAIRED_MAX", 100)?,
                max_pages: get("BR_POLYMARKET_MAX_PAGES", 40)?,
                shard_size: get("BR_POLYMARKET_SHARD_SIZE", 200)?,
                snapshot_period_ms: get("BR_POLYMARKET_SNAPSHOT_PERIOD_MS", 600_000)?,
            },
            kalshi: KalshiConfig {
                enabled: kalshi_enabled,
                rest_url: get("BR_KALSHI_REST_URL", "https://external-api.kalshi.com/trade-api/v2".to_string())?,
                ws_url: get("BR_KALSHI_WS_URL", "wss://external-api-ws.kalshi.com/trade-api/ws/v2".to_string())?,
                key_id,
                private_key_pem: key_pem,
                selection: SelectionConfig {
                    top_n: get("BR_KALSHI_TOP_N", 120)?,
                    fee_free_max: 0,
                    fee_free_min_volume: 0.0,
                    sample_n: get("BR_KALSHI_SAMPLE_N", 80)?,
                    sample_min_volume: get("BR_KALSHI_SAMPLE_MIN_VOLUME", 50.0)?,
                },
                paired_max: get("BR_KALSHI_PAIRED_MAX", 100)?,
                max_pages: get("BR_KALSHI_MAX_PAGES", 300)?,
                snapshot_period_ms: get("BR_KALSHI_SNAPSHOT_PERIOD_MS", 600_000)?,
            },
            s3,
        };
        if cfg.polymarket.shard_size == 0 {
            bail!("BR_POLYMARKET_SHARD_SIZE must be positive");
        }
        if cfg.host_id.contains('/') {
            bail!("BR_HOST_ID must not contain '/'");
        }
        Ok(cfg)
    }
}
