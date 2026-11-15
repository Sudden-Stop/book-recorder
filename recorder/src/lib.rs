//! book-recorder: raw order book capture for Polymarket and Kalshi.
//!
//! Every inbound frame is written as received (plus local receive time and a
//! connection id) to an hourly zstd NDJSON segment; closed segments are hashed
//! and shipped to S3. Nothing is interpreted on the hot path beyond what a
//! venue needs to stay subscribed.

pub mod config;
pub mod health;
pub mod kalshi;
pub mod pairs;
pub mod polymarket;
pub mod select;
pub mod sink;
pub mod state;
pub mod uploader;
pub mod ws;

use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `(YYYY-MM-DD, HH)` in UTC for a millisecond timestamp.
pub fn utc_date_hour(ms: i64) -> (String, String) {
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let hour = secs.rem_euclid(86_400) / 3600;
    let (y, m, d) = civil_from_days(days);
    (format!("{y:04}-{m:02}-{d:02}"), format!("{hour:02}"))
}

/// Days since 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's algorithm).
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

/// Inverse of [`civil_from_days`].
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Parses the leading `YYYY-MM-DDTHH:MM:SS` of an ISO-8601 UTC timestamp to epoch ms.
pub fn parse_iso_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 10 {
        return None;
    }
    let num = |a: usize, z: usize| s.get(a..z)?.parse::<i64>().ok();
    let (y, m, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (hh, mm, ss) = if b.len() >= 19 {
        (num(11, 13)?, num(14, 16)?, num(17, 19)?)
    } else {
        (0, 0, 0)
    };
    Some((days_from_civil(y, m as u32, d as u32) * 86_400 + hh * 3600 + mm * 60 + ss) * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_hour_round_trip() {
        // 2026-11-15T11:58:00Z
        let ms = parse_iso_ms("2026-11-15T11:58:00Z").unwrap();
        assert_eq!(utc_date_hour(ms), ("2026-11-15".to_string(), "11".to_string()));
        assert_eq!(utc_date_hour(0), ("1970-01-01".to_string(), "00".to_string()));
        assert_eq!(utc_date_hour(951_782_400_000).0, "2000-02-29");
        assert_eq!(parse_iso_ms("2026-11-15"), Some(days_from_civil(2026, 11, 15) * 86_400_000));
    }
}
