//! Per-venue state that has to survive a restart: `{data_dir}/state/{venue}.json`.
//!
//! - `sample`: markets picked by the random sample. They stay selected until the venue
//!   stops listing them as open, so each one is followed to its resolution.
//! - `pending`: markets that left tracking before their outcome was known. They are asked
//!   for again, with growing gaps, until the venue reports the outcome.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const HOUR_MS: i64 = 3_600_000;
/// Stop asking after this long; some markets are voided and never get an outcome.
const GIVE_UP_MS: i64 = 45 * 24 * HOUR_MS;
const MAX_GAP_MS: i64 = 24 * HOUR_MS;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pending {
    pub since_ms: i64,
    pub next_ms: i64,
    pub attempts: u32,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct VenueState {
    #[serde(default)]
    pub sample: BTreeSet<String>,
    #[serde(default)]
    pub pending: BTreeMap<String, Pending>,
}

fn path(dir: &Path, venue: &str) -> PathBuf {
    dir.join("state").join(format!("{venue}.json"))
}

impl VenueState {
    /// The saved state, or an empty one when there is none (first start) or it is unreadable.
    pub fn load(dir: &Path, venue: &str) -> Self {
        let file = path(dir, venue);
        let Ok(text) = std::fs::read_to_string(&file) else {
            return Self::default();
        };
        serde_json::from_str(&text).unwrap_or_else(|error| {
            tracing::warn!(file = %file.display(), %error, "unreadable state file; starting empty");
            Self::default()
        })
    }

    /// Written to a temporary file and renamed, so a crash never leaves half a file.
    pub fn save(&self, dir: &Path, venue: &str) -> std::io::Result<()> {
        let file = path(dir, venue);
        if let Some(parent) = file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = file.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec(self)?)?;
        std::fs::rename(&tmp, &file)
    }

    /// Starts waiting for the outcome of `id` (no effect if already waiting).
    pub fn await_outcome(&mut self, id: &str, now_ms: i64) {
        self.pending.entry(id.to_string()).or_insert(Pending { since_ms: now_ms, next_ms: now_ms + HOUR_MS, attempts: 0 });
    }

    /// Up to `max` markets whose next check is due, longest overdue first.
    pub fn due(&self, now_ms: i64, max: usize) -> Vec<String> {
        let mut due: Vec<(&String, &Pending)> = self.pending.iter().filter(|(_, p)| p.next_ms <= now_ms).collect();
        due.sort_by_key(|(id, p)| (p.next_ms, (*id).clone()));
        due.into_iter().take(max).map(|(id, _)| id.clone()).collect()
    }

    /// The outcome of `id` is recorded: stop waiting.
    pub fn settled(&mut self, id: &str) {
        self.pending.remove(id);
    }

    /// Still no outcome: schedules the next check (1 h, 2 h, 4 h … up to daily).
    /// Returns false when the market has been waited on for too long and is dropped.
    pub fn retry(&mut self, id: &str, now_ms: i64) -> bool {
        let Some(p) = self.pending.get_mut(id) else {
            return false;
        };
        if now_ms - p.since_ms > GIVE_UP_MS {
            self.pending.remove(id);
            return false;
        }
        p.attempts += 1;
        p.next_ms = now_ms + (HOUR_MS << p.attempts.min(5)).min(MAX_GAP_MS);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(VenueState::load(dir.path(), "kalshi"), VenueState::default());
        let mut state = VenueState::default();
        state.sample.insert("KXFED-26DEC-H25".into());
        state.await_outcome("KXNBAGAME-26NOV18UTALAL-UTA", 1_000);
        state.save(dir.path(), "kalshi").unwrap();
        assert_eq!(VenueState::load(dir.path(), "kalshi"), state);
        assert_eq!(VenueState::load(dir.path(), "polymarket"), VenueState::default(), "one file per venue");

        std::fs::write(dir.path().join("state/kalshi.json"), b"{not json").unwrap();
        assert_eq!(VenueState::load(dir.path(), "kalshi"), VenueState::default());
    }

    #[test]
    fn outcome_checks_back_off_and_give_up() {
        let mut state = VenueState::default();
        state.await_outcome("a", 0);
        state.await_outcome("a", 500); // already waiting: the clock does not restart
        assert_eq!(state.pending["a"].since_ms, 0);
        assert!(state.due(HOUR_MS - 1, 10).is_empty());
        assert_eq!(state.due(HOUR_MS, 10), vec!["a"]);

        assert!(state.retry("a", HOUR_MS));
        assert_eq!(state.pending["a"].next_ms, 3 * HOUR_MS, "second gap is two hours");
        for _ in 0..10 {
            state.retry("a", 10 * HOUR_MS);
        }
        assert_eq!(state.pending["a"].next_ms, 34 * HOUR_MS, "gaps stop growing at one day");

        state.await_outcome("b", 0);
        state.settled("b");
        assert!(!state.pending.contains_key("b"));

        assert!(!state.retry("a", GIVE_UP_MS + 1), "given up after 45 days");
        assert!(state.pending.is_empty());
        assert!(!state.retry("missing", 0));
    }

    #[test]
    fn due_is_capped_and_oldest_first() {
        let mut state = VenueState::default();
        for (id, at) in [("late", 300), ("early", 100), ("mid", 200)] {
            state.await_outcome(id, at);
        }
        assert_eq!(state.due(HOUR_MS + 300, 2), vec!["early", "mid"]);
    }
}
