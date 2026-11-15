//! The market selection rule, shared by both venues and documented in the dataset card.
//!
//! selected = paired (the other venue lists the same event)
//!          ∪ top `top_n` by 24 h volume
//!          ∪ fee-free markets above a volume floor (Polymarket)
//!          ∪ a stratified sample of the rest, by price bucket × time to resolution.
//!
//! The sample is ordered by a hash of the market id, so it is reproducible and independent
//! of volume rank, and it is sticky: once sampled, a market stays selected until the venue
//! stops listing it, so it is followed to its resolution. A market that drops out of the
//! other groups stays tracked for a grace period so a refresh never cuts it mid-event.

use crate::config::SelectionConfig;
use std::collections::{BTreeMap, HashMap, HashSet};

/// What the rule needs to know about a market, extracted from the venue's own record.
#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub id: String,
    pub volume_24h: f64,
    /// Mid price of the canonical outcome in (0, 1), when known.
    pub price: Option<f64>,
    /// Expected resolution time, when known.
    pub end_ms: Option<i64>,
    pub fee_free: bool,
    pub paired: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Selected {
    pub id: String,
    pub reason: &'static str,
    pub rank: usize,
}

pub fn fnv1a(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ *b as u64).wrapping_mul(0x0000_0100_0000_01b3))
}

/// Five price buckets: [0, .1), [.1, .3), [.3, .7), [.7, .9), [.9, 1]; 5 = unknown.
pub fn price_bucket(price: Option<f64>) -> u8 {
    match price {
        Some(p) if p < 0.1 => 0,
        Some(p) if p < 0.3 => 1,
        Some(p) if p < 0.7 => 2,
        Some(p) if p < 0.9 => 3,
        Some(_) => 4,
        None => 5,
    }
}

/// Four horizons: < 1 day, < 7 days, < 30 days, longer; 4 = unknown.
pub fn horizon_bucket(end_ms: Option<i64>, now_ms: i64) -> u8 {
    const DAY: i64 = 86_400_000;
    match end_ms.map(|e| e - now_ms) {
        Some(d) if d < DAY => 0,
        Some(d) if d < 7 * DAY => 1,
        Some(d) if d < 30 * DAY => 2,
        Some(_) => 3,
        None => 4,
    }
}

/// Applies the rule. `paired_max` caps the paired group; `followed` is the sample so far.
pub fn select(
    candidates: &[Candidate],
    cfg: &SelectionConfig,
    paired_max: usize,
    followed: &HashSet<String>,
    now_ms: i64,
) -> Vec<Selected> {
    let mut by_volume: Vec<&Candidate> = candidates.iter().collect();
    by_volume.sort_by(|a, b| b.volume_24h.total_cmp(&a.volume_24h).then_with(|| a.id.cmp(&b.id)));

    let mut out: Vec<Selected> = Vec::new();
    let mut taken: HashSet<String> = HashSet::new();
    let mut take = |c: &Candidate, reason: &'static str, rank: usize, out: &mut Vec<Selected>| -> bool {
        let fresh = taken.insert(c.id.clone());
        if fresh {
            out.push(Selected { id: c.id.clone(), reason, rank });
        }
        fresh
    };

    let mut n = 0;
    for (rank, c) in by_volume.iter().enumerate() {
        if n >= paired_max {
            break;
        }
        if c.paired && take(c, "paired", rank + 1, &mut out) {
            n += 1;
        }
    }
    n = 0;
    for (rank, c) in by_volume.iter().enumerate() {
        if n >= cfg.top_n {
            break;
        }
        if take(c, "top_volume", rank + 1, &mut out) {
            n += 1;
        }
    }
    n = 0;
    for (rank, c) in by_volume.iter().enumerate() {
        if n >= cfg.fee_free_max || c.volume_24h < cfg.fee_free_min_volume {
            break;
        }
        if c.fee_free && take(c, "fee_free", rank + 1, &mut out) {
            n += 1;
        }
    }

    // Stratified sample. A market already in the sample stays for as long as the venue
    // lists it, whatever its volume, price or horizon have become: that is what lets the
    // dataset follow a market to its resolution.
    let rank_of: HashMap<&str, usize> = by_volume.iter().enumerate().map(|(i, c)| (c.id.as_str(), i + 1)).collect();
    let stratum = |c: &Candidate| (price_bucket(c.price), horizon_bucket(c.end_ms, now_ms));
    let mut filled: HashMap<(u8, u8), usize> = HashMap::new();
    n = 0;
    for c in &by_volume {
        if followed.contains(&c.id) && take(c, "sample", rank_of[c.id.as_str()], &mut out) {
            n += 1;
            *filled.entry(stratum(c)).or_default() += 1;
        }
    }

    // Free places go to new markets above the volume floor, ordered inside each stratum
    // by a hash of the market id: reproducible and independent of volume rank.
    let mut strata: BTreeMap<(u8, u8), Vec<(u64, &Candidate)>> = BTreeMap::new();
    for c in &by_volume {
        if c.volume_24h < cfg.sample_min_volume {
            break;
        }
        strata.entry(stratum(c)).or_default().push((fnv1a(c.id.as_bytes()), c));
    }
    for members in strata.values_mut() {
        members.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.id.cmp(&b.1.id)));
        members.reverse(); // pop() takes the smallest hash first
    }
    // Each place goes to the stratum holding the fewest sampled markets (ties broken by a
    // hash of the stratum). Long-dated markets hold their place for months, so without
    // this the sample would drift towards them as short-dated ones resolve.
    let order = |key: &(u8, u8)| fnv1a(format!("stratum|{}|{}", key.0, key.1).as_bytes());
    while n < cfg.sample_n {
        let Some(key) = strata
            .iter()
            .filter(|(_, members)| !members.is_empty())
            .map(|(key, _)| *key)
            .min_by_key(|key| (filled.get(key).copied().unwrap_or(0), order(key)))
        else {
            break;
        };
        if let Some((_, c)) = strata.get_mut(&key).and_then(|members| members.pop()) {
            if take(c, "sample", rank_of[c.id.as_str()], &mut out) {
                n += 1;
                *filled.entry(key).or_default() += 1;
            }
        }
    }
    out
}

/// Tracked set with a grace period for markets that leave the selection.
#[derive(Debug, Default)]
pub struct Tracked {
    grace_ms: i64,
    last_selected: HashMap<String, i64>,
}

#[derive(Debug, Default, PartialEq)]
pub struct TrackedDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub in_grace: Vec<String>,
}

impl Tracked {
    pub fn new(grace_ms: i64) -> Self {
        Tracked { grace_ms, last_selected: HashMap::new() }
    }

    pub fn update(&mut self, selected: &[String], now_ms: i64) -> TrackedDiff {
        let before: HashSet<String> = self.last_selected.keys().cloned().collect();
        for id in selected {
            self.last_selected.insert(id.clone(), now_ms);
        }
        let current: HashSet<&String> = selected.iter().collect();
        let grace = self.grace_ms;
        let mut diff = TrackedDiff::default();
        self.last_selected.retain(|id, last| {
            if current.contains(id) {
                true
            } else if now_ms - *last > grace {
                diff.removed.push(id.clone());
                false
            } else {
                diff.in_grace.push(id.clone());
                true
            }
        });
        diff.added = self.last_selected.keys().filter(|id| !before.contains(*id)).cloned().collect();
        diff.added.sort();
        diff.removed.sort();
        diff.in_grace.sort();
        diff
    }

    pub fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.last_selected.keys().cloned().collect();
        ids.sort();
        ids
    }

    pub fn contains(&self, id: &str) -> bool {
        self.last_selected.contains_key(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(id: &str, volume: f64, price: f64, days: i64) -> Candidate {
        Candidate { id: id.into(), volume_24h: volume, price: Some(price), end_ms: Some(days * 86_400_000), fee_free: false, paired: false }
    }

    fn cfg() -> SelectionConfig {
        SelectionConfig { top_n: 2, fee_free_max: 2, fee_free_min_volume: 100.0, sample_n: 3, sample_min_volume: 10.0 }
    }

    fn pool() -> Vec<Candidate> {
        let mut pool = vec![
            cand("big1", 9000.0, 0.5, 1),
            cand("big2", 8000.0, 0.5, 1),
            Candidate { fee_free: true, ..cand("free1", 500.0, 0.2, 40) },
            Candidate { fee_free: true, ..cand("free-small", 50.0, 0.2, 40) },
            Candidate { paired: true, ..cand("pair1", 20.0, 0.5, 2) },
            cand("dust", 1.0, 0.5, 2),
        ];
        for i in 0..12 {
            let price = [0.05, 0.2, 0.5, 0.8][i % 4];
            pool.push(cand(&format!("q{i:02}"), 30.0 + i as f64, price, [0, 3, 20][i % 3]));
        }
        pool
    }

    fn sample(sel: &[Selected]) -> Vec<String> {
        sel.iter().filter(|s| s.reason == "sample").map(|s| s.id.clone()).collect()
    }

    #[test]
    fn groups_are_applied_in_order_without_duplicates() {
        let picked = select(&pool(), &cfg(), 5, &HashSet::new(), 0);
        let by_reason = |r: &str| picked.iter().filter(|s| s.reason == r).map(|s| s.id.as_str()).collect::<Vec<_>>();
        assert_eq!(by_reason("paired"), vec!["pair1"]);
        assert_eq!(by_reason("top_volume"), vec!["big1", "big2"]);
        assert_eq!(by_reason("fee_free"), vec!["free1"], "fee-free below the floor is not forced in");
        assert_eq!(by_reason("sample").len(), 3);
        assert!(!picked.iter().any(|s| s.id == "dust"), "below the sample floor");
        let unique: HashSet<&str> = picked.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(unique.len(), picked.len());
        assert_eq!(picked.iter().find(|s| s.id == "big2").unwrap().rank, 2);
    }

    #[test]
    fn the_sample_is_deterministic_and_spreads_over_strata() {
        let a = select(&pool(), &cfg(), 5, &HashSet::new(), 0);
        let b = select(&pool(), &cfg(), 5, &HashSet::new(), 0);
        assert_eq!(a, b);

        // Three picks come from three different strata.
        let pool = pool();
        let strata: HashSet<(u8, u8)> = sample(&a)
            .iter()
            .map(|id| {
                let c = pool.iter().find(|c| &c.id == id).unwrap();
                (price_bucket(c.price), horizon_bucket(c.end_ms, 0))
            })
            .collect();
        assert_eq!(strata.len(), 3);
    }

    #[test]
    fn a_sampled_market_is_followed_until_it_is_no_longer_listed() {
        let first = sample(&select(&pool(), &cfg(), 5, &HashSet::new(), 0));
        let followed: HashSet<String> = first.iter().cloned().collect();

        // Next refresh: every sampled market has fallen below the volume floor and many new
        // markets appeared. The sample does not move.
        let mut later = pool();
        for c in later.iter_mut().filter(|c| followed.contains(&c.id)) {
            c.volume_24h = 0.5;
        }
        later.extend((0..40).map(|i| cand(&format!("new{i:02}"), 60.0, [0.05, 0.5][i % 2], 3)));
        let mut again = sample(&select(&later, &cfg(), 5, &followed, 0));
        again.sort();
        let mut expected = first.clone();
        expected.sort();
        assert_eq!(again, expected);

        // One of them resolves and is no longer listed: its place goes to a new market.
        let gone = first[0].clone();
        later.retain(|c| c.id != gone);
        let after = sample(&select(&later, &cfg(), 5, &followed, 0));
        assert_eq!(after.len(), 3);
        assert!(!after.contains(&gone));
        assert!(first[1..].iter().all(|id| after.contains(id)), "the others are still followed");
    }

    #[test]
    fn free_places_go_to_the_emptiest_stratum() {
        // Two followed markets sit in the same stratum; the one free place must not go there.
        let mut pool = vec![cand("f1", 20.0, 0.5, 40), cand("f2", 20.0, 0.5, 40)];
        pool.extend((0..5).map(|i| cand(&format!("same{i}"), 100.0 + i as f64, 0.5, 40)));
        pool.push(cand("other", 15.0, 0.05, 0));
        let followed: HashSet<String> = ["f1".to_string(), "f2".to_string()].into();
        let cfg = SelectionConfig { top_n: 0, fee_free_max: 0, fee_free_min_volume: 0.0, sample_n: 3, sample_min_volume: 10.0 };
        let mut picked = sample(&select(&pool, &cfg, 0, &followed, 0));
        picked.sort();
        assert_eq!(picked, vec!["f1", "f2", "other"]);
    }

    #[test]
    fn dropped_markets_stay_for_the_grace_period() {
        let mut tracked = Tracked::new(1000);
        let d = tracked.update(&["a".into(), "b".into()], 0);
        assert_eq!(d.added, vec!["a", "b"]);
        let d = tracked.update(&["a".into()], 500);
        assert_eq!((d.in_grace.clone(), d.removed.clone()), (vec!["b".to_string()], vec![]));
        assert!(tracked.contains("b"));
        let d = tracked.update(&["a".into(), "c".into()], 1200);
        assert_eq!(d.removed, vec!["b"]);
        assert_eq!(d.added, vec!["c"]);
        assert_eq!(tracked.ids(), vec!["a", "c"]);
    }
}
