//! Cross-venue matching of the same event, kept deliberately conservative.
//!
//! Only head-to-head game markets in leagues whose identifiers were checked on both
//! venues are matched, and only when league, date and both team codes agree exactly:
//!
//!   Polymarket slug        `nba-uta-lal-2026-11-18`
//!   Kalshi event ticker    `KXNBAGAME-26NOV18UTALAL`
//!
//! A team whose code differs between venues is simply not matched (no false pairs).
//! Everything else (politics, economics, …) is left unmatched for now.

use std::collections::BTreeSet;

/// Kalshi series ticker -> Polymarket league prefix.
const LEAGUES: &[(&str, &str)] = &[
    ("KXNBAGAME", "nba"),
    ("KXNFLGAME", "nfl"),
    ("KXNHLGAME", "nhl"),
    ("KXMLBGAME", "mlb"),
    ("KXNCAAFGAME", "cfb"),
    ("KXNCAAMBGAME", "cbb"),
];
const MONTHS: [&str; 12] = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];

/// A game identified the same way on both venues.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GameKey {
    pub league: String,
    /// `YYYY-MM-DD` as the venue states it.
    pub date: String,
    /// The two team codes, upper-cased and sorted.
    pub teams: (String, String),
}

/// `nba-uta-lal-2026-11-18` -> key. Slugs with a suffix (spreads, totals, maps) are not games.
pub fn polymarket_game(slug: &str) -> Option<GameKey> {
    let parts: Vec<&str> = slug.split('-').collect();
    let [league, a, b, y, m, d] = parts.as_slice() else {
        return None;
    };
    if !LEAGUES.iter().any(|(_, l)| l == league) {
        return None;
    }
    let numeric = |s: &str, n: usize| s.len() == n && s.bytes().all(|c| c.is_ascii_digit());
    let alpha = |s: &str| !s.is_empty() && s.bytes().all(|c| c.is_ascii_alphanumeric());
    if !(numeric(y, 4) && numeric(m, 2) && numeric(d, 2) && alpha(a) && alpha(b)) {
        return None;
    }
    let mut teams = [a.to_ascii_uppercase(), b.to_ascii_uppercase()];
    teams.sort();
    let [t1, t2] = teams;
    Some(GameKey { league: league.to_string(), date: format!("{y}-{m}-{d}"), teams: (t1, t2) })
}

/// `KXNBAGAME-26NOV18UTALAL` -> (league, date, concatenated team codes).
pub fn kalshi_game(event_ticker: &str) -> Option<(String, String, String)> {
    let (series, rest) = event_ticker.split_once('-')?;
    let league = LEAGUES.iter().find(|(s, _)| *s == series)?.1;
    if rest.len() < 9 || !rest.is_ascii() {
        return None;
    }
    let (yy, mon, dd) = (&rest[0..2], &rest[2..5], &rest[5..7]);
    let month = MONTHS.iter().position(|m| *m == mon)? + 1;
    if !yy.bytes().chain(dd.bytes()).all(|c| c.is_ascii_digit()) {
        return None;
    }
    // Some tickers carry a start time after the date; team codes never start with a digit.
    let teams = rest[7..].trim_start_matches(|c: char| c.is_ascii_digit());
    if teams.len() < 4 || !teams.bytes().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }
    Some((league.to_string(), format!("20{yy}-{month:02}-{dd}"), teams.to_string()))
}

/// True when the Kalshi team string is exactly the two Polymarket codes, in either order.
pub fn same_game(key: &GameKey, kalshi: &(String, String, String)) -> bool {
    let (league, date, teams) = kalshi;
    let (a, b) = (&key.teams.0, &key.teams.1);
    *league == key.league && *date == key.date && (*teams == format!("{a}{b}") || *teams == format!("{b}{a}"))
}

/// Kalshi event tickers that match any of the Polymarket games.
pub fn matches<'a>(
    polymarket: impl IntoIterator<Item = &'a GameKey>,
    kalshi_events: impl IntoIterator<Item = &'a String>,
) -> Vec<(GameKey, String)> {
    let games: Vec<&GameKey> = polymarket.into_iter().collect();
    let mut out = BTreeSet::new();
    for event in kalshi_events {
        if let Some(parsed) = kalshi_game(event) {
            if let Some(key) = games.iter().find(|k| same_game(k, &parsed)) {
                out.insert(((*key).clone(), event.clone()));
            }
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_venues() {
        let key = polymarket_game("nba-uta-lal-2026-11-18").unwrap();
        assert_eq!(key.teams, ("LAL".to_string(), "UTA".to_string()));
        assert_eq!(kalshi_game("KXNBAGAME-26NOV18UTALAL").unwrap(), ("nba".into(), "2026-11-18".into(), "UTALAL".into()));
        assert_eq!(kalshi_game("KXNFLGAME-26NOV151300NEDET").unwrap().2, "NEDET");
        assert!(polymarket_game("nfl-ne-det-2026-11-15-spread-home-2pt5").is_none());
        assert!(polymarket_game("cs2-faze-fnc-2026-11-15").is_none(), "league not verified on Kalshi");
        assert!(kalshi_game("KXFEDDECISION-26DEC").is_none());
        assert!(kalshi_game("KXNBAGAME-26XXX18UTALAL").is_none());
    }

    #[test]
    fn matches_only_exact_games() {
        let games = [polymarket_game("nba-uta-lal-2026-11-18").unwrap(), polymarket_game("nfl-ne-det-2026-11-15").unwrap()];
        let events: Vec<String> = [
            "KXNBAGAME-26NOV18UTALAL",
            "KXNBAGAME-26NOV19UTALAL", // other day
            "KXNBAGAME-26NOV18UTALAC", // other team
            "KXNFLGAME-26NOV15DETNE",  // reversed order still matches
            "KXNHLGAME-26NOV18UTALAL", // other league
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let found = matches(games.iter(), events.iter());
        let tickers: Vec<&str> = found.iter().map(|(_, e)| e.as_str()).collect();
        assert_eq!(tickers, vec!["KXNBAGAME-26NOV18UTALAL", "KXNFLGAME-26NOV15DETNE"]);
    }
}
