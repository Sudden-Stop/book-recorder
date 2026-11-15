//! Cross-venue matching of the same event, kept deliberately conservative.
//!
//! Only head-to-head game markets in leagues whose identifiers were checked on both
//! venues are matched:
//!
//!   Polymarket slug        `nba-uta-lal-2026-11-18`
//!   Kalshi event ticker    `KXNBAGAME-26NOV18UTALAL`, markets `…-UTA` and `…-LAL`
//!
//! Every pair carries the rule that produced it, strongest first:
//!
//!   `exact`             league, date and both team codes agree (after [`ALIASES`]);
//!   `date_off_by_one`   as `exact` but the dates differ by one day (the venues do not
//!                       always date a late game the same way);
//!   `one_team_code`     league and date agree and exactly one team code agrees. This is
//!                       for college leagues, where the venues abbreviate hundreds of
//!                       teams differently; treat it as a candidate to be checked.
//!
//! A weaker rule only applies to games the stronger ones left unmatched, and only when
//! it points to exactly one game on each side: an ambiguous match is dropped, not guessed.
//! Everything else (politics, economics, …) is left unmatched for now.
//!
//! Known limit: `date_off_by_one` can pick the wrong game of a baseball series (same two
//! teams on consecutive days) when the venues date the game differently.

use crate::days_from_civil;
use std::collections::{BTreeMap, BTreeSet, HashMap};

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

/// (league, Polymarket code, Kalshi code) where the venues abbreviate a team differently.
/// Built on 2026-11-15 by comparing Polymarket's `/teams` with the team suffixes of
/// Kalshi's game markets; the NBA needed none.
const ALIASES: &[(&str, &str, &str)] = &[
    ("nfl", "JAX", "JAC"),
    ("nfl", "LA", "LAR"),
    ("nhl", "CAL", "CGY"),
    ("nhl", "LAK", "LA"),
    ("nhl", "LAS", "VGK"),
    ("nhl", "MON", "MTL"),
    ("nhl", "UTAH", "UTA"),
    ("mlb", "ARI", "AZ"),
    ("mlb", "OAK", "ATH"),
];

pub const EXACT: &str = "exact";
pub const DATE_OFF_BY_ONE: &str = "date_off_by_one";
pub const ONE_TEAM_CODE: &str = "one_team_code";

/// A game identified the same way on both venues.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GameKey {
    pub league: String,
    /// `YYYY-MM-DD` as the venue states it.
    pub date: String,
    /// The two team codes as Polymarket writes them, upper-cased and sorted.
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

/// The code Kalshi uses for a Polymarket team code.
fn kalshi_code<'a>(league: &str, polymarket_code: &'a str) -> &'a str {
    ALIASES.iter().find(|(l, p, _)| *l == league && *p == polymarket_code).map(|(_, _, k)| *k).unwrap_or(polymarket_code)
}

fn day_number(date: &str) -> Option<i64> {
    let mut parts = date.split('-').map(|p| p.parse::<i64>().ok());
    let (y, m, d) = (parts.next()??, parts.next()??, parts.next()??);
    Some(days_from_civil(y, m as u32, d as u32))
}

/// A Kalshi game event with its two team codes, when they can be told apart.
struct KalshiGame<'a> {
    event: &'a String,
    league: String,
    day: i64,
    concat: String,
    /// From the market tickers (`…-UTA`, `…-LAL`), only when they spell the event's team string.
    split: Option<(String, String)>,
}

fn kalshi_games(events: &BTreeMap<String, Vec<String>>) -> Vec<KalshiGame<'_>> {
    events
        .iter()
        .filter_map(|(event, tickers)| {
            let (league, date, concat) = kalshi_game(event)?;
            let prefix = format!("{event}-");
            let codes: BTreeSet<&str> = tickers.iter().filter_map(|t| t.strip_prefix(prefix.as_str())).collect();
            let split = match codes.iter().copied().collect::<Vec<_>>().as_slice() {
                [a, b] if concat == format!("{a}{b}") || concat == format!("{b}{a}") => Some((a.to_string(), b.to_string())),
                _ => None,
            };
            Some(KalshiGame { event, league, day: day_number(&date)?, concat, split })
        })
        .collect()
}

/// How many of the two (aliased) Polymarket codes the Kalshi game shares: 2, 1 or 0.
fn shared_codes(key: &GameKey, game: &KalshiGame<'_>) -> usize {
    let (a, b) = (kalshi_code(&key.league, &key.teams.0), kalshi_code(&key.league, &key.teams.1));
    if game.concat == format!("{a}{b}") || game.concat == format!("{b}{a}") {
        return 2;
    }
    match &game.split {
        Some((x, y)) => [a, b].iter().filter(|c| **c == x.as_str() || **c == y.as_str()).count().min(1),
        None => 0,
    }
}

/// Polymarket games paired with Kalshi events (`event ticker -> market tickers`), each
/// with the rule that paired it. A game or an event appears in at most one pair.
pub fn matches<'a>(
    polymarket: impl IntoIterator<Item = &'a GameKey>,
    kalshi_events: &BTreeMap<String, Vec<String>>,
) -> Vec<(GameKey, String, &'static str)> {
    let games: Vec<(&GameKey, i64)> = polymarket.into_iter().filter_map(|k| Some((k, day_number(&k.date)?))).collect();
    let kalshi = kalshi_games(kalshi_events);
    let rules: [(&'static str, fn(usize, i64) -> bool); 3] = [
        (EXACT, |shared, days| shared == 2 && days == 0),
        (DATE_OFF_BY_ONE, |shared, days| shared == 2 && days.abs() == 1),
        (ONE_TEAM_CODE, |shared, days| shared == 1 && days == 0),
    ];

    let mut out: BTreeSet<(GameKey, String, &'static str)> = BTreeSet::new();
    let (mut used_games, mut used_events) = (BTreeSet::new(), BTreeSet::new());
    for (rule, fits) in rules {
        let mut edges: Vec<(usize, usize)> = Vec::new();
        for (gi, (key, day)) in games.iter().enumerate() {
            if used_games.contains(&gi) {
                continue;
            }
            for (ki, game) in kalshi.iter().enumerate() {
                if !used_events.contains(&ki) && game.league == key.league && fits(shared_codes(key, game), game.day - day) {
                    edges.push((gi, ki));
                }
            }
        }
        // Keep only matches that are the single option on both sides.
        let mut per_game: HashMap<usize, usize> = HashMap::new();
        let mut per_event: HashMap<usize, usize> = HashMap::new();
        for (gi, ki) in &edges {
            *per_game.entry(*gi).or_default() += 1;
            *per_event.entry(*ki).or_default() += 1;
        }
        for (gi, ki) in edges {
            if per_game[&gi] == 1 && per_event[&ki] == 1 {
                used_games.insert(gi);
                used_events.insert(ki);
                out.insert((games[gi].0.clone(), kalshi[ki].event.clone(), rule));
            }
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Events with their two per-team markets, as the Kalshi listing gives them.
    fn events(list: &[(&str, &str, &str)]) -> BTreeMap<String, Vec<String>> {
        list.iter().map(|(event, a, b)| (event.to_string(), vec![format!("{event}-{a}"), format!("{event}-{b}")])).collect()
    }

    fn pairs_of(slugs: &[&str], kalshi: &BTreeMap<String, Vec<String>>) -> Vec<(String, &'static str)> {
        let games: Vec<GameKey> = slugs.iter().map(|s| polymarket_game(s).unwrap()).collect();
        matches(games.iter(), kalshi).into_iter().map(|(_, event, rule)| (event, rule)).collect()
    }

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
        let kalshi = events(&[
            ("KXNBAGAME-26NOV18UTALAL", "UTA", "LAL"),
            ("KXNBAGAME-26NOV20UTALAL", "UTA", "LAL"), // two days later
            ("KXNBAGAME-26NOV18BOSMIA", "BOS", "MIA"), // other teams
            ("KXNFLGAME-26NOV15DETNE", "DET", "NE"),   // reversed order still matches
            ("KXNHLGAME-26NOV18UTALAL", "UTA", "LAL"), // other league
        ]);
        assert_eq!(
            pairs_of(&["nba-uta-lal-2026-11-18", "nfl-ne-det-2026-11-15"], &kalshi),
            vec![("KXNBAGAME-26NOV18UTALAL".to_string(), EXACT), ("KXNFLGAME-26NOV15DETNE".to_string(), EXACT)]
        );
    }

    #[test]
    fn team_codes_that_differ_between_venues_are_aliased() {
        // The games that failed to pair on 2026-11-15.
        let kalshi = events(&[
            ("KXNFLGAME-26NOV15JACTEN", "JAC", "TEN"),
            ("KXNFLGAME-26NOV15LARARI", "LAR", "ARI"),
            ("KXNFLGAME-26NOV16LACBAL", "LAC", "BAL"),
            ("KXNHLGAME-26NOV15VANVGK", "VAN", "VGK"),
            ("KXNHLGAME-26NOV16VANUTA", "VAN", "UTA"),
            ("KXNHLGAME-26NOV16MTLNYR", "MTL", "NYR"),
            ("KXNHLGAME-26NOV15CGYANA", "CGY", "ANA"),
            ("KXNHLGAME-26NOV15LAVGK", "LA", "VGK"),
        ]);
        let found = pairs_of(
            &[
                "nfl-jax-ten-2026-11-15",
                "nfl-la-ari-2026-11-15",
                "nhl-van-las-2026-11-15",
                "nhl-van-utah-2026-11-16",
                "nhl-mon-nyr-2026-11-16",
                "nhl-cal-ana-2026-11-15",
            ],
            &kalshi,
        );
        assert_eq!(found.len(), 6);
        assert!(found.iter().all(|(_, rule)| *rule == EXACT), "{found:?}");
        // Both codes aliased at once (Kings at Golden Knights).
        assert_eq!(pairs_of(&["nhl-lak-las-2026-11-15"], &kalshi), vec![("KXNHLGAME-26NOV15LAVGK".to_string(), EXACT)]);
    }

    #[test]
    fn a_date_one_day_apart_pairs_only_when_nothing_matches_exactly() {
        let kalshi = events(&[("KXNFLGAME-26NOV15PITCIN", "PIT", "CIN")]);
        assert_eq!(pairs_of(&["nfl-pit-cin-2026-11-16"], &kalshi), vec![("KXNFLGAME-26NOV15PITCIN".to_string(), DATE_OFF_BY_ONE)]);
        assert!(pairs_of(&["nfl-pit-cin-2026-11-17"], &kalshi).is_empty(), "two days apart is another game");

        // A series on consecutive days: each game pairs with its own date, none is borrowed.
        let series = events(&[("KXNHLGAME-26NOV15BOSNYR", "BOS", "NYR"), ("KXNHLGAME-26NOV16BOSNYR", "BOS", "NYR")]);
        assert_eq!(
            pairs_of(&["nhl-bos-nyr-2026-11-15", "nhl-bos-nyr-2026-11-16"], &series),
            vec![("KXNHLGAME-26NOV15BOSNYR".to_string(), EXACT), ("KXNHLGAME-26NOV16BOSNYR".to_string(), EXACT)]
        );
        // One game against two candidate days: ambiguous, so not paired.
        assert!(pairs_of(&["nhl-bos-nyr-2026-11-17"], &events(&[
            ("KXNHLGAME-26NOV16BOSNYR", "BOS", "NYR"),
            ("KXNHLGAME-26NOV18BOSNYR", "BOS", "NYR"),
        ]))
        .is_empty());
    }

    #[test]
    fn one_shared_team_code_pairs_college_games_when_unambiguous() {
        let kalshi = events(&[
            ("KXNCAAMBGAME-26NOV14ULLUNCW", "ULL", "UNCW"),
            ("KXNCAAMBGAME-26NOV15CANBUFF", "CAN", "BUFF"),
            ("KXNCAAFGAME-26NOV14MISSOKLA", "MISS", "OKLA"),
        ]);
        // `miss` matches MISS; `okl` vs OKLA does not, and nothing else that day has MISS.
        assert_eq!(pairs_of(&["cfb-miss-okl-2026-11-14"], &kalshi), vec![("KXNCAAFGAME-26NOV14MISSOKLA".to_string(), ONE_TEAM_CODE)]);
        // No code in common at all: not paired.
        assert!(pairs_of(&["cbb-loulaf-ncw-2026-11-14", "cbb-cans-buf-2026-11-15"], &kalshi).is_empty());

        // The shared code appears in two games that day (a tournament): ambiguous, dropped.
        let double = events(&[("KXNCAAMBGAME-26NOV14DUKEUNC", "DUKE", "UNC"), ("KXNCAAMBGAME-26NOV14DUKEKU", "DUKE", "KU")]);
        assert!(pairs_of(&["cbb-duke-nccu-2026-11-14"], &double).is_empty());

        // Without the per-team markets the team string cannot be split, so no partial match.
        let unsplit: BTreeMap<String, Vec<String>> = [("KXNCAAFGAME-26NOV14MISSOKLA".to_string(), Vec::new())].into();
        assert!(pairs_of(&["cfb-miss-okl-2026-11-14"], &unsplit).is_empty());
    }
}
