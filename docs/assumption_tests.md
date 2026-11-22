# Testing the assumptions of Feil & Nendel on recorded data

Feil & Nendel, *Optimal Market Making in Prediction Markets* (arXiv 2607.17991, July 2026), solve a market
maker's control problem for a binary contract: the price is a martingale in (0, 1) obtained as the logistic
transform of a latent log-odds diffusion, the contract settles as Bernoulli(p_T), fills arrive as Poisson
processes whose intensity depends on time, price and quote, inventory is bounded, and the objective is expected
wealth with a running inventory-variance penalty and a terminal settlement penalty. They solve the HJB equation
numerically, with no small-inventory approximation.

Their parameters (Table 1) are illustrative, not fitted, and §4.1 states that the empirical form of order
intensities in prediction markets has not been studied. This document lists each modelling assumption and how
to test it on the tables in `schema/`. Priority: **A5** (fill intensity, the open question the paper names) and
**A6** (no adverse selection, the assumption most likely to fail).

Background: the model descends from Avellaneda & Stoikov (2008) and Guéant, Lehalle & Fernández-Tapia (2013);
cite those for the classical framework.

## Common preparation

- **Prepared tables.** `python -m book_pipeline.prepare` derives what the curated tables do not state directly:
  `markets` (category, tick, fees, outcome), `trade_reach`, `activity_surplus` and `exclusions`. Trade rows join
  on `(dt, hour, source, host, line, item)`, with `dt` and `hour` taken from the file path.
- **Coverage.** Cut every replay at `gap` rows of `book_deltas` and resume at the next `reset`; count exposure
  time without those intervals. Do not drop whole market-hours for containing a gap: in 2026-11-15..21 that
  discards 12.3 % of Polymarket market-hours to avoid 0.24 % of the time. Only `validate`-clean token-days enter
  estimation. A day whose nightly summary fails only at the `chain` step is clean for books and `market_ws`
  trades: that step judges the `activity` feed.
- **Exclusions.** Remove the windows of `docs/data_exclusions.csv` from events and from exposure time. A venue
  pause with the socket up is not a gap, so coverage does not show it.
- **Book state.** Replay `book_deltas` per token to get best bid, best ask and depth at every `recv_ts`.
  Polymarket: a YES bid is a NO ask at 1 − price, so merge the two tokens of a market into one YES book (see A11).
  Kalshi: one book per market already.
- **Mid and belief.** p_t = (best_bid + best_ask) / 2; log-odds x_t = ln(p / (1 − p)). Flag one-tick spreads and
  empty sides; drop states with p outside [0.02, 0.98] where the logit is unstable.
- **Time to resolution.** τ = resolution time − t. Use `markets.resolution_ts` where present (Kalshi `lifecycle`
  `determined`), else the `final` record's time, else the prepared `markets.resolution_ts`
  (`resolution_source = chain` is the settlement block); keep `end_ts` (listed) separately for A10. Normalise per market as
  t/T over its recorded life where a model quantity is in t/T.
- **Which clock.** `resolution_ts` is settlement, not the moment uncertainty ends, and using it as a horizon is
  also look-ahead. In 2026-11-15..21, one hour before settlement 92 % of Polymarket non-sports and 43 % of sports
  markets already priced the winner above 0.95. Use times known ex ante: Polymarket games have
  `markets.event_start_ts` (5 % decided at that moment), so report pre-game and in-play separately. Kalshi gives no
  start: its `occurrence_datetime` equals the expected end (85 % of sports markets decided by then). In A2 and A4
  stop each series when the event is over, or the approach to T is mostly a market that has already been decided.
- **Strata.** All statistics are reported by venue × category (prepared `markets.category`, because `tags` is
  empty on market rows; fee-free Polymarket markets have none and form a stratum of their own; sports vs the rest
  at least) × p bucket
  (0.05 wide) × τ bucket (log-spaced). Kalshi has fees on most series and Polymarket has fee-free markets
  (`fees_enabled`), so venue also separates fee regimes (A8).
- **Trades.** Use `trades` with `source = market_ws` for side and timing (one copy per fill). Use
  `source = activity` only for wallet-level work (A6) and for the depth a trade reached (A5), and never sum the
  two sources. Anti-join `activity_surplus` whenever `activity` is used.

## Assumptions and tests

### A1. Price is a martingale and equals the event probability (§2.1)

- **Data:** mid series; prepared `markets.outcome` (not curated `resolved_outcome` alone: markets that a recorder
  restart dropped mostly lack it there, which biases the sample).
- **Selection:** resolved-by-the-cutoff is itself a selection ("by date" contracts that resolve early resolve
  YES). Select markets by listed end date before the cutoff, report how many are still unresolved, and cluster
  by event. Horizons count back from `event_start_ts` or the listed `end_ts`, never from `resolution_ts`. One
  week is thin for this test: pre-game Polymarket is its cleanest sample, and 7 d is impossible.
- **Test:** (a) calibration: bin mids at fixed horizons before the event start or listed end (1 h, 1 d, 7 d) and compare with the
  realised frequency of YES; reliability curve and Brier score with bootstrap CIs by market. (b) drift: regress
  Δp over the next interval on p and τ; under the martingale the slope and intercept are zero. Known
  favourite-longshot bias would show as calibration error near 0 and 1.
- **Fails if:** systematic, significant miscalibration or predictable drift. A small bias is not fatal for the
  quoting problem but changes the meaning of p in the terminal penalty.

### A2. Volatility shape: σ(t, x) = σ₀ + σ₁(t/T)^η + σ₂/(1 + x²) in log-odds (§4.1)

- **Data:** x_t series.
- **Test:** realised variance of Δx on a fixed grid (e.g. 1 min, with a check at 5 min for microstructure noise)
  per (τ, p) bucket. Fit the paper's form by non-linear least squares; compare with a free (τ, p) surface. Check
  the two qualitative claims: variance rises as τ → 0, and peaks at p = ½ in log-odds.
- **Fails if:** variance in log-odds is not maximal near p = ½ or does not rise toward resolution. Note that the
  price-space volatility p(1 − p)σ already vanishes at the boundaries; the test is on σ itself.

### A3. Continuous diffusion, no jumps (§2.1)

- **Data:** x_t series.
- **Test:** bipower variation vs realised variance (Barndorff-Nielsen–Shephard) per market-day; share of
  quadratic variation from jumps, by category. Sports are the expected failure (scoring events), elections and
  macro releases too.
- **Fails if:** the jump share is material. Then quoting near known event times needs a jump term the model has
  not got.

### A4. Activity A(t, p) rises toward resolution and peaks at p = ½ (§4.1)

- **Data:** `trades` (`market_ws`).
- **Test:** trade count and volume per unit time by (τ, p) bucket, normalised by the market's own average to
  remove size effects. Fit A₀ + A₁ (e^{ξt/T} − 1)/(e^ξ − 1) · p(1 − p).
- **Fails if:** activity is not hump-shaped in p or not increasing in t. Sports in-play spikes are expected and
  should be reported separately.

### A5. Fill intensity decays exponentially with distance, with k(t) rising and a boundary factor (§4.1)

The central test. The model is Λ^b = A(t, p) · (2π/(π + p))^ν · exp(−k(t)(p − π)) on the bid, symmetric on the ask.

- **Data:** replayed book; `trades` with taker side.
- **Depth reached:** on Polymarket use `trade_reach.reach_price`, not the `market_ws` price, which on a sweep is
  often the average. Where `status` is not `exact` the depth is unknown: report that share and a sensitivity
  bound, and do not put the print in its place silently.
- **Estimator:** for each side and each distance δ (in ticks from mid, then in price), estimate the rate at
  which a resting order at that distance would have been reached: count taker trades whose execution reached
  δ or beyond (a sell that sweeps to the bid at δ fills every level above it), divided by the time the book had
  a level at that distance. This is the standard fill-probability-by-depth estimator used for Avellaneda–Stoikov
  calibration; it needs a time series across many δ, which the book replay gives.
- **Fits:** per side and per (τ, p) bucket: (a) exponential A e^{−kδ}; (b) power law A (1 + δ)^{−α}; (c) the
  paper's form with the boundary factor and k(t). Compare by likelihood (Poisson counts) and out-of-sample
  error on held-out days.
- **Also report:** whether k rises toward resolution (k₁ > k₀), whether ν > 0 is supported near the boundaries,
  and bid/ask asymmetry at the same p.
- **Caveats:** with 1¢ ticks only a handful of distances exist inside the typical spread (the paper's optimal
  spreads are 4–5 ticks, A9), so the fit is over few points per bucket; pool buckets where needed. Queue position
  is not observed, so the estimate is for an order at the back of the queue.

### A6. Bid and ask arrivals are independent Poisson, independent of the price path: no adverse selection (§2.1.3)

- **Data:** `trades` (both sources); mid series.
- **Tests:**
  - **Markouts:** for each fill, the mid move from the fill to +10 s, +1 min, +10 min, +1 h, signed against the
    maker. Under the model the mean is zero. Report by category, p, τ and trade size.
  - **Wallets (Polymarket `activity`):** the same markouts grouped by `wallet`; test whether a minority of
    wallets carries most of the adverse move (informed flow), and whether that varies with τ.
  - **Clustering:** inter-arrival times per side; dispersion index and a Hawkes fit against Poisson; cross-side
    excitation (does a buy raise the rate of sells?).
- **Fails if:** markouts are significantly against the maker, or arrivals are self-exciting. Either is the
  model's largest gap; report its size in cents per share, comparable with the half-spread.

### A7. Fixed trade size Δ (§2.1.2)

- **Data:** `trades.size`.
- **Test:** size distribution by venue and category; tail index. Correlate size with markout (links to A6).
- **Fails if:** heavy-tailed sizes. Mostly a magnitude issue, unless large trades are the informed ones.

### A8. No fees (not modelled)

- **Data:** prepared `markets.fees_enabled`, `fee_type`, `fee_rate` (Polymarket), `fee_multiplier` (Kalshi, from
  its series); `trades.fee_rate_bps`.
- **Test:** compare spreads, depth and A5 fits between fee-free and fee-charging markets at matched (p, τ, volume),
  and across venues on the `pairs` games. Compute the fee per share against the model's optimal half-spread.
- **Fails if:** fees are comparable to the half-spread, which would make them a first-order term.

### A9. Continuous quotes, no tick size (§2.1.2)

- **Data:** `book_deltas.tick_size`, prepared `markets.tick_size`; spreads from the replay. On tapered Kalshi
  markets (`price_structure`) that is the finest step: the step at a given price is in `raw.price_ranges`.
- **Test:** spread distribution in ticks; share of time the spread is one tick (tick-constrained). Compare with
  the model's 0.04–0.055 spread scale.
- **Fails if:** books are mostly one tick wide; then the control is queue position, not price, and the model's
  optimal quote cannot be posted.

### A10. Resolution at a fixed known time T (§2.1)

- **Data:** `markets.end_ts` (listed) vs actual resolution (`resolution_ts`, Kalshi `lifecycle` events).
- **Test:** distribution of actual − listed resolution time by category; share of markets that resolve before
  the listed date ("at or prior to" contracts) or have no fixed date.
- **Fails if:** T is uncertain for a large share; the terminal penalty then applies at a random time.

### A11. YES and NO trade as one book (§2)

- **Data:** Polymarket books of the two tokens of a market (`markets.tokens`).
- **Test:** compare best bid of YES with 1 − best ask of NO at the same time; share of time they differ and by how
  much. Kalshi quotes YES and NO in one book, so it needs no test.
- **Fails if:** the two books diverge often; then a single-contract model understates available liquidity.

## Output

One notebook or script per assumption under `pipeline/` (to decide), each producing a table and a figure per
venue × category, and a summary table: assumption, statistic, value with CI, verdict (holds / magnitude issue /
structural failure). Fitted parameters from A2, A4 and A5 give a calibrated version of the paper's Table 1, which
can be fed back into their numerical scheme to compare the model's optimal spreads with observed ones.
