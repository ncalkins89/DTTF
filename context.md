# DTTF — Drive to the Finals

## Game Rules

- Every day there are NBA playoff games, pick **one player** playing that day.
- Score = that player's **Points + Rebounds + Assists (PRA)** from the actual game.
- A player can only be picked **once** the entire playoffs.
- **No picks on off-days** (days with no games).

## Strategy

**Core tradeoff:** Players are a non-renewable resource. Spending a star in Round 1 means they're gone for the Finals. Save stars on deep-running teams; burn players on teams likely out soon.

### High-Priority Heuristics

**The "Dead Man Walking" Rule:** Only pick stars from teams you expect to be eliminated in the current round. Extracts maximum value without burning players needed for later rounds.

**The "Game 4" Deadline:** Never save a player from a losing team for Games 5–7. If they get swept in 4, you get zero. Use burn players in Games 1–3 to guarantee a score.

**The Blowout Buffer:** Avoid superstars in projected 15+ point blowouts. Coaches bench stars in Q4, killing PRA floor. Target 3v6 or 4v5 seeds where games stay close. *(Model applies spread adjustment for this.)*

**The Injury Pivot:** If a teammate of a superstar is ruled out, that star's usage skyrockets. Treat these as "Flash Sales" — high PRA potential that might not exist next game.

**The Finals Vault:** Identify the 3 best players on each predicted Finals team. Lock them. Don't touch until Conference Finals at earliest.

**The Overtime Hunt:** Target high Over/Under games. A single OT period adds ~5 free minutes of PRA that can create a 10–15 point lead over opponents.

### Daily Checklist

1. **Map the bracket** — decide who loses this round; those players are this week's available inventory
2. **Check minutes** — only pick players averaging 36+ min in last 5 games (volume is king)
3. **Check usage** — prioritize "heliocentric" players (Luka, Brunson, Embiid types)
4. **Check injury report** — if a starter is out, consider their backup as a cheap 25+ PRA without burning a star

### Model-based heuristics
- High Urgency score → pick now (good PRA today, few games left)
- Low Urgency score → save (star, team likely to play many more games)
- When two players have similar Urgency, prefer the one on the team with *lower* series win probability — they're more expendable

## Model

### PRA Projection

1. **Base PRA** — exponential decay weighted average of last 20 games
   - `weight = decay_rate ^ days_since_game` (default decay_rate = 0.95)
   - Recent games weighted more heavily regardless of regular season vs. playoffs
   - Tune decay_rate via `scripts/backtest.py`

2. **Opponent defense adjustment**
   - Scale by `opponent_DEF_RATING / league_avg_DEF_RATING`
   - Clamped to ±30% of base PRA
   - DEF_RATING = opponent points per 100 possessions (lower = better defense)

3. **Blowout/spread adjustment** (if spread data available)
   - Expected margin < -8: ×0.93
   - Expected margin < -15: ×0.85

### Value Score

```
value_score = projected_pra / (1 + expected_future_games)
```

`expected_future_games` computed via Markov chain over best-of-7 series states using current series record (W-L) and per-game win probability.

Higher score = pick now. Lower = save for later.

## Data Sources

| Data | Source | Cadence |
|------|--------|---------|
| Game schedule | nba_api `ScoreboardV2` | Daily (cache 1h) |
| Player game logs | nba_api `PlayerGameLog` | Per player (cache 6h) |
| Team defense ratings | nba_api `LeagueDashTeamStats` | Daily (cache 24h) |
| Series standings | nba_api `PlayoffSeries` | Hourly (cache 1h) |
| Series win probability | Basketball-Reference scrape | Hourly (cache 1h) |
| External projections | Manual entry in dashboard | Per game day |

**TODO:** Wire up The-Odds-API (free key, 500 req/month) for real Vegas series odds and point spreads. Sign up at https://the-odds-api.com/ then add `ODDS_API_KEY=...` to `.env`.

## Running the Dashboard

```bash
cd /Users/nathancalkins/Code/dttf
python3 src/dashboard.py
# Open http://127.0.0.1:8050
```

## Backtesting

```bash
cd /Users/nathancalkins/Code/dttf
python3 scripts/backtest.py
```

Evaluates the model on 2022-2024 playoff seasons. Sweeps decay rates [0.90, 0.92, 0.95, 0.97].
Results saved to `data/backtest_results.json`. Use the best decay_rate in the dashboard slider.

## File Structure

```
dttf/
├── context.md                      <- this file
├── requirements.txt
├── .env                            <- ODDS_API_KEY=... (optional, not committed)
├── data/
│   ├── picks.json                  <- your pick history (source of truth)
│   ├── external_projections.json   <- manually entered DFS projections
│   └── cache/                      <- diskcache (safe to delete to force refresh)
├── scripts/
│   └── backtest.py
└── src/
    ├── picks.py         <- persistence layer
    ├── data_fetcher.py  <- nba_api + caching
    ├── projections.py   <- PRA model (pure functions)
    ├── odds.py          <- series win probability
    └── dashboard.py     <- Plotly Dash app
```

## TODOs

**Model (next up):**
- [ ] **Minutes-based projection model** — decompose `PRA = minutes × per_min_PRA`. Per-minute efficiency is stable; minutes is where context adjustments live (playoff premium for stars, elimination game boost, teammate injury redistribution, blowout benching). Implement `project_minutes()` in `projections.py`, run in parallel with current model to compare, then switch over. See prior discussion for full signal list.

**Data / model:**
- [ ] Wire up The-Odds-API point spreads (already have key) — needed for blowout adjustment and O/U
- [ ] Programmatic scraping of DFS projection sites (FantasyPros, Stokastic) for external PRA
- [ ] Ensemble model: average our projection with external projection
- [ ] Sync picks from friend's game website (scrape tables, fuzzy name->player_id match)

**Strategy features to implement in dashboard:**
- [ ] Minutes filter — surface/flag players under 36 min avg in last 5 games
- [ ] Usage rate display — show usage % alongside PRA projection
- [ ] Injury impact flag — when a starter is ruled out, highlight teammates with usage boost
- [ ] Game 4 deadline alert — flag players on teams down 0-3 or 3-0 as urgent burns
- [ ] O/U display — show game total (if spread data available) to identify OT candidates
- [ ] "Vault" list — UI to manually mark players as saved for later rounds
- [ ] **Urgency soundness review** — is `Best Proj × (1 − Series Win%)` the right metric? Potential issues: (1) it conflates "pick now because team is losing" with "pick now because PRA is high" — these are different decisions; (2) a star on a 10% series-win team gets max urgency even if you'd rather save them for tomorrow's better matchup; (3) doesn't account for how many games remain *this round* vs. future rounds. Consider alternatives: value-over-replacement (what do you give up by not picking this player today vs. a future game), or a round-adjusted score that explicitly weights remaining playoff games across all rounds.
- [ ] **Insight generator** — when multiple players have similar Best Proj scores, surface tiebreaker signals that aren't yet in the model: minutes trend (last 3 vs last 10), usage rate, teammate injury context, blowout risk (spread), game total (O/U for OT potential), home/away split, series record momentum. Display as short human-readable bullets ("Brunson avg 42+ min last 3 games", "ORL starter ruled out → Wagner usage up", "Game total 228 — OT candidate"). Goal: help the user make a confident pick when the numbers are close.
