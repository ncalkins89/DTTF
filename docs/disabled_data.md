# Disabled Data Sources

## The-Odds-API (per-game win probs + spread/O/U)

**Disabled:** 2026-04-30  
**Why:** Free tier is 500 requests/month. The cron job runs every 30 minutes, which burns through the quota in ~3 days. The API returned 401s for the rest of the month anyway.

**What we were pulling:**
- **Per-game h2h win probability** — converted from moneyline to an implied probability for each team in tonight's game. Used as input to the Markov chain model that computes expected games remaining in the series, which feeds the Urgency score.
- **Point spread** — home-team spread (e.g. -4.5). Used in `projections.py` as a blowout adjustment factor: players on heavy favorites get a slight PRA haircut, players on big underdogs a bump.
- **Over/Under (total)** — game total (e.g. 224.5). Not currently wired into the model but stored in the `game_lines` table.

**Current fallback behavior:**
- Per-game win prob: falls back to stale DB value, then 0.5 if no DB row exists.
- Spread adjustment: falls back to stale DB value, then 0 adjustment (neutral).
- Series win prob: **unaffected** — sourced from DraftKings via ScraperAPI, not Odds API.

**To re-enable:**
1. Upgrade to a paid plan (~$10/mo for 10k requests) or find a free alternative.
2. Remove the stub in `scripts/update_db.py` `update_odds()` and restore the original fetch logic.
3. Consider adding a 4-hour TTL like we did for series odds, to cap usage at ~180 req/mo.

**Alternative sources to consider:**
- DraftKings Nash API (same one we use for series odds) likely has per-game h2h lines too — worth exploring so we can drop Odds API entirely.
- ESPN/NBA.com have game lines during the playoffs.
