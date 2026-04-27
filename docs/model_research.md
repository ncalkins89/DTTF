# NBA PRA Model Research

## Key Components — Prioritized by Impact × Ease

| Component | Impact | Ease | Priority |
|-----------|--------|------|----------|
| Minutes projection | 🔴 High | 🟡 Medium | **1** |
| Pace adjustment (market-implied from O/U) | 🔴 High | 🟡 Medium | **2** |
| Per-minute rates (PPM) instead of raw averages | 🔴 High | 🟢 Easy | **3** |
| Defense vs. Position (DvP) | 🟡 Medium | 🟡 Medium | **4** |
| Stat-specific drivers (USG%, REB%, eFG%) | 🟡 Medium | 🟡 Medium | **5** |
| XGBoost / LightGBM ensemble | 🔴 High | 🔴 Hard | **6** |
| Fatigue penalties (back-to-backs) | 🟡 Medium | 🟢 Easy | **7** |
| Outlier filtering (garbage time, early exits) | 🟡 Medium | 🟢 Easy | **8** |
| Monte Carlo simulation (distribution over outcomes) | 🟡 Medium | 🔴 Hard | **9** |
| LSTM sequential model | 🟡 Medium | 🔴 Hard | **10** |
| Bayesian hierarchical clustering | 🟡 Low | 🔴 Hard | **11** |
| SHAP interpretability | 🟢 Low | 🟡 Medium | **12** |

---

## Component Details

### 1. Minutes Projection (High Impact / Medium Ease)
Most critical variable. Current model uses raw PRA averages — decomposing into `projected_minutes × per_minute_PRA` would unlock role-change sensitivity (e.g. bench player starting due to injury).
- Inputs: season MPG, injury report, back-to-back flag, spread (garbage time risk)
- Already partially planned: `project_minutes()` in projections.py

### 2. Pace Adjustment via Market O/U (High Impact / Medium Ease)
Scale projections up/down based on market-implied total possessions. O/U directly encodes expected scoring environment.
- Formula: `pace_factor = game_total / league_avg_total`
- Requires historical O/U data (currently on todo list to find free source)
- Note: spread adjustment is already in projections.py but disabled — this replaces it properly

### 3. Per-Minute Rates (High Impact / Easy)
Replace or supplement decay-weighted PRA average with `PRA/MIN`. Captures role changes faster and handles players with volatile minutes better.
- Already have MIN in game logs
- Simple ratio, can run in parallel with current model

### 4. Defense vs. Position (DvP) (Medium Impact / Medium Ease)
Adjust for opponent's weakness at specific positions (e.g. team allows 30+ PRA to centers). More granular than the current DEF_RATING team-level adjustment.
- Requires positional classification (already have POSITION in roster data)
- NBA API has position-level defensive stats via LeagueDashPtStats or similar

### 5. Stat-Specific Drivers (Medium Impact / Medium Ease)
Custom features per stat rather than treating PRA as monolithic:
- **Points**: USG%, FGA, FTA
- **Rebounds**: REB%, opponent missed-shot rate, center minutes
- **Assists**: teammate eFG% — better shooters → more converted assists

### 6. XGBoost Ensemble (High Impact / Hard)
Train on historical game logs with engineered features. Requires labeled training data (actual PRA outcomes) and feature engineering pipeline. High ceiling but significant build time.
- Needs historical O/U, pace, DvP, injury data for training set
- Best tackled after features 1–5 are in place as model inputs

### 7. Fatigue Penalties / Back-to-Backs (Medium Impact / Easy)
Apply automatic adjustment (~1.2 pt decrease) for B2B games.
- NBA schedule data has game dates — can flag B2B with a date diff check
- Low build effort, measurable effect

### 8. Outlier Filtering (Medium Impact / Easy)
Exclude games where player left early (injury) or played garbage time.
- Proxy: already filter `MIN >= 5`. Could tighten or add a max-minutes filter for blowouts.
- True garbage time detection needs point differential by quarter (harder)

### 9. Monte Carlo Simulation (Medium Impact / Hard)
Run 10k+ simulations drawing from distributions over minutes and PPM to get probability of hitting Over/Under rather than just a point estimate.
- Most useful for betting applications
- Depends on minutes projection (item 1) being solid first

### 10. LSTM Sequential Model (Medium Impact / Hard)
Process last N games as a time sequence. More expressive than decay weights but significantly more complex to train and maintain. Likely overkill vs. XGBoost for this use case.

### 11. Bayesian Hierarchical Clustering (Low Impact / Hard)
Groups players by archetype; helps with small samples (bench players, rookies). Low ROI given we're focused on starters in the playoffs.

### 12. SHAP Interpretability (Low Impact / Medium Ease)
Explains which features drove a prediction. Useful for debugging XGBoost once it's built. Not needed yet.

---

## Current Model Status

| Feature | Status |
|---------|--------|
| Exponential decay weighted avg | ✅ Implemented |
| Opponent DEF_RATING adjustment | ✅ Implemented (disabled — pending tuning) |
| Spread adjustment | ✅ Implemented (disabled — pending tuning) |
| Series win prob / urgency | ✅ Implemented |
| Per-minute rates (PPM) | ❌ Not built |
| Minutes projection | ❌ Not built |
| Pace / O/U adjustment | ❌ No historical O/U data yet |
| DvP (positional defense) | ❌ Not built |
| Back-to-back penalty | ❌ Not built |
| XGBoost ensemble | ❌ Not built |

---

## Open Questions / Observations

### Outlier robustness in EWMA
A single game of 5 PRA or 70 PRA can meaningfully shift the decay-weighted average, especially for players with short recent histories. Options to address:
- Winsorize inputs (e.g. cap at ±N SD from the player's own mean before weighting). The winsorization threshold N should be jointly optimized with other EWMA hyperparameters (decay rate, window size) rather than chosen independently — they interact, and the optimal N depends on what decay rate is used.
- Use median instead of weighted mean as a robustness check
- Flag games where MIN < 15 or team point differential > 20 as "context games" and down-weight separately from recency decay

### Team elimination probability and pick strategy
A player on a team with <50% series win probability can still be the right pick today if:
- Their expected PRA is high
- Their team is likely to be eliminated soon (high urgency)
- The alternative picks (on surviving teams) have lower expected PRA or will still be available next round

The urgency formula captures this directionally (`PRA × elimination_prob`), but the interaction with other available picks across the bracket isn't modeled. A full optimal-pick solver would need to reason about the entire remaining pick slate, not just today's urgency scores.
