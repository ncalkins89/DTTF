"""
backtest.py — PRA model evaluation engine.

Reads from local DB (data/dttf_local.db — copy of Oracle prod).
Pull fresh: scp -i ssh-key-2026-04-22.key ubuntu@147.224.51.47:~/DTTF/data/dttf.db data/dttf_local.db

Usage in notebook:
    import sys; sys.path.insert(0, '..')
    from scripts.backtest import (
        load_game_logs, data_audit, build_dataset, split_temporal,
        baseline_ewma_predictions, train_xgb, predict_xgb,
        metrics, compare_models, subgroup_mae,
        plot_diagnostics, plot_residuals_over_time, plot_feature_importance,
        FEATURE_COLS,
    )
"""

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import scipy.stats as scipy_stats

_ROOT = Path(__file__).parent.parent
DB_PATH = _ROOT / "data" / "dttf_local.db"
DECAY_RATE = 0.82

FEATURE_COLS = [
    # ── Recency (PRA) ──────────────────────────────────────
    "ewma_pra_all",         # full-history EWMA baseline (= production model)
    "recent_form",          # ewma_pra_5 / ewma_pra_20 — captures hot/cold trend
    # ── Stat components ─────────────────────────────────────
    "ewma_pts_10",          # points: high variance, usage-sensitive
    "ewma_reb_10",          # rebounds: more stable, position-driven
    "ewma_ast_10",          # assists: role-driven
    # ── Minutes / efficiency ────────────────────────────────
    "ewma_min_20",
    "ewma_ppm_10",          # per-minute efficiency (MIN≥15 floor per game)
    # ── Variability ─────────────────────────────────────────
    "rolling_std_pra_10",   # inconsistency — spiky players score high but miss often
    "rolling_std_min_10",   # minutes uncertainty — coaching-driven, not performance
    # ── Player identity ─────────────────────────────────────
    "season_avg_pra",       # RS baseline — star vs role player
    "season_usg_pct",       # RS usage rate — offensive role (from player_season_stats)
    "season_ts_pct",        # RS true shooting % — efficiency baseline
    "is_guard",             # position bucket (G=1)
    "is_center",            # position bucket (C=1); forward is implicit reference
    # ── Game context ────────────────────────────────────────
    "opp_def_rating",       # opponent team DEF_RATING for the season (from team_season_stats)
    "opp_pace",             # opponent team PACE — fewer possessions when low
    "days_rest",            # 0=B2B, NaN=cross-season gap (filled with median)
    "is_home",
    "game_in_series",       # 1-7; defenses tighten as series progresses
    "is_elimination_game",  # game 6 or 7
    "is_playoffs",          # 0=RS 1=playoffs; interacts with season_avg_pra
]

# ─── Data Loading ─────────────────────────────────────────────────────────────

def load_game_logs(
    db_path: Path = DB_PATH,
    qualifier_season: str = "2024-25",
    min_playoff_mpg: float = 15.0,
    min_rs_mpg: float = 20.0,
) -> dict[int, pd.DataFrame]:
    """
    Load all game logs from local DB.
    Returns {player_id: df sorted newest-first} for players who meet the MPG
    threshold in qualifier_season (≥15 MPG in playoffs OR ≥20 MPG in RS).
    """
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM game_logs", con)
    pos_df = pd.read_sql_query("SELECT DISTINCT player_id, position FROM rosters", con)

    # team_season_stats: (team_abbr, season) → (def_rating, pace)
    try:
        ts = pd.read_sql_query("SELECT team_abbr, season, def_rating, pace FROM team_season_stats", con)
        team_stats_map: dict[tuple, tuple] = {
            (r.team_abbr, r.season): (r.def_rating, r.pace)
            for r in ts.itertuples()
        }
    except Exception:
        team_stats_map = {}

    # player_season_stats: (player_id, season) → (usg_pct, ts_pct)
    try:
        ps = pd.read_sql_query("SELECT player_id, season, usg_pct, ts_pct FROM player_season_stats", con)
        player_stats_map: dict[tuple, tuple] = {
            (int(r.player_id), r.season): (r.usg_pct, r.ts_pct)
            for r in ps.itertuples()
        }
    except Exception:
        player_stats_map = {}

    con.close()

    df["game_date"] = pd.to_datetime(df["game_date"])
    df["pra"] = pd.to_numeric(df["pra"], errors="coerce")
    df["min"] = pd.to_numeric(df["min"], errors="coerce")
    df = df.dropna(subset=["pra", "min"])

    # Qualify players from qualifier_season
    q = df[df["season"] == qualifier_season]
    po_mpg = q[q["season_type"] == "Playoffs"].groupby("player_id")["min"].mean()
    rs_mpg = q[q["season_type"] == "Regular Season"].groupby("player_id")["min"].mean()
    qualified = set(po_mpg[po_mpg >= min_playoff_mpg].index) | set(rs_mpg[rs_mpg >= min_rs_mpg].index)

    # Load position from rosters; collapse to G/F/C by primary position
    def _primary_pos(p):
        if not isinstance(p, str) or not p:
            return "F"
        first = p.split("-")[0].strip()
        return first if first in ("G", "F", "C") else "F"
    pos_map = {int(r.player_id): _primary_pos(r.position) for r in pos_df.itertuples()}

    def _opp_abbr(matchup: str) -> str:
        """Parse opponent team abbreviation from matchup string."""
        if "vs." in matchup:
            return matchup.split("vs.")[-1].strip()
        if "@" in matchup:
            return matchup.split("@")[-1].strip()
        return ""

    result: dict[int, pd.DataFrame] = {}
    for pid, group in df[df["player_id"].isin(qualified)].groupby("player_id"):
        grp = group.sort_values("game_date", ascending=False).reset_index(drop=True)

        pos = pos_map.get(int(pid), "F")
        grp["is_guard"]  = int(pos == "G")
        grp["is_center"] = int(pos == "C")

        # Opponent DEF_RATING and PACE — per-game, looked up by (opp_abbr, season)
        opp_def, opp_pace = [], []
        for _, row in grp.iterrows():
            opp = _opp_abbr(str(row.get("matchup") or ""))
            val = team_stats_map.get((opp, row["season"]), (None, None))
            opp_def.append(val[0])
            opp_pace.append(val[1])
        grp["opp_def_rating"] = opp_def
        grp["opp_pace"]       = opp_pace

        # Player season USG% and TS% — per-game, looked up by (player_id, season)
        usg_list, ts_list = [], []
        for _, row in grp.iterrows():
            val = player_stats_map.get((int(pid), row["season"]), (None, None))
            usg_list.append(val[0])
            ts_list.append(val[1])
        grp["usg_pct"] = usg_list
        grp["ts_pct"]  = ts_list

        result[pid] = grp

    return result


def data_audit(player_logs: dict[int, pd.DataFrame]) -> pd.DataFrame:
    """How many games per player per season/type — verify data coverage."""
    rows = []
    for pid, df in player_logs.items():
        for (season, stype), g in df.groupby(["season", "season_type"]):
            rows.append({
                "player_id": pid, "season": season,
                "season_type": stype, "n_games": len(g),
                "avg_pra": round(g["pra"].mean(), 1),
                "avg_min": round(g["min"].mean(), 1),
            })
    return pd.DataFrame(rows).sort_values(["season", "season_type"])


# ─── Feature Engineering ──────────────────────────────────────────────────────

def _ewma(values: np.ndarray, window: int | None = None, decay: float = DECAY_RATE) -> float:
    """Exponential weighted mean, newest-first (index 0 = most recent)."""
    v = values[:window] if window is not None else values
    if len(v) == 0:
        return np.nan
    w = np.array([decay ** i for i in range(len(v))])
    return float(np.dot(w, v) / w.sum())


def build_features(logs: pd.DataFrame, game_idx: int, min_prior: int = 3) -> dict | None:
    """
    Compute all features for the game at game_idx using only prior (older) games.
    logs must be sorted newest-first (index 0 = most recent). Returns None if
    insufficient history (< min_prior games).
    """
    prior = logs.iloc[game_idx + 1:]
    if len(prior) < min_prior:
        return None

    row = logs.iloc[game_idx]
    prev_row = logs.iloc[game_idx + 1]

    pra_vals = prior["pra"].values
    min_vals = prior["min"].values
    pts_vals = prior["pts"].values if "pts" in prior.columns else np.full(len(prior), np.nan)
    reb_vals = prior["reb"].values if "reb" in prior.columns else np.full(len(prior), np.nan)
    ast_vals = prior["ast"].values if "ast" in prior.columns else np.full(len(prior), np.nan)

    # PPM: only from games where player played meaningful minutes
    ppm_mask = prior["min"] >= 15
    ppm_vals = (prior.loc[ppm_mask, "pra"] / prior.loc[ppm_mask, "min"]).values \
        if ppm_mask.any() else np.array([])

    # Days rest — only meaningful within a season; cross-season gaps become NaN
    # (XGBoost fills NaN with training median, keeping the signal clean)
    days_since_last = (row["game_date"] - prev_row["game_date"]).days
    if prev_row.get("season") != row.get("season") or days_since_last > 30:
        days_rest = np.nan
    else:
        days_rest = float(max(0, days_since_last - 1))

    matchup = str(row.get("matchup") or "")
    season_type = str(row.get("season_type") or "")
    season = str(row.get("season") or "")

    # Player role proxies from prior RS games
    rs_prior = prior[prior["season_type"] == "Regular Season"]
    po_prior = prior[prior["season_type"] == "Playoffs"]
    season_avg_pra = float(rs_prior["pra"].mean()) if not rs_prior.empty else _ewma(pra_vals, 30)

    # Playoff elevation: do they score more/less in playoffs vs RS?
    po_avg = float(po_prior["pra"].mean()) if not po_prior.empty else np.nan
    playoff_pra_delta = round(po_avg - season_avg_pra, 2) if not np.isnan(po_avg) else 0.0

    # Hot/cold streak: ratio of recent to longer-term average
    ewma5 = _ewma(pra_vals, 5)
    ewma20 = _ewma(pra_vals, 20)
    recent_form = round(ewma5 / ewma20, 3) if ewma20 and ewma20 > 1 else 1.0

    # Game-in-series: count prior playoff games this season vs same opponent
    game_in_series = 1
    if season_type == "Playoffs" and matchup:
        opp = (matchup.split("vs.")[-1] if "vs." in matchup else matchup.split("@")[-1]).strip()
        if opp:
            series_games = prior[
                (prior["season"] == season) &
                (prior["season_type"] == "Playoffs") &
                (prior["matchup"].str.contains(opp, na=False))
            ]
            game_in_series = len(series_games) + 1
    game_in_series = min(game_in_series, 7)

    return {
        # PRA recency
        "ewma_pra_all":        _ewma(pra_vals),
        "ewma_pra_5":          ewma5,
        "ewma_pra_10":         _ewma(pra_vals, 10),
        "ewma_pra_20":         ewma20,
        # Stat components
        "ewma_pts_10":         _ewma(pts_vals[~np.isnan(pts_vals)], 10),
        "ewma_reb_10":         _ewma(reb_vals[~np.isnan(reb_vals)], 10),
        "ewma_ast_10":         _ewma(ast_vals[~np.isnan(ast_vals)], 10),
        # Minutes
        "ewma_min_5":          _ewma(min_vals, 5),
        "ewma_min_20":         _ewma(min_vals, 20),
        "ewma_ppm_10":         _ewma(ppm_vals, 10) if len(ppm_vals) >= 2 else np.nan,
        # Variability / form
        "rolling_std_pra_10":  float(np.std(pra_vals[:10])) if len(pra_vals) >= 3 else np.nan,
        "recent_form":         recent_form,
        # Player role
        "season_avg_pra":      round(season_avg_pra, 2),
        "is_guard":            int(row.get("is_guard", 0)),
        "is_center":           int(row.get("is_center", 0)),
        # Game context
        # Game context
        "opp_def_rating":      float(row.get("opp_def_rating") or np.nan),
        "opp_pace":            float(row.get("opp_pace") or np.nan),
        "is_b2b":              int(days_rest == 0),
        "days_rest":           min(days_rest, 10),
        "is_home":             int("vs." in matchup),
        "game_in_series":      game_in_series,
        "is_elimination_game": int(game_in_series >= 6),
        "is_playoffs":         int(season_type == "Playoffs"),
        # Player season role (RS averages — no in-season leakage for playoff rows)
        "season_usg_pct":      float(row.get("usg_pct") or np.nan),
        "season_ts_pct":       float(row.get("ts_pct") or np.nan),
        # Minutes variability — coaching-driven uncertainty
        "rolling_std_min_10":  float(np.std(min_vals[:10])) if len(min_vals) >= 3 else np.nan,
        "n_prior_games":       len(prior),
    }


def build_dataset(
    player_logs: dict[int, pd.DataFrame],
    target_season_types: list[str] | None = None,
) -> pd.DataFrame:
    """
    Walk-forward: for every game of the target type(s), compute features from
    prior games only. Default: both RS and Playoffs so is_playoffs has variance
    in training. Evaluate metrics on playoff rows only.
    """
    if target_season_types is None:
        target_season_types = ["Playoffs", "Regular Season"]

    rows = []
    for pid, logs in player_logs.items():
        for i in range(len(logs)):
            row = logs.iloc[i]
            if row["season_type"] not in target_season_types:
                continue
            feats = build_features(logs, i)
            if feats is None:
                continue
            feats.update({
                "player_id":   pid,
                "game_date":   row["game_date"],
                "season":      row["season"],
                "season_type": row["season_type"],
                "actual_pra":  row["pra"],
                "actual_min":  row["min"],
            })
            rows.append(feats)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("game_date").reset_index(drop=True)
    return df


# ─── Train / Test Split ───────────────────────────────────────────────────────

def split_temporal(
    df: pd.DataFrame,
    val_season: str = "2023-24",
    test_season: str = "2024-25",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Strictly temporal split — no future leakage.
    train = seasons strictly before val_season (RS + playoffs).
    val / test = playoff rows only from their respective seasons.
    Seasons after test_season are excluded entirely.
    """
    all_seasons = sorted(df["season"].unique())
    train_seasons = [s for s in all_seasons if s < val_season]
    train = df[df["season"].isin(train_seasons)].copy()
    val   = df[(df["season"] == val_season)  & (df["season_type"] == "Playoffs")].copy()
    test  = df[(df["season"] == test_season) & (df["season_type"] == "Playoffs")].copy()
    return train, val, test


# ─── Baseline ────────────────────────────────────────────────────────────────

def baseline_ewma_predictions(df: pd.DataFrame) -> np.ndarray:
    """Production model: ewma_pra_all (no window cap, decay=0.82) — matches src/projections.py."""
    return (
        df["ewma_pra_all"]
        .fillna(df["ewma_pra_20"])
        .fillna(df["ewma_pra_10"])
        .values
    )


# ─── XGBoost ─────────────────────────────────────────────────────────────────

MIN_FEATURE_COLS = [
    "ewma_min_5", "ewma_min_20",
    "is_b2b", "days_rest", "is_home",
    "is_playoffs", "season_avg_pra", "n_prior_games",
]


def train_xgb(
    train: pd.DataFrame,
    val: pd.DataFrame,
    feature_cols: list[str] = FEATURE_COLS,
    target_col: str = "actual_pra",
    **kwargs,
):
    """
    Train XGBRegressor with early stopping on val MAE.
    Returns (model, fill_values) where fill_values are column medians from train
    — pass them to predict_xgb to handle missing features consistently.
    target_col: column to predict (default "actual_pra"; use "actual_min" for MIN model).
    """
    import xgboost as xgb

    fill_vals = train[feature_cols].median()
    X_train = train[feature_cols].fillna(fill_vals)
    y_train = train[target_col].values
    X_val = val[feature_cols].fillna(fill_vals)
    y_val = val[target_col].values

    params = dict(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=4,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        objective="reg:absoluteerror",
        eval_metric="mae",
        early_stopping_rounds=30,
        random_state=42,
    )
    params.update(kwargs)

    model = xgb.XGBRegressor(**params)
    model.fit(X_train, y_train, eval_set=[(X_train, y_train), (X_val, y_val)], verbose=False)
    return model, fill_vals


def predict_xgb(model, fill_vals: pd.Series, df: pd.DataFrame, feature_cols: list[str] = FEATURE_COLS) -> np.ndarray:
    X = df[feature_cols].fillna(fill_vals)
    return model.predict(X)


# ─── Metrics ─────────────────────────────────────────────────────────────────

def metrics(actuals: np.ndarray, preds: np.ndarray) -> dict:
    """MAE, RMSE, R², Bias (mean error), Median AE, 90th-pct AE."""
    mask = ~(np.isnan(actuals) | np.isnan(preds))
    a, p = actuals[mask], preds[mask]
    res = a - p
    ss_res = np.sum(res ** 2)
    ss_tot = np.sum((a - a.mean()) ** 2)
    return {
        "n":            int(len(a)),
        "MAE":          round(float(np.mean(np.abs(res))), 3),
        "RMSE":         round(float(np.sqrt(np.mean(res ** 2))), 3),
        "R²":           round(float(1 - ss_res / ss_tot) if ss_tot else float("nan"), 3),
        "Bias":         round(float(np.mean(res)), 3),
        "Median AE":    round(float(np.median(np.abs(res))), 3),
        "90th-pct AE":  round(float(np.percentile(np.abs(res), 90)), 3),
    }


def compare_models(results: dict[str, tuple[np.ndarray, np.ndarray]]) -> pd.DataFrame:
    """Side-by-side metrics. results = {model_name: (actuals, preds)}."""
    return pd.DataFrame({name: metrics(a, p) for name, (a, p) in results.items()}).T


# ─── Subgroup Analysis ───────────────────────────────────────────────────────

def subgroup_mae(df: pd.DataFrame, actuals: np.ndarray, preds: np.ndarray) -> pd.DataFrame:
    """MAE broken down by minutes tier."""
    tmp = df.copy()
    tmp["actual"] = actuals
    tmp["pred"] = preds
    tmp["abs_err"] = np.abs(actuals - preds)
    ewma_min = tmp["ewma_min_20"].fillna(tmp["ewma_min_5"])
    tmp["min_tier"] = pd.cut(ewma_min, bins=[0, 20, 28, 99], labels=["<20 min", "20-28 min", "28+ min"])
    return (
        tmp.groupby("min_tier", observed=True)["abs_err"]
        .agg(MAE="mean", n="count")
        .round(2)
    )


# ─── Diagnostic Plots ────────────────────────────────────────────────────────

def plot_diagnostics(actuals: np.ndarray, preds: np.ndarray, title: str = "") -> None:
    """4-panel: predicted vs actual, residuals vs predicted, histogram, Q-Q."""
    mask = ~(np.isnan(actuals) | np.isnan(preds))
    a, p = actuals[mask], preds[mask]
    res = a - p

    fig, axes = plt.subplots(2, 2, figsize=(13, 10))
    fig.suptitle(title or "Model Diagnostics", fontsize=14, fontweight="bold")

    # 1. Predicted vs Actual
    ax = axes[0, 0]
    lo = min(a.min(), p.min()) - 2
    hi = max(a.max(), p.max()) + 2
    ax.scatter(p, a, alpha=0.25, s=14, color="#1f77b4")
    ax.plot([lo, hi], [lo, hi], "r--", linewidth=1.2)
    ax.set_xlabel("Predicted PRA")
    ax.set_ylabel("Actual PRA")
    ax.set_title("Predicted vs Actual")

    # 2. Residuals vs Predicted
    ax = axes[0, 1]
    ax.scatter(p, res, alpha=0.25, s=14, color="#ff7f0e")
    ax.axhline(0, color="r", linestyle="--", linewidth=1.2)
    ax.set_xlabel("Predicted PRA")
    ax.set_ylabel("Actual − Predicted")
    ax.set_title("Residuals vs Predicted")

    # 3. Residual Histogram + Normal Fit
    ax = axes[1, 0]
    ax.hist(res, bins=40, density=True, alpha=0.65, color="#2ca02c")
    mu, sigma = scipy_stats.norm.fit(res)
    xfit = np.linspace(res.min(), res.max(), 200)
    ax.plot(xfit, scipy_stats.norm.pdf(xfit, mu, sigma), "r-", linewidth=2,
            label=f"N(μ={mu:.1f}, σ={sigma:.1f})")
    ax.set_xlabel("Residual")
    ax.set_title("Residual Distribution")
    ax.legend(fontsize=9)

    # 4. Q-Q Plot
    ax = axes[1, 1]
    scipy_stats.probplot(res, dist="norm", plot=ax)
    ax.set_title("Q-Q Plot (Residuals vs Normal)")

    plt.tight_layout()
    plt.show()
    m = metrics(a, p)
    print(f"  MAE={m['MAE']}  RMSE={m['RMSE']}  R²={m['R²']}  Bias={m['Bias']}  n={m['n']}")


def plot_residuals_over_time(df: pd.DataFrame, actuals: np.ndarray, preds: np.ndarray) -> None:
    """Rolling mean residual over dates — catches temporal drift."""
    tmp = df[["game_date"]].copy()
    tmp["residual"] = actuals - preds
    tmp = tmp.sort_values("game_date").dropna(subset=["residual"])
    tmp["rolling"] = tmp["residual"].rolling(50, min_periods=10).mean()

    plt.figure(figsize=(13, 4))
    plt.scatter(tmp["game_date"], tmp["residual"], alpha=0.18, s=10, color="#1f77b4")
    plt.plot(tmp["game_date"], tmp["rolling"], "r-", linewidth=2, label="50-game rolling mean")
    plt.axhline(0, color="k", linestyle="--", linewidth=1)
    plt.xlabel("Date")
    plt.ylabel("Actual − Predicted")
    plt.title("Residuals Over Time (temporal drift check)")
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_learning_curve(model) -> None:
    """Train vs val MAE by boosting round — diagnose overfitting."""
    results = model.evals_result()
    train_mae = results.get("validation_0", {}).get("mae", [])
    val_mae   = results.get("validation_1", {}).get("mae", [])
    plt.figure(figsize=(10, 4))
    plt.plot(train_mae, label="Train MAE", alpha=0.8)
    plt.plot(val_mae,   label="Val MAE",   alpha=0.8)
    best_iter = model.best_iteration
    if best_iter is not None:
        plt.axvline(best_iter, color="gray", linestyle=":", label=f"Best iter ({best_iter})")
    plt.xlabel("Boosting round")
    plt.ylabel("MAE")
    plt.title("Learning Curve — overfitting if val MAE rises while train MAE drops")
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_feature_importance(model, feature_cols: list[str] = FEATURE_COLS) -> None:
    """Horizontal bar chart of XGBoost feature importance (gain)."""
    imp = model.feature_importances_
    idx = np.argsort(imp)
    plt.figure(figsize=(8, 5))
    plt.barh([feature_cols[i] for i in idx], imp[idx], color="#1f77b4")
    plt.xlabel("Importance (gain)")
    plt.title("XGBoost Feature Importance")
    plt.tight_layout()
    plt.show()
