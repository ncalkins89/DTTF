"""
DTTF Backtesting Script
-----------------------
Evaluates the PRA projection model against historical playoff data (2022–2024).
For each game in each season, predicts PRA using only data available before that game,
then measures error against actual PRA.

Also sweeps decay_rate values to find the optimal setting.

Usage:
    cd /Users/nathancalkins/Code/dttf
    python3 scripts/backtest.py

Output: printed results table + data/backtest_results.json
"""

import json
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data_fetcher import (
    CACHE,
    get_player_game_logs_season,
    get_team_defense_ratings,
)
from src.projections import project_player

BACKTEST_SEASONS = ["2021-22", "2022-23", "2023-24"]
DECAY_RATES = [0.90, 0.92, 0.95, 0.97]
RESULTS_PATH = Path(__file__).parent.parent / "data" / "backtest_results.json"

# Hardcoded placeholder series record (neutral) since we're evaluating projection accuracy,
# not the value score. The value score depends on series context which isn't the target here.
NEUTRAL_SERIES_RECORD = {"wins": 0, "losses": 0}


def get_playoff_player_ids(season: str) -> list[int]:
    """Pull all player IDs that appeared in playoff games for a given season."""
    from nba_api.stats.endpoints import LeagueGameLog
    time.sleep(0.7)
    try:
        logs = LeagueGameLog(
            season=season,
            season_type_all_star="Playoffs",
            player_or_team_abbreviation="P",
        ).league_game_log.get_data_frame()
        if logs.empty:
            return []
        return logs["PLAYER_ID"].astype(int).unique().tolist()
    except Exception as e:
        print(f"  [warn] Could not fetch player IDs for {season}: {e}")
        return []


def evaluate_season(season: str, decay_rate: float) -> list[dict]:
    """
    For each player/game in the season, predict PRA using prior-game data only.
    Returns list of {player_id, game_date, actual_pra, predicted_pra, error}.
    """
    player_ids = get_playoff_player_ids(season)
    print(f"  {season}: {len(player_ids)} players found")

    def_ratings = get_team_defense_ratings(season=season)
    results = []

    for pid in player_ids:
        all_logs = get_player_game_logs_season(pid, season, season_type="Playoffs")
        if all_logs.empty or len(all_logs) < 2:
            continue

        all_logs = all_logs.sort_values("GAME_DATE").reset_index(drop=True)

        for i in range(1, len(all_logs)):
            prior_logs = all_logs.iloc[:i].copy()
            current_game = all_logs.iloc[i]

            proj = project_player(
                player_id=pid,
                opponent_team_id=0,  # no opponent-specific data in backtest
                game_logs=prior_logs,
                def_ratings=def_ratings,
                series_record=NEUTRAL_SERIES_RECORD,
                per_game_win_prob=0.5,
                decay_rate=decay_rate,
            )

            actual = float(current_game["PRA"])
            predicted = proj["base_pra"]  # Use base_pra (no opponent adj in isolation)

            results.append({
                "season": season,
                "player_id": int(pid),
                "game_date": str(current_game["GAME_DATE"])[:10],
                "actual_pra": actual,
                "predicted_pra": round(predicted, 2),
                "error": round(predicted - actual, 2),
                "abs_error": round(abs(predicted - actual), 2),
                "decay_rate": decay_rate,
            })

    return results


def compute_metrics(results: list[dict]) -> dict:
    if not results:
        return {}
    errors = np.array([r["error"] for r in results])
    abs_errors = np.array([r["abs_error"] for r in results])
    return {
        "n": len(results),
        "mae": round(float(np.mean(abs_errors)), 3),
        "rmse": round(float(np.sqrt(np.mean(errors ** 2))), 3),
        "bias": round(float(np.mean(errors)), 3),
        "median_ae": round(float(np.median(abs_errors)), 3),
    }


def main():
    print("DTTF Backtest\n" + "=" * 50)

    all_results = []
    summary_rows = []

    for decay_rate in DECAY_RATES:
        print(f"\n── Decay rate: {decay_rate} ──")
        rate_results = []
        for season in BACKTEST_SEASONS:
            print(f"  Evaluating {season}...")
            season_results = evaluate_season(season, decay_rate)
            rate_results.extend(season_results)
            season_metrics = compute_metrics(season_results)
            print(f"    n={season_metrics.get('n', 0)}, "
                  f"MAE={season_metrics.get('mae', '?')}, "
                  f"RMSE={season_metrics.get('rmse', '?')}, "
                  f"Bias={season_metrics.get('bias', '?')}")

        overall = compute_metrics(rate_results)
        print(f"  → Overall: MAE={overall.get('mae')}, RMSE={overall.get('rmse')}, "
              f"Bias={overall.get('bias')}")
        summary_rows.append({"decay_rate": decay_rate, **overall})
        all_results.extend(rate_results)

    # Best decay rate
    summary_df = pd.DataFrame(summary_rows)
    if not summary_df.empty:
        best = summary_df.loc[summary_df["rmse"].idxmin()]
        print(f"\n{'='*50}")
        print(f"Best decay rate: {best['decay_rate']} → RMSE={best['rmse']}, MAE={best['mae']}")
        print(summary_df.to_string(index=False))

    # Save results
    RESULTS_PATH.parent.mkdir(exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump({
            "summary": summary_rows,
            "best_decay_rate": float(best["decay_rate"]) if not summary_df.empty else 0.95,
            "sample_predictions": all_results[:200],  # first 200 for inspection
        }, f, indent=2)
    print(f"\nResults saved to {RESULTS_PATH}")


if __name__ == "__main__":
    main()
