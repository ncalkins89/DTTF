"""
optimize_decay.py — find per-player optimal EWMA decay rate.

Uses 2024-25 regular season data already in SQLite. No API calls.
Sweeps decay 0.70–0.98 (step 0.02) for each player with RS avg PRA >= 15.
Saves results to data/optimal_decay.json.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db import _conn
from src.projections import compute_rolling_predictions

SEASON = "2024-25"
MIN_AVG_PRA = 15
MIN_GAMES = 10
DECAY_VALUES = np.round(np.arange(0.70, 1.00, 0.02), 2).tolist()  # 0.70, 0.72, ..., 0.98
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "optimal_decay.json"


def load_qualifying_players() -> list[dict]:
    with _conn() as cx:
        rows = cx.execute("""
            SELECT player_id, COUNT(*) as n, AVG(pra) as avg_pra
            FROM game_logs
            WHERE season=? AND season_type='Regular Season'
            GROUP BY player_id
            HAVING n >= ? AND avg_pra >= ?
            ORDER BY avg_pra DESC
        """, (SEASON, MIN_GAMES, MIN_AVG_PRA)).fetchall()
    return [{"player_id": r["player_id"], "n_games": r["n"], "avg_pra": round(r["avg_pra"], 1)}
            for r in rows]


def load_player_logs(player_id: int) -> pd.DataFrame:
    with _conn() as cx:
        rows = cx.execute("""
            SELECT game_date, pra FROM game_logs
            WHERE player_id=? AND season=? AND season_type='Regular Season'
            ORDER BY game_date DESC
        """, (player_id, SEASON)).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df.columns = ["GAME_DATE", "PRA"]
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    return df  # newest-first


def compute_mae(logs: pd.DataFrame, decay: float) -> float | None:
    preds = compute_rolling_predictions(logs, decay)
    pairs = [(a, p) for a, p in zip(logs["PRA"].tolist(), preds) if p == p]
    if len(pairs) < 3:
        return None
    return float(np.mean([abs(a - p) for a, p in pairs]))


def main():
    from nba_api.stats.static import players as nba_players
    id_to_name = {p["id"]: p["full_name"] for p in nba_players.get_players()}

    players = load_qualifying_players()
    print(f"Optimizing decay for {len(players)} players (RS avg PRA >= {MIN_AVG_PRA}, season {SEASON})")
    print(f"Decay sweep: {DECAY_VALUES[0]} → {DECAY_VALUES[-1]} ({len(DECAY_VALUES)} values)\n")

    results = []
    for i, p in enumerate(players):
        pid = p["player_id"]
        logs = load_player_logs(pid)
        if logs.empty:
            continue

        maes = {}
        for d in DECAY_VALUES:
            mae = compute_mae(logs, d)
            if mae is not None:
                maes[d] = mae

        if not maes:
            continue

        optimal = min(maes, key=maes.get)
        mae_095 = maes.get(0.95)

        results.append({
            "player_id": pid,
            "name": id_to_name.get(pid, str(pid)),
            "optimal_decay": optimal,
            "mae_at_optimal": round(maes[optimal], 3),
            "mae_at_0.95": round(mae_095, 3) if mae_095 is not None else None,
            "avg_pra": p["avg_pra"],
            "n_games": p["n_games"],
        })
        mae_095_str = f"{mae_095:.2f}" if mae_095 is not None else "n/a"
        print(f"[{i+1}/{len(players)}] {id_to_name.get(pid, pid)}: "
              f"optimal={optimal}  MAE={maes[optimal]:.2f}  (vs 0.95→{mae_095_str})",
              flush=True)

    if not results:
        print("No results.")
        return

    opt_decays = [r["optimal_decay"] for r in results]
    median_d = float(np.median(opt_decays))
    mean_d = float(np.mean(opt_decays))
    std_d = float(np.std(opt_decays))

    output = {
        "proposed_decay": median_d,
        "median": median_d,
        "mean": round(mean_d, 4),
        "std": round(std_d, 4),
        "n_players": len(results),
        "season": SEASON,
        "decay_values_tested": DECAY_VALUES,
        "players": results,
    }

    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n{'='*50}")
    print(f"Proposed single optimal decay: {median_d}")
    print(f"Distribution — median: {median_d}  mean: {mean_d:.3f}  std: {std_d:.3f}")
    print(f"Results saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
