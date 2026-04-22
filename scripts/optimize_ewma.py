"""
optimize_ewma.py — 2D grid search over (decay_rate × window_size).

Evaluation is on PLAYOFF games only, using RS + prior playoff games as context.
This measures how well the model predicts what actually matters: playoff PRA.

Seasons used: all seasons in DB that have both RS and Playoffs data.
Players: those with >= MIN_PLAYOFF_GAMES playoff games in a given season.

Saves results to data/optimal_ewma.json.

Usage:
    python3 scripts/optimize_ewma.py
    python3 scripts/optimize_ewma.py --seasons 2024-25 2023-24
    python3 scripts/optimize_ewma.py --min-games 3
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db import _conn
from src.projections import compute_rolling_predictions

DECAY_VALUES = [round(d, 2) for d in np.arange(0.70, 1.01, 0.02)]
WINDOW_VALUES = [10, 15, 20, 25, 30, 40, None]  # None = all available history
WINDOW_LABELS = [str(w) if w is not None else "all" for w in WINDOW_VALUES]

MIN_PLAYOFF_GAMES = 4
OUTPUT_PATH = Path(__file__).parent.parent / "data" / "optimal_ewma.json"


def available_seasons() -> list[str]:
    """Seasons that have both RS and Playoffs data."""
    with _conn() as cx:
        rows = cx.execute("""
            SELECT season, season_type, COUNT(DISTINCT player_id) as n
            FROM game_logs
            GROUP BY season, season_type
        """).fetchall()
    by_season: dict[str, set] = {}
    for r in rows:
        by_season.setdefault(r["season"], set()).add(r["season_type"])
    return sorted(
        [s for s, types in by_season.items()
         if "Regular Season" in types and "Playoffs" in types],
        reverse=True,
    )


def load_player_season(player_id: int, season: str) -> pd.DataFrame:
    """All games for player/season, newest-first, with season_type column."""
    with _conn() as cx:
        rows = cx.execute("""
            SELECT game_date, pra, season_type FROM game_logs
            WHERE player_id=? AND season=?
            ORDER BY game_date DESC
        """, (player_id, season)).fetchall()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame([dict(r) for r in rows])
    df.columns = ["GAME_DATE", "PRA", "SEASON_TYPE"]
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    return df


def qualifying_players(season: str, min_playoff_games: int) -> list[dict]:
    with _conn() as cx:
        rows = cx.execute("""
            SELECT player_id, COUNT(*) as n
            FROM game_logs
            WHERE season=? AND season_type='Playoffs'
            GROUP BY player_id
            HAVING n >= ?
        """, (season, min_playoff_games)).fetchall()
    return [{"player_id": r["player_id"], "n_playoff_games": r["n"]} for r in rows]


def eval_player_season(df: pd.DataFrame, decay: float, window: int | None) -> float | None:
    """
    MAE on playoff games only.
    For each playoff game at position i, predict using df.iloc[i+1:i+1+window]
    (all games that came before it, up to window count).
    """
    preds = compute_rolling_predictions(df, decay, window)
    playoff_mask = df["SEASON_TYPE"] == "Playoffs"
    pairs = [
        (df.iloc[i]["PRA"], preds[i])
        for i in range(len(df))
        if playoff_mask.iloc[i] and preds[i] == preds[i]  # not NaN
    ]
    if len(pairs) < 2:
        return None
    return float(np.mean([abs(a - p) for a, p in pairs]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seasons", nargs="+", default=None)
    parser.add_argument("--min-games", type=int, default=MIN_PLAYOFF_GAMES)
    args = parser.parse_args()

    seasons = args.seasons or available_seasons()
    print(f"Seasons: {seasons}")
    print(f"Decay values: {DECAY_VALUES[0]} → {DECAY_VALUES[-1]} ({len(DECAY_VALUES)} values)")
    print(f"Window values: {WINDOW_LABELS}")
    print(f"Min playoff games: {args.min_games}\n")

    from nba_api.stats.static import players as nba_players
    id_to_name = {p["id"]: p["full_name"] for p in nba_players.get_players()}

    # aggregate MAE grid: sum of MAEs across all player-seasons, shape [n_decay, n_window]
    agg_mae = np.zeros((len(DECAY_VALUES), len(WINDOW_VALUES)))
    agg_count = np.zeros((len(DECAY_VALUES), len(WINDOW_VALUES)))

    player_results = []
    total_player_seasons = 0

    for season in seasons:
        players = qualifying_players(season, args.min_games)
        print(f"=== {season} — {len(players)} qualifying players ===")

        for p in players:
            pid = p["player_id"]
            df = load_player_season(pid, season)
            if df.empty:
                continue

            best_mae = None
            best_di, best_wi = 0, 0

            for di, decay in enumerate(DECAY_VALUES):
                for wi, window in enumerate(WINDOW_VALUES):
                    mae = eval_player_season(df, decay, window)
                    if mae is None:
                        continue
                    agg_mae[di, wi] += mae
                    agg_count[di, wi] += 1
                    if best_mae is None or mae < best_mae:
                        best_mae = mae
                        best_di, best_wi = di, wi

            if best_mae is None:
                continue

            total_player_seasons += 1
            player_results.append({
                "player_id": pid,
                "name": id_to_name.get(pid, str(pid)),
                "season": season,
                "optimal_decay": DECAY_VALUES[best_di],
                "optimal_window": WINDOW_LABELS[best_wi],
                "mae_at_optimal": round(best_mae, 3),
                "n_playoff_games": p["n_playoff_games"],
            })

        season_players = [r for r in player_results if r["season"] == season]
        if season_players:
            print(f"  {len(season_players)} evaluated, "
                  f"median optimal decay={np.median([r['optimal_decay'] for r in season_players]):.2f}, "
                  f"median MAE={np.median([r['mae_at_optimal'] for r in season_players]):.2f}")

    if total_player_seasons == 0:
        print("No results — check that playoff data exists.")
        return

    # Compute mean MAE grid (only cells with data)
    with np.errstate(invalid="ignore"):
        mean_mae_grid = np.where(agg_count > 0, agg_mae / agg_count, np.nan)

    # Proposed optimal = argmin of aggregate mean MAE
    flat_idx = np.nanargmin(mean_mae_grid)
    best_di, best_wi = np.unravel_index(flat_idx, mean_mae_grid.shape)
    proposed_decay = DECAY_VALUES[int(best_di)]
    proposed_window = WINDOW_LABELS[int(best_wi)]

    # Per-dimension distributions
    opt_decays = [r["optimal_decay"] for r in player_results]
    opt_windows = [r["optimal_window"] for r in player_results]

    output = {
        "proposed_decay": proposed_decay,
        "proposed_window": proposed_window,
        "seasons_evaluated": seasons,
        "n_player_seasons": total_player_seasons,
        "grid": {
            "decay_values": DECAY_VALUES,
            "window_labels": WINDOW_LABELS,
            "mean_mae": [
                [round(mean_mae_grid[di, wi], 4) if not np.isnan(mean_mae_grid[di, wi]) else None
                 for wi in range(len(WINDOW_VALUES))]
                for di in range(len(DECAY_VALUES))
            ],
        },
        "decay_distribution": {
            "median": float(np.median(opt_decays)),
            "mean": round(float(np.mean(opt_decays)), 4),
            "std": round(float(np.std(opt_decays)), 4),
        },
        "window_distribution": {
            w: opt_windows.count(w) for w in WINDOW_LABELS
        },
        "players": player_results,
    }

    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n{'='*55}")
    print(f"Proposed optimal:  decay={proposed_decay}  window={proposed_window}")
    print(f"Decay distribution — median: {output['decay_distribution']['median']}  "
          f"mean: {output['decay_distribution']['mean']:.3f}  "
          f"std: {output['decay_distribution']['std']:.3f}")
    print(f"Window distribution: {output['window_distribution']}")
    print(f"Player-seasons evaluated: {total_player_seasons}")
    print(f"\nAggregate MAE grid (rows=decay, cols=window={WINDOW_LABELS}):")
    header = f"{'decay':>6}  " + "  ".join(f"{w:>5}" for w in WINDOW_LABELS)
    print(header)
    for di, decay in enumerate(DECAY_VALUES):
        row = f"{decay:>6.2f}  " + "  ".join(
            f"{mean_mae_grid[di, wi]:>5.2f}" if not np.isnan(mean_mae_grid[di, wi]) else "  n/a"
            for wi in range(len(WINDOW_VALUES))
        )
        print(row)
    print(f"\nResults saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
