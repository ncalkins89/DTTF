#!/usr/bin/env python3
"""
estimate_blend_weights.py — fit optimal blending weights from historical playoff data.

For each non-empty subset of {our, de, fd}, finds weights (≥ 0, sum = 1) that
minimize MSE against actual PRA.  Writes data/blend_weights.json.

Run after game logs are updated (our EWMA is recomputed retroactively from game_logs).
"""
import json
import sys
from pathlib import Path

import numpy as np
from scipy.optimize import minimize
import sqlite3

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

DECAY = 0.82
DB_PATH = ROOT / "data" / "dttf.db"
WEIGHTS_PATH = ROOT / "data" / "blend_weights.json"
SOURCE_NAMES = ("our", "de", "fd")


def _ewma(conn: sqlite3.Connection, player_id: int, before_date: str) -> float | None:
    rows = conn.execute(
        "SELECT pra FROM game_logs WHERE player_id = ? AND game_date < ? "
        "ORDER BY game_date DESC LIMIT 20",
        (player_id, before_date),
    ).fetchall()
    if not rows:
        return None
    vals = np.array([r[0] for r in rows], dtype=float)
    w = np.array([DECAY**i for i in range(len(vals))])
    w /= w.sum()
    return float(np.dot(w, vals))


def _constrained_fit(X: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Weights ≥ 0, sum = 1, minimizing MSE."""
    n = X.shape[1]
    res = minimize(
        lambda w: float(np.sum((X @ w - y) ** 2)),
        np.full(n, 1.0 / n),
        method="SLSQP",
        constraints={"type": "eq", "fun": lambda w: float(w.sum() - 1)},
        bounds=[(0.0, 1.0)] * n,
    )
    return res.x


def _formula_str(sources: tuple, weights: np.ndarray) -> str:
    labels = {"our": "Ours", "de": "DE", "fd": "FD"}
    shown = [(s, w) for s, w in zip(sources, weights) if w >= 0.005]
    if not shown:
        return ""
    pcts = [round(w * 100) for _, w in shown]
    # Adjust last term so percentages sum exactly to 100
    pcts[-1] += 100 - sum(pcts)
    return " + ".join(f"{pct}% {labels[s]}" for (s, _), pct in zip(shown, pcts))


def _mae(pred: np.ndarray, actual: np.ndarray) -> float:
    return float(np.mean(np.abs(pred - actual)))


def build_dataset(conn: sqlite3.Connection) -> list[dict]:
    dates = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT de.date FROM de_projections de "
            "JOIN game_logs gl ON gl.player_id = de.player_id AND gl.game_date = de.date "
            "AND gl.season_type = 'Playoffs' ORDER BY de.date"
        ).fetchall()
    ]
    if not dates:
        print("  No dates found — is this the right DB?")
        return []

    print(f"  Building from {len(dates)} dates: {dates[0]} → {dates[-1]}")
    records = []
    for d in dates:
        rows = conn.execute(
            "SELECT de.player_id, de.pra, fd.pra, gl.pra "
            "FROM de_projections de "
            "JOIN game_logs gl ON gl.player_id = de.player_id AND gl.game_date = de.date "
            "  AND gl.season_type = 'Playoffs' "
            "LEFT JOIN fd_projections fd ON fd.player_id = de.player_id AND fd.date = de.date "
            "WHERE de.date = ?",
            (d,),
        ).fetchall()
        for pid, de_proj, fd_proj, actual in rows:
            records.append(
                {
                    "our": _ewma(conn, pid, d),
                    "de": de_proj,
                    "fd": fd_proj,
                    "actual": actual,
                }
            )
    return records


def fit_all(records: list[dict]) -> dict:
    results = {}
    for bits in range(1, 8):  # all non-empty subsets of 3 sources
        subset = tuple(s for i, s in enumerate(SOURCE_NAMES) if bits & (1 << i))
        key = "+".join(subset)

        eligible = [
            r for r in records
            if r["actual"] is not None and all(r[s] is not None for s in subset)
        ]
        n = len(eligible)

        if n < 10:
            print(f"  {key}: only {n} rows — using equal weights")
            w = np.full(len(subset), 1.0 / len(subset))
        elif len(subset) == 1:
            w = np.array([1.0])
        else:
            X = np.column_stack([[r[s] for r in eligible] for s in subset])
            y = np.array([r["actual"] for r in eligible])
            w = _constrained_fit(X, y)
            pred = X @ w
            print(f"  {key} (n={n}): MAE={_mae(pred, y):.2f}  →  {_formula_str(subset, w)}")

        results[key] = {
            "sources": list(subset),
            "weights": [round(float(x), 4) for x in w],
            "formula": _formula_str(subset, w),
            "n": n,
        }

    return results


def main() -> None:
    print(f"[estimate_blend_weights] DB: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    records = build_dataset(conn)
    if not records:
        sys.exit(1)
    print(f"  Total records: {len(records)}")
    results = fit_all(records)
    WEIGHTS_PATH.write_text(json.dumps(results, indent=2))
    print(f"  Saved → {WEIGHTS_PATH}")


if __name__ == "__main__":
    main()
